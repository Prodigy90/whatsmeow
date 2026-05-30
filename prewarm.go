package whatsmeow

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.mau.fi/libsignal/keys/prekey"
	"go.mau.fi/libsignal/session"
	"go.mau.fi/libsignal/signalerror"

	"go.mau.fi/whatsmeow/types"
)

// PrewarmOpts configures a PrewarmSessions pass.
type PrewarmOpts struct {
	// BatchSize is the max number of session-less devices to request prekey
	// bundles for in a single encrypt-namespace IQ. Defaults to 5000.
	BatchSize int
	// BatchDelay is slept between consecutive prekey-fetch batches. Zero means
	// no delay (back-to-back). Used to spread the acquisition burst.
	BatchDelay time.Duration
}

// PrewarmResult reports what a PrewarmSessions pass accomplished.
type PrewarmResult struct {
	Devices     int // total devices resolved via usync
	AlreadyWarm int // devices that already had a Signal session
	Warmed      int // devices for which a new session was established
	Failed      int // devices we tried to warm but couldn't (fetch/bundle/process error)
	Batches     int // number of prekey-fetch batches issued
}

// PrewarmSessions establishes Signal sessions for the given JIDs ahead of time,
// WITHOUT sending any message.
//
// This decouples the expensive cold session acquisition — usync device-list
// resolution + prekey-bundle fetch + X3DH — from the actual broadcast send.
// Native WhatsApp Web does exactly this: when a status audience is resolved it
// bulk-prefetches prekey bundles for the whole audience ~100s before the post,
// so the broadcast itself emits a clean fanout over already-warm sessions.
// whatsmeow's default path instead fetches prekeys lazily inside
// encryptMessageForDevices at send time, fusing a tens-of-thousands-IQ cold
// burst into the message emission — a strong spam signal. Calling
// PrewarmSessions at audience-resolution time (and re-running it just before a
// send to top up deltas) reproduces the WA Web behaviour.
//
// It is idempotent: devices that already have a session are skipped, so a
// second call only warms the delta. Per-device fetch/bundle errors are counted
// in the result rather than returned, so a few unreachable devices don't abort
// the pass. The session-establishment path mirrors the bundle branch of
// encryptMessageForDevice exactly (same PN→LID encryption identity, same
// prekey-keyed-by-original-JID lookup, same AutoTrustIdentity retry, same
// cached-session write-back), so a subsequent send finds ContainsSession==true
// for every warmed device and emits no prekey IQ for it.
func (cli *Client) PrewarmSessions(ctx context.Context, jids []types.JID, opts PrewarmOpts) (*PrewarmResult, error) {
	if opts.BatchSize <= 0 {
		opts.BatchSize = 5000
	}
	res := &PrewarmResult{}
	if len(jids) == 0 {
		return res, nil
	}

	// 1. Resolve the device list (usync) — once, decoupled from any send.
	allDevices, err := cli.GetUserDevicesContext(ctx, jids)
	if err != nil {
		return res, fmt.Errorf("prewarm: failed to resolve device list: %w", err)
	}
	res.Devices = len(allDevices)
	if len(allDevices) == 0 {
		return res, nil
	}

	ownJID := cli.getOwnID()
	ownLID := cli.getOwnLID()

	// 2. Resolve PN→LID encryption identities so we warm the SAME signal
	//    address the send will later look up (mirrors encryptMessageForDevices).
	var pnDevices []types.JID
	for _, jid := range allDevices {
		if jid.Server == types.DefaultUserServer {
			pnDevices = append(pnDevices, jid)
		}
	}
	var lidMappings map[types.JID]types.JID
	if len(pnDevices) > 0 {
		lidMappings, err = cli.Store.LIDs.GetManyLIDsForPNs(ctx, pnDevices)
		if err != nil {
			return res, fmt.Errorf("prewarm: failed to fetch LID mappings: %w", err)
		}
	}

	// encIdentity maps each device's original JID → its encryption identity
	// (the LID when a mapping exists, else the device JID itself). The prekey
	// bundle is fetched keyed by the original JID but the session is stored
	// under the encryption identity's signal address — identical to the send.
	encIdentity := make(map[types.JID]types.JID, len(allDevices))
	addrToOriginal := make(map[string]types.JID, len(allDevices))
	sessionAddresses := make([]string, 0, len(allDevices))
	for _, jid := range allDevices {
		if jid == ownJID || jid == ownLID {
			continue // never warm our own devices; the send skips them too
		}
		identity := jid
		if jid.Server == types.DefaultUserServer {
			if lid, ok := lidMappings[jid]; ok && !lid.IsEmpty() {
				identity = lid
			}
		}
		encIdentity[jid] = identity
		addr := identity.SignalAddress().String()
		sessionAddresses = append(sessionAddresses, addr)
		addrToOriginal[addr] = jid
	}
	if len(sessionAddresses) == 0 {
		return res, nil
	}

	// 3. Bulk-load existing sessions + identities into a request-scoped cache.
	existingSessions, ctx, err := cli.Store.WithCachedSessions(ctx, sessionAddresses)
	if err != nil {
		return res, fmt.Errorf("prewarm: failed to load existing sessions: %w", err)
	}
	ctx, err = cli.Store.WithCachedIdentities(ctx, sessionAddresses)
	if err != nil {
		return res, fmt.Errorf("prewarm: failed to load existing identities: %w", err)
	}

	// coldOriginals are the original JIDs we still need prekey bundles for.
	var coldOriginals []types.JID
	for addr, exists := range existingSessions {
		if exists {
			res.AlreadyWarm++
			continue
		}
		coldOriginals = append(coldOriginals, addrToOriginal[addr])
	}
	if len(coldOriginals) == 0 {
		return res, nil
	}

	// 4. Fetch prekey bundles + establish sessions, in paced batches.
	for start := 0; start < len(coldOriginals); start += opts.BatchSize {
		end := start + opts.BatchSize
		if end > len(coldOriginals) {
			end = len(coldOriginals)
		}
		batch := coldOriginals[start:end]
		res.Batches++

		bundles := cli.fetchPreKeysNoError(ctx, batch)
		for _, original := range batch {
			bundle := bundles[original]
			if bundle == nil {
				res.Failed++ // no bundle returned (offline device, error, etc.)
				continue
			}
			if err := cli.processPrewarmBundle(ctx, encIdentity[original], bundle); err != nil {
				cli.Log.Warnf("Prewarm: failed to establish session for %s: %v", encIdentity[original], err)
				res.Failed++
				continue
			}
			res.Warmed++
		}

		if opts.BatchDelay > 0 && end < len(coldOriginals) {
			select {
			case <-ctx.Done():
				cli.flushPrewarmCaches(ctx)
				return res, ctx.Err()
			case <-time.After(opts.BatchDelay):
			}
		}
	}

	// 5. Persist the warmed sessions/identities from the request cache to the
	//    store, so a subsequent send (different ctx) sees them.
	if err := cli.flushPrewarmCaches(ctx); err != nil {
		return res, err
	}
	return res, nil
}

// processPrewarmBundle establishes (and persists into the cached session store)
// a Signal session from a fetched prekey bundle, WITHOUT encrypting or sending
// anything. Mirrors the bundle branch of encryptMessageForDevice, including the
// AutoTrustIdentity clear-and-retry.
func (cli *Client) processPrewarmBundle(ctx context.Context, to types.JID, bundle *prekey.Bundle) error {
	builder := session.NewBuilderFromSignal(cli.Store, to.SignalAddress(), pbSerializer)
	err := builder.ProcessBundle(ctx, bundle)
	if cli.AutoTrustIdentity && errors.Is(err, signalerror.ErrUntrustedIdentity) {
		cli.Log.Warnf("Prewarm: untrusted identity for %s, clearing and retrying", to)
		if clearErr := cli.clearUntrustedIdentity(ctx, to); clearErr != nil {
			return fmt.Errorf("failed to clear untrusted identity: %w", clearErr)
		}
		err = builder.ProcessBundle(ctx, bundle)
	}
	if err != nil {
		return fmt.Errorf("failed to process prekey bundle: %w", err)
	}
	return nil
}

// flushPrewarmCaches writes the cached sessions and identities accumulated
// during a prewarm pass back to the persistent store.
func (cli *Client) flushPrewarmCaches(ctx context.Context) error {
	if err := cli.Store.PutCachedSessions(ctx); err != nil {
		return fmt.Errorf("prewarm: failed to persist sessions: %w", err)
	}
	if err := cli.Store.PutCachedIdentities(ctx); err != nil {
		return fmt.Errorf("prewarm: failed to persist identities: %w", err)
	}
	return nil
}
