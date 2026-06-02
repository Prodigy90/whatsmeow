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
	// BatchDelay is slept between consecutive prekey-fetch batches within a chunk.
	// Zero means no delay (back-to-back). Used to spread the acquisition burst.
	BatchDelay time.Duration
	// UsyncChunkDelay is slept between consecutive ≤500-contact warm chunks. Zero
	// means back-to-back. Native WA Web spaces its 500-user usync chunks ~4-13s
	// apart; this is the gap that keeps the warm a slow trickle, not a burst.
	UsyncChunkDelay time.Duration
	// UsyncContext is the usync `context` attr used for device resolution.
	// Defaults to "background" (WA Web's roster-warm context) when empty.
	UsyncContext string
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
// bulk-prefetches prekey bundles for the whole audience ~100s before the post, so
// the broadcast itself emits a clean fanout over already-warm sessions.
//
// It STREAMS the work in ≤maxUsyncUsersPerQuery (500) contact chunks: resolve one
// chunk's device list (usync, persisted to the device cache), warm that chunk's
// Signal sessions in a chunk-scoped context, flush them to the store, evict the
// chunk's device lists from memory, and drop everything before the next chunk.
// Peak heap is therefore O(chunk), independent of roster size — nothing from a
// finished chunk (device lists, session/identity caches, prekey bundles) is
// retained. This is what makes a full-roster warm safe on a memory-constrained
// pod: the earlier resolve-all-then-warm-all shape held the entire roster's device
// list + session cache until one end-of-pass flush and OOM-crashlooped a basic-tier
// worker (whose never-completing restart sweep then 403-banned the account).
//
// NOTE: the O(chunk) flat-memory guarantee REQUIRES a persistent device-list store
// (Store.DeviceLists, wired by the app under DEVICE_CACHE_PERSIST_ENABLED). With it
// nil the per-chunk evictDeviceCache below no-ops, so resolved device lists accumulate
// in the in-memory userDevicesCache and peak heap becomes O(roster) again.
//
// It is idempotent: devices that already have a session are skipped (per chunk), so
// a second pass only warms the delta. Per-device fetch/bundle errors are counted in
// the result rather than returned, so a few unreachable devices don't abort the
// pass. The session-establishment path mirrors the bundle branch of
// encryptMessageForDevice exactly, so a subsequent send finds ContainsSession==true
// for every warmed device and emits no prekey IQ for it.
func (cli *Client) PrewarmSessions(ctx context.Context, jids []types.JID, opts PrewarmOpts) (*PrewarmResult, error) {
	if opts.BatchSize <= 0 {
		opts.BatchSize = 5000
	}
	res := &PrewarmResult{}
	if len(jids) == 0 {
		return res, nil
	}
	usyncContext := opts.UsyncContext
	if usyncContext == "" {
		usyncContext = "background"
	}

	contactChunks := usyncDeviceChunks(jids)
	for i, chunk := range contactChunks {
		if i > 0 && opts.UsyncChunkDelay > 0 {
			select {
			case <-ctx.Done():
				return res, ctx.Err()
			case <-time.After(opts.UsyncChunkDelay):
			}
		}

		// 1. Resolve this chunk's device list — ≤500 contacts → a single usync IQ in
		//    `background` context (cache / persistent-store hits resolve without an
		//    IQ). getUserDevices write-throughs the result to the persistent device
		//    cache. A usync error means the server rejected us (the 403 precursor) —
		//    abort rather than keep hammering.
		chunkDevices, err := cli.getUserDevices(ctx, chunk, usyncContext)
		if err != nil {
			cli.Log.Warnf("Prewarm: usync failed for chunk %d/%d: %v", i+1, len(contactChunks), err)
			return res, fmt.Errorf("prewarm: failed to resolve device list: %w", err)
		}
		res.Devices += len(chunkDevices)

		// 2. Warm this chunk's sessions in a chunk-scoped cache (flushed + dropped
		//    inside warmDeviceChunk). Best-effort: a chunk-level error is logged and
		//    counted; only ctx cancellation aborts the whole pass.
		if err := cli.warmDeviceChunk(ctx, chunkDevices, opts, res); err != nil {
			if ctx.Err() != nil {
				return res, ctx.Err()
			}
			cli.Log.Warnf("Prewarm: warm failed for chunk %d/%d: %v", i+1, len(contactChunks), err)
		}

		// 3. Drop this chunk's device lists from the in-memory cache — they're in the
		//    persistent store now and reload on demand at send time (read-on-miss),
		//    so in-memory device state stays flat across the whole warm. No-op without
		//    a persistent store wired.
		cli.evictDeviceCache(chunk)
	}
	return res, nil
}

// warmDeviceChunk establishes Signal sessions for one chunk's devices using a
// chunk-scoped session/identity cache (attached to a context that is GC'd when this
// function returns), fetches prekeys for the cold ones in paced sub-batches, and
// flushes the warmed sessions to the store. It holds only this chunk's data and
// accumulates counts into res. Mirrors the encryption-identity / session-build path
// of encryptMessageForDevices.
func (cli *Client) warmDeviceChunk(ctx context.Context, devices []types.JID, opts PrewarmOpts, res *PrewarmResult) error {
	if len(devices) == 0 {
		return nil
	}
	ownJID := cli.getOwnID()
	ownLID := cli.getOwnLID()

	// Resolve PN→LID encryption identities so we warm the SAME signal address the
	// send will later look up (mirrors encryptMessageForDevices).
	var pnDevices []types.JID
	for _, jid := range devices {
		if jid.Server == types.DefaultUserServer {
			pnDevices = append(pnDevices, jid)
		}
	}
	var lidMappings map[types.JID]types.JID
	if len(pnDevices) > 0 {
		var err error
		lidMappings, err = cli.Store.LIDs.GetManyLIDsForPNs(ctx, pnDevices)
		if err != nil {
			return fmt.Errorf("fetch LID mappings: %w", err)
		}
	}

	// encIdentity maps each device's original JID → its encryption identity (the LID
	// when a mapping exists, else the device JID itself). The prekey bundle is fetched
	// keyed by the original JID but the session is stored under the encryption
	// identity's signal address — identical to the send.
	encIdentity := make(map[types.JID]types.JID, len(devices))
	addrToOriginal := make(map[string]types.JID, len(devices))
	sessionAddresses := make([]string, 0, len(devices))
	pnToLIDMappings := make(map[types.JID]types.JID)
	for _, jid := range devices {
		if jid == ownJID || jid == ownLID {
			continue // skip the primary own JID/LID; the send skips them too
		}
		identity := jid
		if jid.Server == types.DefaultUserServer {
			if lid, ok := lidMappings[jid]; ok && !lid.IsEmpty() {
				identity = lid
				pnToLIDMappings[jid] = lid
			}
		}
		encIdentity[jid] = identity
		addr := identity.SignalAddress().String()
		sessionAddresses = append(sessionAddresses, addr)
		addrToOriginal[addr] = jid
	}
	if len(sessionAddresses) == 0 {
		return nil
	}

	// Migrate any PN-addressed sessions/identities to their LID address before the
	// cached lookup, exactly as the send does — else a device whose session still
	// lives under the old PN address reads as cold under its LID address and gets a
	// redundant prekey re-fetch. Mirrors encryptMessageForDevices.
	if len(pnToLIDMappings) > 0 {
		if mErr := cli.Store.Sessions.MigrateManyPNsToLIDs(ctx, pnToLIDMappings); mErr != nil {
			cli.Log.Errorf("Prewarm: failed to migrate PN→LID sessions: %v", mErr)
		}
	}

	// Bulk-load this chunk's existing sessions + identities into a chunk-scoped cache.
	// The cache lives in cctx and is reclaimed when this function returns.
	existingSessions, cctx, err := cli.Store.WithCachedSessions(ctx, sessionAddresses)
	if err != nil {
		return fmt.Errorf("load existing sessions: %w", err)
	}
	cctx, err = cli.Store.WithCachedIdentities(cctx, sessionAddresses)
	if err != nil {
		return fmt.Errorf("load existing identities: %w", err)
	}

	// coldOriginals are the original JIDs in this chunk we still need bundles for.
	var coldOriginals []types.JID
	for addr, exists := range existingSessions {
		if exists {
			res.AlreadyWarm++
			continue
		}
		coldOriginals = append(coldOriginals, addrToOriginal[addr])
	}
	if len(coldOriginals) == 0 {
		return nil // nothing cold in this chunk; caches unchanged, nothing to flush
	}

	// warmed is counted locally and folded into res.Warmed only after a successful
	// flush below. If flushPrewarmCaches fails the warmed sessions never reach the
	// store, so crediting them up front would over-report (a subsequent send re-warms
	// them via a prekey IQ regardless).
	warmed := 0

	// Fetch prekey bundles + establish sessions, in paced sub-batches.
	for start := 0; start < len(coldOriginals); start += opts.BatchSize {
		end := start + opts.BatchSize
		if end > len(coldOriginals) {
			end = len(coldOriginals)
		}
		batch := coldOriginals[start:end]
		res.Batches++

		bundles := cli.fetchPreKeysNoError(cctx, batch)
		for _, original := range batch {
			bundle := bundles[original]
			if bundle == nil {
				res.Failed++ // no bundle returned (offline device, error, etc.)
				continue
			}
			if err := cli.processPrewarmBundle(cctx, encIdentity[original], bundle); err != nil {
				cli.Log.Warnf("Prewarm: failed to establish session for %s: %v", encIdentity[original], err)
				res.Failed++
				continue
			}
			warmed++
		}

		if opts.BatchDelay > 0 && end < len(coldOriginals) {
			select {
			case <-ctx.Done():
				// Flush what we warmed in this chunk on a non-cancelled ctx —
				// PutCachedSessions issues DB writes that would fail on the
				// already-cancelled ctx, losing the partial chunk. Credit
				// res.Warmed only if that flush actually persists them.
				if flushErr := cli.flushPrewarmCaches(context.WithoutCancel(cctx)); flushErr == nil {
					res.Warmed += warmed
				}
				return ctx.Err()
			case <-time.After(opts.BatchDelay):
			}
		}
	}

	// Persist this chunk's warmed sessions/identities so a subsequent send (different
	// ctx) sees them; then cctx (and its caches) drop when we return. Credit res.Warmed
	// only now that the flush has succeeded.
	if err := cli.flushPrewarmCaches(cctx); err != nil {
		return err
	}
	res.Warmed += warmed
	return nil
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
