package whatsmeow

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"time"

	"go.mau.fi/libsignal/keys/prekey"

	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types"
)

// PreKeyFetchReport describes the prekey fetch a send made for devices it had no
// Signal session with, and what the server answered.
//
// Why it exists: a device a contact has unlinked can stay in the device-list cache
// indefinitely. It never gets a session (the server has no prekeys for it), so every
// send asks for its prekeys again, and the server takes seconds to refuse. One such
// device in an audience added ~2.5s to every 5,000-recipient status batch (2026-10-29).
// The client logger is SILENT in production, so the report carries the answer to the
// caller's own logs.
type PreKeyFetchReport struct {
	// Requested is how many devices prekeys were asked for.
	Requested int
	// Duration is the time spent waiting on prekey fetches, including solo re-probes.
	Duration time.Duration
	// Rejected are devices the server refused: an IQ 404/406 for the device, or left
	// out of an otherwise successful reply. They are dropped from the device-list cache.
	Rejected []types.JID
	// Reasons says how each rejected device was refused ("406 not-acceptable", "omitted").
	Reasons map[types.JID]string
	// Emptied are contacts left with no cached devices after the drop.
	Emptied []types.JID
	// Err is a fetch failure that says nothing about the devices (timeout, disconnect,
	// 5xx, rate limit). Nothing is dropped for it.
	Err error
}

func (r *PreKeyFetchReport) reject(jid types.JID, reason string) {
	if r.Reasons == nil {
		r.Reasons = make(map[types.JID]string)
	}
	if _, seen := r.Reasons[jid]; seen {
		return
	}
	r.Reasons[jid] = reason
	r.Rejected = append(r.Rejected, jid)
}

// fetchPreKeysForSend fetches prekey bundles for devices without a session and records
// what the server answered in report. Unlike fetchPreKeysNoError it tells a definitive
// rejection apart from a transient failure, the same way the pre-warmer does: only a
// 404/406 or a device missing from a successful reply counts as rejected.
func (cli *Client) fetchPreKeysForSend(ctx context.Context, devices []types.JID, report *PreKeyFetchReport) map[types.JID]*prekey.Bundle {
	start := time.Now()
	defer func() { report.Duration += time.Since(start) }()
	report.Requested += len(devices)

	resp, err := cli.fetchPreKeys(ctx, devices)
	if err != nil {
		if !isDefinitivePrekeyRejection(err) {
			report.Err = err
			cli.Log.Warnf("Failed to fetch prekeys for %v with no existing session: %v", devices, err)
			return nil
		}
		// One refused device can fail the whole IQ, so a multi-device batch is re-probed
		// one device at a time to name the culprit (capped at prewarmMaxIsolate).
		reason := prekeyRejectionReason(err)
		for _, dead := range cli.isolateRejectedDevices(ctx, devices, err, &PrewarmResult{}) {
			report.reject(dead, reason)
		}
		return nil
	}
	bundles := make(map[types.JID]*prekey.Bundle, len(devices))
	for _, jid := range devices {
		r, ok := resp[jid]
		switch {
		case !ok:
			report.reject(jid, "omitted")
		case r.err != nil:
			cli.Log.Warnf("Failed to fetch prekey for %s: %v", jid, r.err)
		default:
			bundles[jid] = r.bundle
		}
	}
	return bundles
}

func prekeyRejectionReason(err error) string {
	var iqErr *IQError
	if errors.As(err, &iqErr) {
		return fmt.Sprintf("%d %s", iqErr.Code, iqErr.Text)
	}
	return err.Error()
}

// dropRejectedDevices removes devices the server refused prekeys for from the in-memory
// and persistent device-list caches, so later sends stop asking for them. The list hash
// is recomputed the way device notifications compute it, so a later notification for
// the contact compares against the trimmed list. Returns the contacts left with no
// devices; those are removed from both caches rather than stored empty.
func (cli *Client) dropRejectedDevices(ctx context.Context, rejected []types.JID) (emptied []types.JID) {
	if len(rejected) == 0 {
		return nil
	}
	byUser := make(map[types.JID]map[types.JID]struct{})
	for _, dev := range rejected {
		user := dev.ToNonAD()
		if byUser[user] == nil {
			byUser[user] = make(map[types.JID]struct{})
		}
		byUser[user][dev] = struct{}{}
	}

	cli.userDevicesCacheLock.Lock()
	defer cli.userDevicesCacheLock.Unlock()

	ourJID := cli.getOwnID().ToNonAD()
	persistent := cli.Store != nil && cli.Store.DeviceLists != nil && !ourJID.IsEmpty()

	// Contacts not in memory (released idle caches) may still have a persistent row.
	var stored map[types.JID]store.DeviceListEntry
	if persistent {
		var missing []types.JID
		for user := range byUser {
			if _, ok := cli.userDevicesCache[user]; !ok {
				missing = append(missing, user)
			}
		}
		if len(missing) > 0 {
			var err error
			stored, err = cli.Store.DeviceLists.GetManyDeviceLists(ctx, ourJID, missing)
			if err != nil {
				cli.Log.Warnf("Failed to load %d device lists to drop rejected devices: %v", len(missing), err)
			}
		}
	}

	var updates []store.DeviceListEntry
	for user, drop := range byUser {
		var current []types.JID
		if cached, ok := cli.userDevicesCache[user]; ok {
			current = cached.devices
		} else if entry, ok := stored[user]; ok {
			current = entry.Devices
		} else {
			continue
		}
		kept := make([]types.JID, 0, len(current))
		for _, dev := range current {
			if _, refused := drop[dev]; !refused {
				kept = append(kept, dev)
			}
		}
		if len(kept) == len(current) {
			continue
		}
		if len(kept) == 0 {
			delete(cli.userDevicesCache, user)
			emptied = append(emptied, user)
			if persistent {
				if err := cli.Store.DeviceLists.DeleteDeviceList(ctx, ourJID, user); err != nil {
					cli.Log.Warnf("Failed to delete emptied device list for %s: %v", user, err)
				}
			}
			continue
		}
		dhash := participantListHashV2(kept)
		if _, ok := cli.userDevicesCache[user]; ok {
			cli.userDevicesCache[user] = deviceCache{devices: kept, dhash: dhash}
		}
		updates = append(updates, store.DeviceListEntry{TheirJID: user, Devices: kept, DHash: dhash})
	}
	if persistent && len(updates) > 0 {
		if err := cli.Store.DeviceLists.PutManyDeviceLists(ctx, ourJID, updates); err != nil {
			cli.Log.Warnf("Failed to persist %d trimmed device lists: %v", len(updates), err)
		}
	}
	return emptied
}

// notifyPreKeyRejectedContacts invokes OnPreKeyRejectedContacts under panic recovery.
func (cli *Client) notifyPreKeyRejectedContacts(jids []types.JID) {
	if len(jids) == 0 || cli.OnPreKeyRejectedContacts == nil {
		return
	}
	defer func() {
		if r := recover(); r != nil {
			cli.Log.Errorf("OnPreKeyRejectedContacts callback panicked: %v\n%s", r, debug.Stack())
		}
	}()
	cli.OnPreKeyRejectedContacts(jids)
}
