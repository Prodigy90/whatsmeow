package whatsmeow

import (
	"time"

	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types"
)

// ReleasedCaches reports how many entries ReleaseIdleCaches dropped.
type ReleasedCaches struct {
	DeviceLists int
	LIDMappings int
	Contacts    int
}

// ReleaseIdleCaches drops in-memory caches that grow with the size of the
// account's audience and are never trimmed otherwise: the device lists of every
// recipient ever sent to, the PN↔LID map and the contact store's cache. A
// 35K-recipient status account holds ~50 MB of these long after its last send.
//
// Every dropped entry is rebuilt from the database on the next read, exactly as
// after a restart. Device lists are only dropped when a persistent device-list
// store is wired (Store.DeviceLists): without it, a miss means a usync to
// WhatsApp, and a cold usync burst across a whole audience is a ban trigger.
//
// Safe to call at any time — every cache is swapped under its own lock, and a
// concurrent send that misses simply reads the database. Callers should still
// wait for the account to go quiet, or busy accounts would reload their whole
// audience from the database on every send.
//
// Session locks (Store.LockSession) are deliberately NOT released: decryption of
// inbound messages takes them too, and replacing a mutex another goroutine holds
// would let two goroutines advance the same Signal ratchet.
func (cli *Client) ReleaseIdleCaches() ReleasedCaches {
	var out ReleasedCaches
	if cli == nil || cli.Store == nil {
		return out
	}
	if cli.Store.DeviceLists != nil {
		cli.userDevicesCacheLock.Lock()
		out.DeviceLists = len(cli.userDevicesCache)
		// A fresh map, not delete(): a Go map never shrinks its bucket array.
		cli.userDevicesCache = make(map[types.JID]deviceCache)
		cli.userDevicesCacheLock.Unlock()
	}
	if r, ok := cli.Store.LIDs.(store.IdleCacheReleaser); ok {
		out.LIDMappings = r.ReleaseCache()
	}
	if r, ok := cli.Store.Contacts.(store.IdleCacheReleaser); ok {
		out.Contacts = r.ReleaseCache()
	}
	return out
}

// LastDeviceListUse reports when a send last resolved recipients' device lists,
// or the zero time if none has since the client was created. Every status, group
// and direct send goes through that lookup, so this is when the account last
// sent anything — the signal for when ReleaseIdleCaches is worth calling.
func (cli *Client) LastDeviceListUse() time.Time {
	if cli == nil {
		return time.Time{}
	}
	ns := cli.lastDeviceListUse.Load()
	if ns == 0 {
		return time.Time{}
	}
	return time.Unix(0, ns)
}
