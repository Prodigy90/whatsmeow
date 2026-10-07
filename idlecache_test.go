package whatsmeow

import (
	"context"
	"testing"
	"time"

	waLog "go.mau.fi/whatsmeow/util/log"

	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types"
)

// memDeviceLists is an in-memory DeviceListStore standing in for the persistent
// whatsmeow_device_cache table.
type memDeviceLists struct {
	rows  map[types.JID]store.DeviceListEntry
	reads int
}

func (m *memDeviceLists) PutManyDeviceLists(_ context.Context, _ types.JID, entries []store.DeviceListEntry) error {
	for _, e := range entries {
		m.rows[e.TheirJID.ToNonAD()] = e
	}
	return nil
}

func (m *memDeviceLists) GetManyDeviceLists(_ context.Context, _ types.JID, theirJIDs []types.JID) (map[types.JID]store.DeviceListEntry, error) {
	m.reads++
	out := make(map[types.JID]store.DeviceListEntry)
	for _, j := range theirJIDs {
		if e, ok := m.rows[j.ToNonAD()]; ok {
			out[j.ToNonAD()] = e
		}
	}
	return out, nil
}

func (m *memDeviceLists) DeleteDeviceList(context.Context, types.JID, types.JID) error { return nil }
func (m *memDeviceLists) DeleteAllDeviceLists(context.Context, types.JID) error        { return nil }

type countingReleaser struct{ n, calls int }

func (c *countingReleaser) ReleaseCache() int { c.calls++; return c.n }

type releasableLIDs struct {
	store.LIDStore
	countingReleaser
}

type releasableContacts struct {
	store.ContactStore
	countingReleaser
}

func idleCacheTestClient(lists store.DeviceListStore) (*Client, []types.JID) {
	own := types.JID{User: "2348000000000", Server: types.DefaultUserServer}
	cli := &Client{
		Log:              waLog.Noop,
		userDevicesCache: make(map[types.JID]deviceCache),
		Store: &store.Device{
			ID:          &own,
			DeviceLists: lists,
			LIDs:        &releasableLIDs{countingReleaser: countingReleaser{n: 7}},
			Contacts:    &releasableContacts{countingReleaser: countingReleaser{n: 9}},
		},
	}
	var users []types.JID
	for _, u := range []string{"2348011111111", "2348022222222", "2348033333333"} {
		user := types.JID{User: u, Server: types.DefaultUserServer}
		dev1 := user
		dev1.Device = 1
		devices := []types.JID{user, dev1}
		cli.userDevicesCache[user] = deviceCache{devices: devices, dhash: "h-" + u}
		if mem, ok := lists.(*memDeviceLists); ok {
			mem.rows[user] = store.DeviceListEntry{TheirJID: user, Devices: devices, DHash: "h-" + u}
		}
		users = append(users, user)
	}
	return cli, users
}

// The ban-safety property: after a release, the next send must rebuild every
// device list from the persistent store and send NO usync to WhatsApp. If
// ReleaseIdleCaches ever dropped device lists whose rows are not in the store, or
// the read-on-miss path stopped consulting the store, hitWire would turn true here.
func TestReleaseIdleCaches_NextSendReadsStoreNotWire(t *testing.T) {
	lists := &memDeviceLists{rows: make(map[types.JID]store.DeviceListEntry)}
	cli, users := idleCacheTestClient(lists)

	got := cli.ReleaseIdleCaches()
	if got.DeviceLists != 3 || got.LIDMappings != 7 || got.Contacts != 9 {
		t.Fatalf("released = %+v, want {3 7 9}", got)
	}
	if len(cli.userDevicesCache) != 0 {
		t.Fatalf("device cache still holds %d entries", len(cli.userDevicesCache))
	}

	devices, hitWire, err := cli.getUserDevicesReportingSync(context.Background(), users, "message")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if hitWire {
		t.Fatal("hitWire = true after a release: the next send would usync the whole audience")
	}
	if lists.reads != 1 {
		t.Fatalf("persistent store read %d times, want 1", lists.reads)
	}
	if len(devices) != 6 {
		t.Fatalf("got %d devices, want 6", len(devices))
	}
	if len(cli.userDevicesCache) != 3 {
		t.Fatalf("cache refilled with %d entries, want 3", len(cli.userDevicesCache))
	}
}

// Without a persistent store a dropped device list can only come back by usync,
// so the device cache must be left alone. The other caches are DB-backed either way.
func TestReleaseIdleCaches_KeepsDeviceListsWithoutStore(t *testing.T) {
	cli, _ := idleCacheTestClient(nil)

	got := cli.ReleaseIdleCaches()
	if got.DeviceLists != 0 {
		t.Fatalf("released %d device lists with no persistent store", got.DeviceLists)
	}
	if len(cli.userDevicesCache) != 3 {
		t.Fatalf("device cache has %d entries, want 3 kept", len(cli.userDevicesCache))
	}
	if got.LIDMappings != 7 || got.Contacts != 9 {
		t.Fatalf("released = %+v, want LID 7 and contacts 9", got)
	}
}

func TestReleaseIdleCaches_NilSafe(t *testing.T) {
	var cli *Client
	if got := cli.ReleaseIdleCaches(); got != (ReleasedCaches{}) {
		t.Fatalf("nil client released %+v", got)
	}
}

func TestLastDeviceListUseTracksSends(t *testing.T) {
	lists := &memDeviceLists{rows: make(map[types.JID]store.DeviceListEntry)}
	cli, users := idleCacheTestClient(lists)
	if !cli.LastDeviceListUse().IsZero() {
		t.Fatal("LastDeviceListUse set before any send")
	}
	before := time.Now()
	if _, _, err := cli.getUserDevicesReportingSync(context.Background(), users, "message"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := cli.LastDeviceListUse(); got.Before(before) {
		t.Fatalf("LastDeviceListUse = %v, want >= %v", got, before)
	}
}
