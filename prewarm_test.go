package whatsmeow

import (
	"context"
	"testing"

	waLog "go.mau.fi/whatsmeow/util/log"

	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types"
)

// TestGetUserDevicesReportingSync_CacheHitNoWire locks in the fix for the prewarm
// dead-time bug: when every requested JID resolves from the in-memory device cache,
// the call must report hitWire=false (no usync IQ issued). PrewarmSessions skips its
// per-chunk UsyncChunkDelay on a false hitWire, so an all-warm pass no longer sleeps
// ~6s per ≤500 contacts while warming nothing. A regression (hitWire=true on a pure
// cache hit) would re-introduce the ~96s-per-8.3k-roster stall on the pre_send path.
func TestGetUserDevicesReportingSync_CacheHitNoWire(t *testing.T) {
	cli := &Client{
		Log:              waLog.Noop,
		userDevicesCache: make(map[types.JID]deviceCache),
	}

	user := types.JID{User: "2348012345678", Server: types.DefaultUserServer}
	dev0 := user
	dev0.Device = 0
	dev1 := user
	dev1.Device = 1
	cli.userDevicesCache[user] = deviceCache{devices: []types.JID{dev0, dev1}}

	devices, hitWire, err := cli.getUserDevicesReportingSync(context.Background(), []types.JID{user}, "background")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if hitWire {
		t.Fatal("hitWire = true for a fully cache-resolved lookup; prewarm would sleep UsyncChunkDelay for nothing")
	}
	if len(devices) != 2 {
		t.Fatalf("expected 2 cached devices, got %d", len(devices))
	}
}

// allWarmSessions is a SessionStore that reports every queried address as already
// warm and records whether the blob-loading GetManySessions was ever called.
type allWarmSessions struct {
	*store.NoopStore
	getManyCalled bool
}

func (a *allWarmSessions) ContainsManySessions(ctx context.Context, addresses []string) (map[string]bool, error) {
	m := make(map[string]bool, len(addresses))
	for _, addr := range addresses {
		m[addr] = true
	}
	return m, nil
}

func (a *allWarmSessions) GetManySessions(ctx context.Context, addresses []string) (map[string][]byte, error) {
	a.getManyCalled = true
	return a.NoopStore.GetManySessions(ctx, addresses)
}

// TestWarmDeviceChunk_AllWarmSkipsBlobLoad verifies the ContainsManySessions
// optimization: when every device in a chunk already has a session, warmDeviceChunk
// counts them via the existence check and returns WITHOUT calling GetManySessions
// (no session-blob transfer/deserialize) and without fetching any prekeys. This is the
// steady-state path for an established roster; the whole point of the change is that it
// no longer pulls every session blob just to count it.
func TestWarmDeviceChunk_AllWarmSkipsBlobLoad(t *testing.T) {
	ownJID := types.JID{User: "10000000000", Server: types.DefaultUserServer}
	sessions := &allWarmSessions{NoopStore: &store.NoopStore{}}
	dev := &store.Device{
		ID:         &ownJID,
		Sessions:   sessions,
		LIDs:       &store.NoopStore{},
		Identities: &store.NoopStore{},
	}
	cli := &Client{Store: dev, Log: waLog.Noop}

	user := types.JID{User: "2348011112222", Server: types.DefaultUserServer}
	d0 := user
	d0.Device = 0
	d1 := user
	d1.Device = 1
	devices := []types.JID{d0, d1}

	res := &PrewarmResult{}
	if err := cli.warmDeviceChunk(context.Background(), devices, PrewarmOpts{BatchSize: 5000}, res); err != nil {
		t.Fatalf("warmDeviceChunk: %v", err)
	}
	if res.AlreadyWarm != len(devices) {
		t.Fatalf("AlreadyWarm = %d, want %d", res.AlreadyWarm, len(devices))
	}
	if res.Warmed != 0 || res.Batches != 0 || res.Failed != 0 {
		t.Fatalf("expected no warming work, got Warmed=%d Batches=%d Failed=%d", res.Warmed, res.Batches, res.Failed)
	}
	if sessions.getManyCalled {
		t.Fatal("GetManySessions called on an all-warm chunk — blob load not skipped")
	}
}

// TestGetUserDevicesReportingSync_EmptyInput keeps the no-input path wire-free.
func TestGetUserDevicesReportingSync_EmptyInput(t *testing.T) {
	cli := &Client{
		Log:              waLog.Noop,
		userDevicesCache: make(map[types.JID]deviceCache),
	}
	devices, hitWire, err := cli.getUserDevicesReportingSync(context.Background(), nil, "background")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if hitWire {
		t.Fatal("hitWire = true for empty input")
	}
	if len(devices) != 0 {
		t.Fatalf("expected 0 devices, got %d", len(devices))
	}
}
