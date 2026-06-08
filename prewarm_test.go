package whatsmeow

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"go.mau.fi/libsignal/keys/prekey"
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

// coldSessions reports every queried address as cold (no stored session) and records
// whether the blob-loading GetManySessions was ever called.
type coldSessions struct {
	*store.NoopStore
	getManyCalled bool
}

func (c *coldSessions) ContainsManySessions(ctx context.Context, addresses []string) (map[string]bool, error) {
	return map[string]bool{}, nil // all cold
}

func (c *coldSessions) GetManySessions(ctx context.Context, addresses []string) (map[string][]byte, error) {
	c.getManyCalled = true
	return c.NoopStore.GetManySessions(ctx, addresses)
}

// TestWarmDeviceChunk_SkipDeviceSkipsColdDevices locks in the negative-cache hook: a
// COLD device the caller's SkipDevice predicate rejects is dropped from the pass — it's
// counted in Skipped, never has its prekey bundle fetched, and the cold-blob load is
// skipped entirely. This is what stops a couple of permanently-dead companion devices
// from re-issuing a prekey IQ (and dragging in the inter-chunk pacing delay) on every
// otherwise-all-warm pass.
func TestWarmDeviceChunk_SkipDeviceSkipsColdDevices(t *testing.T) {
	ownJID := types.JID{User: "10000000000", Server: types.DefaultUserServer}
	sessions := &coldSessions{NoopStore: &store.NoopStore{}}
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

	var asked []types.JID
	res := &PrewarmResult{}
	opts := PrewarmOpts{BatchSize: 5000, SkipDevice: func(j types.JID) bool {
		asked = append(asked, j)
		return true
	}}
	if err := cli.warmDeviceChunk(context.Background(), devices, opts, res); err != nil {
		t.Fatalf("warmDeviceChunk: %v", err)
	}
	if res.Skipped != len(devices) {
		t.Fatalf("Skipped = %d, want %d", res.Skipped, len(devices))
	}
	if res.Warmed != 0 || res.Failed != 0 || res.Batches != 0 {
		t.Fatalf("expected no fetch work, got Warmed=%d Failed=%d Batches=%d", res.Warmed, res.Failed, res.Batches)
	}
	if len(res.FailedDevices) != 0 {
		t.Fatalf("skipped devices must not be reported as failed, got %v", res.FailedDevices)
	}
	if sessions.getManyCalled {
		t.Fatal("GetManySessions called despite all devices skipped — cold blob load not avoided")
	}
	if len(asked) != len(devices) {
		t.Fatalf("SkipDevice consulted %d times, want %d", len(asked), len(devices))
	}
}

// TestClassifyPrewarmBatch is the regression guard for the ban-safety fix: only
// per-device failures (omitted from the response, parse error, or nil bundle) may be
// negative-cached; a device with a usable bundle warms. The whole-BATCH transport-error
// case never reaches this function — warmDeviceChunk handles a non-nil fetchPreKeys error
// before calling classifyPrewarmBatch, counting the batch as Failed without caching any
// device — so classify only ever sees a successfully-fetched response map.
func TestClassifyPrewarmBatch(t *testing.T) {
	good := types.JID{User: "111", Server: types.DefaultUserServer}    // valid bundle → warm
	missing := types.JID{User: "222", Server: types.DefaultUserServer} // absent from resp → cache
	errd := types.JID{User: "333", Server: types.DefaultUserServer}    // per-device error → cache
	nilB := types.JID{User: "444", Server: types.DefaultUserServer}    // present, nil bundle → cache
	batch := []types.JID{good, missing, errd, nilB}

	resp := map[types.JID]preKeyResp{
		good: {bundle: &prekey.Bundle{}},
		errd: {err: errors.New("boom")},
		nilB: {bundle: nil},
		// `missing` deliberately absent from the map.
	}

	warm, cache := classifyPrewarmBatch(batch, resp)

	if len(warm) != 1 || warm[0] != good {
		t.Fatalf("warm = %v, want [%v]", warm, good)
	}
	wantCache := map[types.JID]bool{missing: true, errd: true, nilB: true}
	if len(cache) != len(wantCache) {
		t.Fatalf("cache = %v, want the 3 per-device failures", cache)
	}
	for _, c := range cache {
		if !wantCache[c] {
			t.Errorf("unexpected device negative-cached: %v", c)
		}
	}
}

// TestClassifyPrewarmBatch_AllGoodCachesNothing confirms a fully-successful batch
// negative-caches nothing (the steady-state cold-warm path).
func TestClassifyPrewarmBatch_AllGoodCachesNothing(t *testing.T) {
	a := types.JID{User: "111", Server: types.DefaultUserServer}
	b := types.JID{User: "222", Server: types.DefaultUserServer}
	batch := []types.JID{a, b}
	resp := map[types.JID]preKeyResp{a: {bundle: &prekey.Bundle{}}, b: {bundle: &prekey.Bundle{}}}

	warm, cache := classifyPrewarmBatch(batch, resp)
	if len(warm) != 2 {
		t.Fatalf("warm = %v, want both devices", warm)
	}
	if len(cache) != 0 {
		t.Fatalf("cache = %v, want nothing cached on an all-good batch", cache)
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

// TestIsDefinitivePrekeyRejection locks in the classifier that decides whether a failed
// prekey-fetch IQ may be negative-cached. A per-target refusal (406 not-acceptable / 404
// item-not-found) means the server won't ever serve that JID's bundle → cache it so a
// permanently-dead companion device stops re-probing every send. A transient failure
// (timeout / disconnect / 5xx / rate-limit / empty response) says nothing about the device,
// so caching it would blacklist live devices and force the cold prekey burst the pre-warm
// exists to prevent. Errors are checked through fetchPreKeys's `%w` wrapping.
func TestIsDefinitivePrekeyRejection(t *testing.T) {
	wrap := func(err error) error { return fmt.Errorf("failed to send prekey request: %w", err) }
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"406 not-acceptable (dead device)", wrap(&IQError{Code: 406, Text: "not-acceptable"}), true},
		{"404 item-not-found", wrap(&IQError{Code: 404, Text: "item-not-found"}), true},
		{"bare 406 unwrapped", &IQError{Code: 406, Text: "not-acceptable"}, true},
		{"403 forbidden (account-level, not per-device)", wrap(&IQError{Code: 403, Text: "forbidden"}), false},
		{"429 rate-overlimit (transient)", wrap(&IQError{Code: 429, Text: "rate-overlimit"}), false},
		{"503 service-unavailable (transient)", wrap(&IQError{Code: 503, Text: "service-unavailable"}), false},
		{"IQ timed out (transient)", wrap(ErrIQTimedOut), false},
		{"empty-response error (transient)", errors.New("got empty response to prekey request"), false},
		{"nil error", nil, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isDefinitivePrekeyRejection(tt.err); got != tt.want {
				t.Fatalf("isDefinitivePrekeyRejection(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}
