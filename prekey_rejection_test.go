package whatsmeow

import (
	"context"
	"errors"
	"slices"
	"testing"

	"go.mau.fi/libsignal/keys/prekey"

	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types"
	waLog "go.mau.fi/whatsmeow/util/log"
)

func pnDevice(user string, device uint16) types.JID {
	return types.JID{User: user, Server: types.DefaultUserServer, Device: device}
}

// prekeyServer fakes the prekey IQ: devices in dead are refused, the rest get a
// bundle. A request made only of dead devices fails with 406, and a mixed request
// leaves the dead ones out, which is what WhatsApp answered on 2026-10-29.
func prekeyServer(dead ...types.JID) (func(context.Context, []types.JID) (map[types.JID]preKeyResp, error), *[][]types.JID) {
	var calls [][]types.JID
	return func(_ context.Context, users []types.JID) (map[types.JID]preKeyResp, error) {
		calls = append(calls, slices.Clone(users))
		resp := make(map[types.JID]preKeyResp)
		for _, u := range users {
			if !slices.Contains(dead, u) {
				resp[u] = preKeyResp{bundle: &prekey.Bundle{}}
			}
		}
		if len(resp) == 0 {
			return nil, &IQError{Code: 406, Text: "not-acceptable"}
		}
		return resp, nil
	}, &calls
}

func TestFetchPreKeysForSend_OmittedDeviceIsRejected(t *testing.T) {
	live, dead := pnDevice("2348031568621", 27), pnDevice("2348031568621", 25)
	hook, _ := prekeyServer(dead)
	cli := &Client{Log: waLog.Noop, fetchPreKeysHook: hook}

	var report PreKeyFetchReport
	bundles := cli.fetchPreKeysForSend(context.Background(), []types.JID{live, dead}, &report)

	if bundles[live] == nil {
		t.Fatal("live device got no bundle")
	}
	if _, ok := bundles[dead]; ok {
		t.Fatal("dead device got a bundle entry")
	}
	if !slices.Equal(report.Rejected, []types.JID{dead}) || report.Reasons[dead] != "omitted" {
		t.Fatalf("rejected = %v reasons = %v, want only the omitted dead device", report.Rejected, report.Reasons)
	}
	if report.Requested != 2 || report.Err != nil {
		t.Fatalf("requested = %d err = %v", report.Requested, report.Err)
	}
}

func TestFetchPreKeysForSend_WholeRequest406(t *testing.T) {
	a, b := pnDevice("2348031568621", 25), pnDevice("2347044982391", 35)
	hook, calls := prekeyServer(a, b)
	cli := &Client{Log: waLog.Noop, fetchPreKeysHook: hook}

	var report PreKeyFetchReport
	cli.fetchPreKeysForSend(context.Background(), []types.JID{a, b}, &report)

	if len(report.Rejected) != 2 || report.Reasons[a] != "406 not-acceptable" {
		t.Fatalf("rejected = %v reasons = %v, want both refused with 406", report.Rejected, report.Reasons)
	}
	// One batch request, then one solo probe per device to name the culprits.
	if len(*calls) != 3 {
		t.Fatalf("made %d prekey requests, want 3 (batch + 2 solo probes)", len(*calls))
	}
}

func TestFetchPreKeysForSend_SingleDevice406SkipsProbe(t *testing.T) {
	dead := pnDevice("2347011753352", 24)
	hook, calls := prekeyServer(dead)
	cli := &Client{Log: waLog.Noop, fetchPreKeysHook: hook}

	var report PreKeyFetchReport
	cli.fetchPreKeysForSend(context.Background(), []types.JID{dead}, &report)

	if !slices.Equal(report.Rejected, []types.JID{dead}) {
		t.Fatalf("rejected = %v, want the single device", report.Rejected)
	}
	if len(*calls) != 1 {
		t.Fatalf("made %d prekey requests, want 1 (a single-device 406 needs no probe)", len(*calls))
	}
}

func TestFetchPreKeysForSend_TransientErrorRejectsNothing(t *testing.T) {
	devs := []types.JID{pnDevice("2348031568621", 25), pnDevice("2348031568621", 26)}
	for name, err := range map[string]error{
		"server error": &IQError{Code: 500, Text: "internal-server-error"},
		"rate limit":   &IQError{Code: 429, Text: "rate-overlimit"},
		"timeout":      errors.New("info query timed out"),
	} {
		t.Run(name, func(t *testing.T) {
			cli := &Client{Log: waLog.Noop, fetchPreKeysHook: func(context.Context, []types.JID) (map[types.JID]preKeyResp, error) {
				return nil, err
			}}
			var report PreKeyFetchReport
			cli.fetchPreKeysForSend(context.Background(), devs, &report)
			if len(report.Rejected) != 0 {
				t.Fatalf("a %s rejected %v; only a definitive refusal may drop devices", name, report.Rejected)
			}
			if report.Err == nil {
				t.Fatal("transient error not reported")
			}
		})
	}
}

func TestDropRejectedDevices(t *testing.T) {
	own := pnDevice("2349018943768", 30)
	trimmed := types.JID{User: "2348031568621", Server: types.DefaultUserServer}
	emptied := types.JID{User: "2347011753352", Server: types.DefaultUserServer}
	coldOnly := types.JID{User: "2347044982391", Server: types.DefaultUserServer}
	untouched := types.JID{User: "2348000000000", Server: types.DefaultUserServer}

	trimmedDevs := []types.JID{pnDevice(trimmed.User, 0), pnDevice(trimmed.User, 25), pnDevice(trimmed.User, 26), pnDevice(trimmed.User, 27)}
	coldDevs := []types.JID{pnDevice(coldOnly.User, 0), pnDevice(coldOnly.User, 35)}
	lists := &memDeviceLists{rows: map[types.JID]store.DeviceListEntry{
		trimmed:  {TheirJID: trimmed, Devices: trimmedDevs},
		emptied:  {TheirJID: emptied, Devices: []types.JID{pnDevice(emptied.User, 24)}},
		coldOnly: {TheirJID: coldOnly, Devices: coldDevs},
	}}
	cli := &Client{
		Log:   waLog.Noop,
		Store: &store.Device{ID: &own, DeviceLists: lists},
		userDevicesCache: map[types.JID]deviceCache{
			trimmed:   {devices: trimmedDevs, dhash: "stale"},
			emptied:   {devices: []types.JID{pnDevice(emptied.User, 24)}},
			untouched: {devices: []types.JID{pnDevice(untouched.User, 0)}},
		},
	}

	got := cli.dropRejectedDevices(context.Background(), []types.JID{
		pnDevice(trimmed.User, 25), pnDevice(trimmed.User, 26),
		pnDevice(emptied.User, 24),
		pnDevice(coldOnly.User, 35), // not in memory: trimmed in the store only
	})

	if !slices.Equal(got, []types.JID{emptied}) {
		t.Fatalf("emptied = %v, want [%s]", got, emptied)
	}
	wantTrimmed := []types.JID{pnDevice(trimmed.User, 0), pnDevice(trimmed.User, 27)}
	if c := cli.userDevicesCache[trimmed]; !slices.Equal(c.devices, wantTrimmed) || c.dhash != participantListHashV2(wantTrimmed) {
		t.Fatalf("memory cache for %s = %v (hash %q), want %v with a recomputed hash", trimmed, c.devices, c.dhash, wantTrimmed)
	}
	if e := lists.rows[trimmed]; !slices.Equal(e.Devices, wantTrimmed) {
		t.Fatalf("stored list for %s = %v, want %v", trimmed, e.Devices, wantTrimmed)
	}
	if _, ok := cli.userDevicesCache[emptied]; ok {
		t.Fatal("emptied contact still in memory cache")
	}
	if _, ok := lists.rows[emptied]; ok {
		t.Fatal("emptied contact still in the store (an empty list must not be kept)")
	}
	if e := lists.rows[coldOnly]; !slices.Equal(e.Devices, coldDevs[:1]) {
		t.Fatalf("stored list for memory-cold %s = %v, want only device 0", coldOnly, e.Devices)
	}
	if _, ok := cli.userDevicesCache[coldOnly]; ok {
		t.Fatal("dropping from a memory-cold contact must not add it to the memory cache")
	}
	if len(cli.userDevicesCache[untouched].devices) != 1 {
		t.Fatal("unrelated contact changed")
	}
}

// One refused device can fail the whole IQ even with live devices in it (the
// behaviour the pre-warmer was written against). The solo probes must spare the
// live device.
func TestFetchPreKeysForSend_PoisonedBatchSparesLiveDevice(t *testing.T) {
	live, dead := pnDevice("2348031568621", 27), pnDevice("2348031568621", 25)
	cli := &Client{Log: waLog.Noop, fetchPreKeysHook: func(_ context.Context, users []types.JID) (map[types.JID]preKeyResp, error) {
		if slices.Contains(users, dead) {
			return nil, &IQError{Code: 406, Text: "not-acceptable"}
		}
		return map[types.JID]preKeyResp{live: {bundle: &prekey.Bundle{}}}, nil
	}}

	var report PreKeyFetchReport
	cli.fetchPreKeysForSend(context.Background(), []types.JID{live, dead}, &report)

	if !slices.Equal(report.Rejected, []types.JID{dead}) {
		t.Fatalf("rejected = %v, want only the dead device", report.Rejected)
	}
}

// Past the isolation cap nothing can be named, but the refusal must still be
// reported so the caller's log shows why the send was slow.
func TestFetchPreKeysForSend_OverIsolationCapIsReported(t *testing.T) {
	var devs []types.JID
	for i := 0; i <= prewarmMaxIsolate; i++ {
		devs = append(devs, pnDevice("2348031568621", uint16(i+1)))
	}
	hook, calls := prekeyServer(devs...)
	cli := &Client{Log: waLog.Noop, fetchPreKeysHook: hook}

	var report PreKeyFetchReport
	cli.fetchPreKeysForSend(context.Background(), devs, &report)

	if len(report.Rejected) != 0 {
		t.Fatalf("rejected %d devices past the isolation cap", len(report.Rejected))
	}
	if report.Err == nil {
		t.Fatal("refusal past the isolation cap was not reported")
	}
	if len(*calls) != 1 {
		t.Fatalf("made %d prekey requests, want 1 (no solo probes past the cap)", len(*calls))
	}
}
