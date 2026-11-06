package whatsmeow

import (
	"errors"
	"testing"

	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types"
)

// A LID-addressed status must never carry a phone-addressed participant: the
// 2026-05-09 send that mixed five LIDs with our own phone JID came back as one
// <ack error="400"> for the whole batch.
func TestCheckStatusLIDParticipants(t *testing.T) {
	lid := func(u string) types.JID { return types.NewJID(u, types.HiddenUserServer) }
	pn := func(u string) types.JID { return types.NewJID(u, types.DefaultUserServer) }

	if err := checkStatusLIDParticipants([]types.JID{lid("111"), lid("222"), lid("81557301796887")}); err != nil {
		t.Fatalf("all-LID list refused: %v", err)
	}

	bad := map[string][]types.JID{
		"own phone JID among LIDs": {lid("111"), lid("222"), pn("2349018943768")},
		"phone JID first":          {pn("2348000000000"), lid("111")},
		"device-suffixed LID":      {lid("111"), {User: "333", Server: types.HiddenUserServer, Device: 14}},
		"group JID":                {lid("111"), types.NewJID("120363000000000000", types.GroupServer)},
		"empty user":               {{Server: types.HiddenUserServer}},
		"no participants":          nil,
	}
	for name, list := range bad {
		err := checkStatusLIDParticipants(list)
		if !errors.Is(err, ErrStatusMixedAddressing) {
			t.Errorf("%s: got %v, want ErrStatusMixedAddressing", name, err)
		}
	}
}

// WhatsApp Web leaves its sending device out of a status's <participants> (captures 2026-05-19
// and 2026-11-06); our 11-06 handset test listed the sending worker 31482697482258:13 among the
// recipients. Only the sending device goes: the account's other devices (the phone, an iPad)
// stay, or the status would not show on the user's own phone.
func TestWithoutSendingDevice(t *testing.T) {
	ownPN := types.JID{User: "2348105300696", Server: types.DefaultUserServer, Device: 13}
	cli := &Client{Store: &store.Device{ID: &ownPN, LID: types.JID{User: "31482697482258", Server: types.HiddenUserServer, Device: 13}}}
	lidDev := func(u string, d uint16) types.JID {
		return types.JID{User: u, Server: types.HiddenUserServer, Device: d}
	}

	in := []types.JID{
		lidDev("31482697482258", 0), lidDev("31482697482258", 1), lidDev("31482697482258", 9),
		lidDev("31482697482258", 13), // the sending device, LID form
		{User: "2348105300696", Server: types.DefaultUserServer, Device: 13, RawAgent: 0}, // and phone form
		lidDev("58587783151865", 0), lidDev("58587783151865", 13), // someone else's device 13 stays
	}
	got := cli.withoutSendingDevice(append([]types.JID(nil), in...))
	want := []types.JID{
		lidDev("31482697482258", 0), lidDev("31482697482258", 1), lidDev("31482697482258", 9),
		lidDev("58587783151865", 0), lidDev("58587783151865", 13),
	}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %v, want %v", got, want)
		}
	}
}
