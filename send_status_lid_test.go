package whatsmeow

import (
	"errors"
	"testing"

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
		"device-suffixed LID":      {lid("111"), types.NewADJID("222", 0, 14).ToNonAD(), {User: "333", Server: types.HiddenUserServer, Device: 14}},
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
