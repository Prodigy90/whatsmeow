package whatsmeow

import (
	"context"
	"errors"
	"slices"
	"testing"

	"go.mau.fi/libsignal/protocol"
	waBinary "go.mau.fi/whatsmeow/binary"
	"go.mau.fi/whatsmeow/proto/waCommon"
	"go.mau.fi/whatsmeow/proto/waE2E"
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

func tags(nodes []waBinary.Node) []string {
	out := make([]string, len(nodes))
	for i, n := range nodes {
		out[i] = n.Tag
	}
	return out
}

// WhatsApp Web's child order on a status post (capture 2026-11-06 lines 1156/1236/1278).
func TestOrderStatusChildren(t *testing.T) {
	in := []waBinary.Node{{Tag: "participants"}, {Tag: "device-identity"}, {Tag: "meta"}, {Tag: "enc"}, {Tag: "reporting"}}
	got := tags(orderStatusChildren(in))
	want := []string{"reporting", "participants", "enc", "meta", "device-identity"}
	if !slices.Equal(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

// WhatsApp Web lists a user's device-suffixed <to> entries before the bare one.
func TestBareEntryLastPerUser(t *testing.T) {
	to := func(u string, d uint16) waBinary.Node {
		return waBinary.Node{Tag: "to", Attrs: waBinary.Attrs{"jid": types.JID{User: u, Server: types.HiddenUserServer, Device: d}}}
	}
	in := []waBinary.Node{to("a", 0), to("a", 1), to("a", 9), to("b", 0), to("c", 0), to("c", 41), to("c", 43)}
	var got []string
	for _, n := range bareEntryLastPerUser(in) {
		got = append(got, n.Attrs["jid"].(types.JID).String())
	}
	want := []string{"a:1@lid", "a:9@lid", "a@lid", "b@lid", "c:41@lid", "c:43@lid", "c@lid"}
	if !slices.Equal(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

// WhatsApp Web's status delete (capture 2026-11-06 line 1371): <status edit="7">, each
// recipient once as a bare user LID, no per-device encryption, no <meta>, no device-identity.
func TestPrepareStatusWebRevokeNode(t *testing.T) {
	cli := &Client{}
	lid := func(u string, d uint16) types.JID {
		return types.JID{User: u, Server: types.HiddenUserServer, Device: d}
	}
	revoke := &waE2E.Message{ProtocolMessage: &waE2E.ProtocolMessage{
		Type: waE2E.ProtocolMessage_REVOKE.Enum(),
		Key:  &waCommon.MessageKey{RemoteJID: new("status@broadcast"), FromMe: new(true), ID: new("3EB0BC46DDC3478A1716C7")},
	}}
	node, users, err := cli.prepareStatusWebRevokeNode(types.StatusBroadcastJID, "3EB0148B4E661E8A004BC8", revoke,
		[]types.JID{lid("81557301796887", 0), lid("31482697482258", 0), lid("31482697482258", 9), lid("81557301796887", 0), lid("58587783151865", 41)})
	if err != nil {
		t.Fatal(err)
	}
	if node.Tag != "status" || node.Attrs["edit"] != "7" || node.Attrs["type"] != "text" {
		t.Fatalf("root = <%s %v>", node.Tag, node.Attrs)
	}
	if got := tags(node.GetChildren()); !slices.Equal(got, []string{"participants"}) {
		t.Fatalf("children = %v, want only participants (the caller appends the skmsg)", got)
	}
	var jids []string
	for _, to := range node.GetChildren()[0].GetChildren() {
		if len(to.GetChildren()) != 0 || to.Content != nil {
			t.Fatalf("revoke <to> carries content (per-device enc): %v", to)
		}
		jids = append(jids, to.Attrs["jid"].(types.JID).String())
	}
	if want := []string{"81557301796887@lid", "31482697482258@lid", "58587783151865@lid"}; !slices.Equal(jids, want) {
		t.Fatalf("revoke recipients = %v, want %v", jids, want)
	}
	if len(users) != 3 {
		t.Fatalf("users = %v", users)
	}
}

type fakeSenderKeys struct {
	keys map[string][]byte
	err  error
}

func (f fakeSenderKeys) PutSenderKey(_ context.Context, group, user string, session []byte) error {
	f.keys[group+"|"+user] = session
	return nil
}
func (f fakeSenderKeys) GetSenderKey(_ context.Context, group, user string) ([]byte, error) {
	return f.keys[group+"|"+user], f.err
}

// The bare delete is only decryptable under the sender key the original post used. No stored
// key (wiped store, reset) or an unreadable one must fall back to the per-device delete.
func TestHasStoredSenderKey(t *testing.T) {
	own := types.JID{User: "31482697482258", Server: types.HiddenUserServer, Device: 13}
	name := protocol.NewSenderKeyName(types.StatusBroadcastJID.String(), own.SignalAddress())
	keys := fakeSenderKeys{keys: map[string][]byte{}}
	cli := &Client{Store: &store.Device{SenderKeys: keys}}
	if cli.hasStoredSenderKey(context.Background(), name) {
		t.Fatal("no stored key, but the bare delete was allowed")
	}
	keys.keys[name.GroupID()+"|"+name.Sender().String()] = []byte{1}
	if !cli.hasStoredSenderKey(context.Background(), name) {
		t.Fatal("stored key not found")
	}
	cli.Store.SenderKeys = fakeSenderKeys{keys: keys.keys, err: errors.New("db down")}
	if cli.hasStoredSenderKey(context.Background(), name) {
		t.Fatal("unreadable key store, but the bare delete was allowed")
	}
}
