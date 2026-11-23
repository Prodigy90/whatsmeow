package whatsmeow

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"

	"google.golang.org/protobuf/proto"

	waBinary "go.mau.fi/whatsmeow/binary"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	waLog "go.mau.fi/whatsmeow/util/log"
)

// recordingLogger keeps every formatted Warnf line, so a test can see that an
// ack was attempted (with no socket, sendAck logs "Failed to send acknowledgement").
type recordingLogger struct {
	mu    sync.Mutex
	warns []string
}

func (l *recordingLogger) Warnf(msg string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.warns = append(l.warns, fmt.Sprintf(msg, args...))
}
func (l *recordingLogger) Errorf(string, ...any)   {}
func (l *recordingLogger) Infof(string, ...any)    {}
func (l *recordingLogger) Debugf(string, ...any)   {}
func (l *recordingLogger) Sub(string) waLog.Logger { return l }

func (l *recordingLogger) has(substr string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, w := range l.warns {
		if strings.Contains(w, substr) {
			return true
		}
	}
	return false
}

// statusRoutingClient builds a client through NewClient, so the test goes through
// the real nodeHandlers table: the "status" entry is the line upstream 06b1c29
// changed, and a merge can flip it back silently.
func statusRoutingClient(t *testing.T) (*Client, *recordingLogger, *[]any) {
	t.Helper()
	noop := store.NoopDevice
	own := types.NewADJID("2340000000000", 0, 3)
	dev := &store.Device{
		ID:          &own,
		LID:         types.NewJID("999999999999999", types.HiddenUserServer),
		NoiseKey:    noop.NoiseKey,
		IdentityKey: noop.IdentityKey,

		Identities:    noop.Identities,
		Sessions:      noop.Sessions,
		PreKeys:       noop.PreKeys,
		SenderKeys:    noop.SenderKeys,
		AppStateKeys:  noop.AppStateKeys,
		AppState:      noop.AppState,
		Contacts:      noop.Contacts,
		ChatSettings:  noop.ChatSettings,
		MsgSecrets:    noop.MsgSecrets,
		PrivacyTokens: noop.PrivacyTokens,
		NCTSalt:       noop.NCTSalt,
		EventBuffer:   noop.EventBuffer,
		LIDs:          noop.LIDs,
		Container:     noop.Container,
	}
	log := &recordingLogger{}
	cli := NewClient(dev, log)
	var mu sync.Mutex
	var dispatched []any
	cli.AddEventHandler(func(evt any) {
		mu.Lock()
		defer mu.Unlock()
		dispatched = append(dispatched, evt)
	})
	return cli, log, &dispatched
}

func statusStanza(from types.JID, participant types.JID, content ...waBinary.Node) *waBinary.Node {
	attrs := waBinary.Attrs{"from": from, "id": "3EB0STATUSROUTING", "t": "1794039607", "type": "text"}
	if !participant.IsEmpty() {
		attrs["participant"] = participant
	}
	return &waBinary.Node{Tag: "status", Attrs: attrs, Content: content}
}

// Deliberately NOT upstream 06b1c29: a contact status arriving as <status><enc> is
// acked and dropped, with no decrypt attempt and no event, exactly as before the
// sync. See .scratch/whatsmeow-upstream-2026-11/01-findings.md, finding 1.
func TestStatusStanzaFromContactIsAckedAndDropped(t *testing.T) {
	cli, log, dispatched := statusRoutingClient(t)
	node := statusStanza(types.StatusBroadcastJID, types.NewJID("270578695069938", types.HiddenUserServer),
		waBinary.Node{Tag: "enc", Attrs: waBinary.Attrs{"type": "msg", "v": "2"}, Content: []byte("not a real signal message")})
	cli.nodeHandlers["status"](context.Background(), node)
	if len(*dispatched) != 0 {
		t.Fatalf("contact status was dispatched (%d events, first %T); it must be acked and dropped", len(*dispatched), (*dispatched)[0])
	}
	if !log.has("Failed to send acknowledgement for status 3EB0STATUSROUTING") {
		t.Fatalf("contact status was not acked; warnings: %v", log.warns)
	}
	if log.has("Error decrypting message") {
		t.Fatal("contact status went through decryption")
	}
}

// A group status still goes through the decrypt path. With no sender key in a noop
// store it fails, which proves it was attempted and was flagged as a group status.
func TestStatusStanzaFromGroupIsDecrypted(t *testing.T) {
	cli, log, dispatched := statusRoutingClient(t)
	group := types.NewJID("120363414701935822", types.GroupServer)
	node := statusStanza(group, types.NewJID("270578695069938", types.HiddenUserServer),
		waBinary.Node{Tag: "enc", Attrs: waBinary.Attrs{"type": "skmsg", "v": "2"}, Content: []byte("not a real sender key message")})
	cli.nodeHandlers["status"](context.Background(), node)
	if !log.has("Error decrypting message 3EB0STATUSROUTING") {
		t.Fatalf("group status was not decrypted; warnings: %v", log.warns)
	}
	var got *events.UndecryptableMessage
	for _, evt := range *dispatched {
		if u, ok := evt.(*events.UndecryptableMessage); ok {
			got = u
		}
	}
	if got == nil {
		t.Fatalf("no UndecryptableMessage dispatched; got %v", *dispatched)
	}
	if !got.Info.IsGroupStatus || got.Info.StanzaTag != "status" {
		t.Fatalf("IsGroupStatus=%v StanzaTag=%q, want true/status", got.Info.IsGroupStatus, got.Info.StanzaTag)
	}
}

// A newsletter status is still handled as plaintext and flagged.
func TestStatusStanzaFromNewsletterIsPlaintext(t *testing.T) {
	cli, _, dispatched := statusRoutingClient(t)
	plaintext, err := proto.Marshal(&waE2E.Message{Conversation: proto.String("channel status")})
	if err != nil {
		t.Fatal(err)
	}
	node := statusStanza(types.NewJID("120363411617137121", types.NewsletterServer), types.EmptyJID,
		waBinary.Node{Tag: "plaintext", Content: plaintext})
	node.Attrs["server_id"] = "101"
	cli.nodeHandlers["status"](context.Background(), node)
	var got *events.Message
	for _, evt := range *dispatched {
		if m, ok := evt.(*events.Message); ok {
			got = m
		}
	}
	if got == nil {
		t.Fatalf("newsletter status not dispatched; got %v", *dispatched)
	}
	if !got.Info.IsNewsletterStatus || got.Info.StanzaTag != "status" || got.Message.GetConversation() != "channel status" {
		t.Fatalf("IsNewsletterStatus=%v StanzaTag=%q text=%q", got.Info.IsNewsletterStatus, got.Info.StanzaTag, got.Message.GetConversation())
	}
}

// The message ID of a grouped receipt: message_id first, then key, then the
// parent <receipt> id (newer status receipts carry neither attribute). Upstream
// 52f1408 prefers key over message_id and has no parent fallback, so a merge
// that takes it changes the two cases marked below.
func TestHandleGroupedReceiptMessageIDFallback(t *testing.T) {
	cases := []struct {
		name  string
		attrs waBinary.Attrs
		want  types.MessageID
	}{
		{"message_id only", waBinary.Attrs{"message_id": "MSGID"}, "MSGID"},
		{"key only", waBinary.Attrs{"key": "KEYID"}, "KEYID"},
		{"both: message_id wins (upstream picks key)", waBinary.Attrs{"message_id": "MSGID", "key": "KEYID"}, "MSGID"},
		{"neither: parent id (upstream gives empty)", waBinary.Attrs{}, "3C089EAAEBF3B5C36529"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			cli, _ := testClient(t)
			var got []*events.Receipt
			cli.AddEventHandler(func(evt any) {
				if r, ok := evt.(*events.Receipt); ok {
					got = append(got, r)
				}
			})
			node := &waBinary.Node{
				Tag: "receipt",
				Attrs: waBinary.Attrs{
					"from": types.StatusBroadcastJID,
					"id":   "3C089EAAEBF3B5C36529",
					"t":    "1780925302",
				},
				Content: []waBinary.Node{{
					Tag:   "participants",
					Attrs: c.attrs,
					Content: []waBinary.Node{
						{Tag: "user", Attrs: waBinary.Attrs{"jid": types.NewJID("111111111111111", types.HiddenUserServer), "t": "1780925303", "type": "read"}},
						{Tag: "user", Attrs: waBinary.Attrs{"jid": types.NewJID("222222222222222", types.HiddenUserServer), "t": "1780925304", "type": "delivery"}},
					},
				}},
			}
			cli.handleReceipt(context.Background(), node)
			if len(got) != 2 {
				t.Fatalf("expected 2 child receipts, got %d", len(got))
			}
			for i, r := range got {
				if len(r.MessageIDs) != 1 || r.MessageIDs[0] != c.want {
					t.Errorf("child %d MessageIDs = %v, want [%s]", i, r.MessageIDs, c.want)
				}
				if !r.SenderAlt.IsEmpty() {
					t.Errorf("child %d has SenderAlt %s", i, r.SenderAlt)
				}
			}
			if got[0].Type != types.ReceiptTypeRead || got[1].Type != types.ReceiptTypeDelivered {
				t.Errorf("per-user types = %q, %q; want read, delivered", got[0].Type, got[1].Type)
			}
		})
	}
}
