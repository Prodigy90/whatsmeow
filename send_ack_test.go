package whatsmeow

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"

	waBinary "go.mau.fi/whatsmeow/binary"
	"go.mau.fi/whatsmeow/proto/waCommon"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	waLog "go.mau.fi/whatsmeow/util/log"
)

// Ack shapes captured live on 2026-10-12 (Harness Group): every delivered group
// send carried count="N" (the other members), a group with only us acked
// count="0", and a non-admin send into an announce-only group acked with no
// error and no count, and was never delivered.
func TestParseSendAck(t *testing.T) {
	group := types.NewJID("120363000000000001", types.GroupServer)
	dm := types.NewJID("2348011111111", types.DefaultUserServer)
	status := types.StatusBroadcastJID
	text := &waE2E.Message{Conversation: proto.String("hi")}

	ack := func(attrs waBinary.Attrs) *waBinary.Node {
		base := waBinary.Attrs{"class": "message", "id": "3EB0TEST", "t": "1760270000"}
		for k, v := range attrs {
			base[k] = v
		}
		return &waBinary.Node{Tag: "ack", Attrs: base}
	}
	intPtr := func(n int) *int { return &n }

	cases := []struct {
		name      string
		to        types.JID
		attrs     waBinary.Attrs
		dropErr   bool // Client.GroupSendDropAsError
		wantCount *int
		wantErr   error
		wantWarn  bool // "group send ack without count"
	}{
		{"group delivered", group, waBinary.Attrs{"count": "3", "phash": "2:abc"}, true, intPtr(3), nil, false},
		{"group with only us acks count=0", group, waBinary.Attrs{"count": "0"}, true, intPtr(0), nil, false},
		{"group dropped, flag off: success", group, waBinary.Attrs{"phash": "2:abc"}, false, nil, nil, true},
		{"group dropped, flag on: ErrGroupSendDropped", group, waBinary.Attrs{"phash": "2:abc"}, true, nil, ErrGroupSendDropped, true},
		{"group error wins over missing count", group, waBinary.Attrs{"error": "403"}, true, nil, ErrServerReturnedError, false},
		{"dm without count is not a drop", dm, waBinary.Attrs{}, true, nil, nil, false},
		{"status broadcast without count is not a drop", status, waBinary.Attrs{"phash": "2:abc"}, true, nil, nil, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			log := &warnRecorder{}
			cli := &Client{Log: log, GroupSendDropAsError: tc.dropErr}
			var resp SendResponse
			err := cli.parseSendAck(ack(tc.attrs), tc.to, "3EB0TEST", "2:abc", text, &resp)

			if tc.wantErr == nil && err != nil {
				t.Fatalf("err = %v, want nil", err)
			}
			if tc.wantErr != nil && !errors.Is(err, tc.wantErr) {
				t.Fatalf("err = %v, want %v", err, tc.wantErr)
			}
			switch {
			case tc.wantCount == nil && resp.ServerCount != nil:
				t.Fatalf("ServerCount = %d, want nil (ack had no count)", *resp.ServerCount)
			case tc.wantCount != nil && resp.ServerCount == nil:
				t.Fatalf("ServerCount = nil, want %d", *tc.wantCount)
			case tc.wantCount != nil && *resp.ServerCount != *tc.wantCount:
				t.Fatalf("ServerCount = %d, want %d", *resp.ServerCount, *tc.wantCount)
			}
			if resp.Timestamp.IsZero() {
				t.Fatal("Timestamp not parsed from the ack")
			}
			if got := log.has("group send ack without count"); got != tc.wantWarn {
				t.Fatalf("drop warn logged = %t, want %t (warns: %q)", got, tc.wantWarn, log.warns)
			}
		})
	}
}

// The drop check is scoped to plain content sends: a missing `count` was only observed (and only shown to
// mean "dropped") for those. Actions on an existing message never warn or fail on it, flag on or off, but
// still get ServerCount when the ack carries one.
func TestParseSendAckDropScope(t *testing.T) {
	group := types.NewJID("120363000000000001", types.GroupServer)
	target := &waCommon.MessageKey{
		RemoteJID: proto.String(group.String()),
		FromMe:    proto.Bool(true),
		ID:        proto.String("3EB0TARGET"),
	}
	othersTarget := &waCommon.MessageKey{
		RemoteJID:   proto.String(group.String()),
		FromMe:      proto.Bool(false),
		ID:          proto.String("3EB0OTHER"),
		Participant: proto.String("2348022222222@s.whatsapp.net"),
	}
	text := &waE2E.Message{Conversation: proto.String("hi")}

	cases := []struct {
		name  string
		msg   *waE2E.Message
		plain bool
	}{
		{"plain text", text, true},
		{"extended text", &waE2E.Message{ExtendedTextMessage: &waE2E.ExtendedTextMessage{Text: proto.String("hi")}}, true},
		{"image", &waE2E.Message{ImageMessage: &waE2E.ImageMessage{Mimetype: proto.String("image/jpeg")}}, true},
		{"poll creation", &waE2E.Message{PollCreationMessage: &waE2E.PollCreationMessage{Name: proto.String("q")}}, true},
		{"ephemeral-wrapped text", &waE2E.Message{EphemeralMessage: &waE2E.FutureProofMessage{Message: text}}, true},
		{"reaction", &waE2E.Message{ReactionMessage: &waE2E.ReactionMessage{Key: target, Text: proto.String("👍")}}, false},
		{"reaction removal", &waE2E.Message{ReactionMessage: &waE2E.ReactionMessage{Key: target, Text: proto.String("")}}, false},
		{"ephemeral-wrapped reaction", &waE2E.Message{EphemeralMessage: &waE2E.FutureProofMessage{
			Message: &waE2E.Message{ReactionMessage: &waE2E.ReactionMessage{Key: target, Text: proto.String("👍")}}}}, false},
		{"enc reaction", &waE2E.Message{EncReactionMessage: &waE2E.EncReactionMessage{TargetMessageKey: target}}, false},
		{"pin", &waE2E.Message{PinInChatMessage: &waE2E.PinInChatMessage{Key: target, Type: waE2E.PinInChatMessage_PIN_FOR_ALL.Enum()}}, false},
		{"sender revoke", &waE2E.Message{ProtocolMessage: &waE2E.ProtocolMessage{Key: target, Type: waE2E.ProtocolMessage_REVOKE.Enum()}}, false},
		{"admin revoke", &waE2E.Message{ProtocolMessage: &waE2E.ProtocolMessage{Key: othersTarget, Type: waE2E.ProtocolMessage_REVOKE.Enum()}}, false},
		{"edit", &waE2E.Message{EditedMessage: &waE2E.FutureProofMessage{Message: &waE2E.Message{ProtocolMessage: &waE2E.ProtocolMessage{
			Key: target, Type: waE2E.ProtocolMessage_MESSAGE_EDIT.Enum(), EditedMessage: text}}}}, false},
		{"ephemeral-wrapped poll vote", &waE2E.Message{EphemeralMessage: &waE2E.FutureProofMessage{
			Message: &waE2E.Message{PollUpdateMessage: &waE2E.PollUpdateMessage{PollCreationMessageKey: target}}}}, false},
		{"poll vote", &waE2E.Message{PollUpdateMessage: &waE2E.PollUpdateMessage{PollCreationMessageKey: target}}, false},
		{"keep in chat", &waE2E.Message{KeepInChatMessage: &waE2E.KeepInChatMessage{Key: target, KeepType: waE2E.KeepType_KEEP_FOR_ALL.Enum()}}, false},
		{"keyless protocol message", &waE2E.Message{ProtocolMessage: &waE2E.ProtocolMessage{Type: waE2E.ProtocolMessage_EPHEMERAL_SETTING.Enum()}}, false},
	}
	noCount := func() *waBinary.Node {
		return &waBinary.Node{Tag: "ack", Attrs: waBinary.Attrs{"class": "message", "id": "3EB0TEST", "t": "1760270000", "phash": "2:abc"}}
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isPlainContentSend(tc.msg); got != tc.plain {
				t.Fatalf("isPlainContentSend = %t, want %t", got, tc.plain)
			}
			for _, flag := range []bool{false, true} {
				log := &warnRecorder{}
				cli := &Client{Log: log, GroupSendDropAsError: flag}
				var resp SendResponse
				err := cli.parseSendAck(noCount(), group, "3EB0TEST", "2:abc", tc.msg, &resp)
				wantErr := tc.plain && flag
				if wantErr && !errors.Is(err, ErrGroupSendDropped) {
					t.Fatalf("flag=%t: err = %v, want ErrGroupSendDropped", flag, err)
				}
				if !wantErr && err != nil {
					t.Fatalf("flag=%t: err = %v, want nil", flag, err)
				}
				if got := log.has("group send ack without count"); got != tc.plain {
					t.Fatalf("flag=%t: drop warn logged = %t, want %t (warns: %q)", flag, got, tc.plain, log.warns)
				}
			}

			// A count on the ack is exposed for every kind of send.
			cli := &Client{Log: &warnRecorder{}, GroupSendDropAsError: true}
			node := noCount()
			node.Attrs["count"] = "1"
			var resp SendResponse
			if err := cli.parseSendAck(node, group, "3EB0TEST", "2:abc", tc.msg, &resp); err != nil {
				t.Fatalf("with count: err = %v, want nil", err)
			}
			if resp.ServerCount == nil || *resp.ServerCount != 1 {
				t.Fatalf("with count: ServerCount = %v, want 1", resp.ServerCount)
			}
		})
	}
}

func TestParseSendAckErrorKeepsDetail(t *testing.T) {
	cli := &Client{Log: waLog.Noop}
	group := types.NewJID("120363000000000001", types.GroupServer)
	node := &waBinary.Node{Tag: "ack", Attrs: waBinary.Attrs{"class": "message", "id": "X", "t": "1760270000", "error": "429", "backoff": "30"}}
	var resp SendResponse
	err := cli.parseSendAck(node, group, "X", "", &waE2E.Message{Conversation: proto.String("hi")}, &resp)
	if !errors.Is(err, ErrServerReturnedError) || err.Error() != "server returned error 429" {
		t.Fatalf("err = %v, want server returned error 429", err)
	}
	if resp.BackoffSeconds != 30 || resp.RawErrorResponse == "" {
		t.Fatalf("backoff=%d raw=%q, want 30 and the raw ack", resp.BackoffSeconds, resp.RawErrorResponse)
	}
}

type warnRecorder struct{ warns []string }

func (w *warnRecorder) Warnf(msg string, args ...any) {
	w.warns = append(w.warns, fmt.Sprintf(msg, args...))
}
func (w *warnRecorder) Errorf(string, ...any)   {}
func (w *warnRecorder) Infof(string, ...any)    {}
func (w *warnRecorder) Debugf(string, ...any)   {}
func (w *warnRecorder) Sub(string) waLog.Logger { return w }
func (w *warnRecorder) has(s string) bool {
	for _, m := range w.warns {
		if strings.Contains(m, s) {
			return true
		}
	}
	return false
}
