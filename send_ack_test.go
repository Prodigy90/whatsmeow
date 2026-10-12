package whatsmeow

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"

	waBinary "go.mau.fi/whatsmeow/binary"
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
