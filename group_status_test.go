package whatsmeow

import (
	"testing"

	"google.golang.org/protobuf/proto"

	waBinary "go.mau.fi/whatsmeow/binary"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
)

func wrapGroupStatus(inner *waE2E.Message) *waE2E.Message {
	return &waE2E.Message{GroupStatusMessageV2: &waE2E.FutureProofMessage{Message: inner}}
}

// The stanza type/mediatype attrs must come from the wrapped content: the phone sends an image
// group status as type="media" with <enc mediatype="image">.
func TestGroupStatusTypeLooksThroughWrapper(t *testing.T) {
	img := wrapGroupStatus(&waE2E.Message{ImageMessage: &waE2E.ImageMessage{Caption: proto.String("x")}})
	if got := getTypeFromMessage(img); got != "media" {
		t.Errorf("type for wrapped image = %q, want media", got)
	}
	if got := getMediaTypeFromMessage(img); got != "image" {
		t.Errorf("mediatype for wrapped image = %q, want image", got)
	}
	text := wrapGroupStatus(&waE2E.Message{ExtendedTextMessage: &waE2E.ExtendedTextMessage{Text: proto.String("hi")}})
	if got := getTypeFromMessage(text); got != "text" {
		t.Errorf("type for wrapped text = %q, want text", got)
	}
	if got := getMediaTypeFromMessage(text); got != "" {
		t.Errorf("mediatype for wrapped text = %q, want empty", got)
	}
}

func TestGroupStatusReceiptClass(t *testing.T) {
	group := types.NewJID("120363413717217465", types.GroupServer)
	newsletter := types.NewJID("120363411617137121", types.NewsletterServer)
	cases := []struct {
		name string
		tag  string
		from types.JID
		want any
	}{
		{"group status", "status", group, "status"},
		{"group message", "message", group, nil},
		{"newsletter status", "status", newsletter, nil},
	}
	for _, c := range cases {
		node := &waBinary.Node{Tag: c.tag, Attrs: waBinary.Attrs{"from": c.from, "id": "X"}}
		got, ok := buildBaseReceipt("X", node)["class"]
		if c.want == nil && ok {
			t.Errorf("%s: unexpected class %v", c.name, got)
		} else if c.want != nil && got != c.want {
			t.Errorf("%s: class = %v, want %v", c.name, got, c.want)
		}
	}
}

func TestUnwrapRawGroupStatus(t *testing.T) {
	inner := &waE2E.Message{ExtendedTextMessage: &waE2E.ExtendedTextMessage{
		Text:        proto.String("Hi, Just testing"),
		ContextInfo: &waE2E.ContextInfo{IsGroupStatus: proto.Bool(true)},
	}}
	evt := (&events.Message{RawMessage: wrapGroupStatus(inner)}).UnwrapRaw()
	if evt.Message.GetExtendedTextMessage().GetText() != "Hi, Just testing" {
		t.Fatalf("UnwrapRaw did not unwrap groupStatusMessageV2: %v", evt.Message)
	}
	if !evt.Message.GetExtendedTextMessage().GetContextInfo().GetIsGroupStatus() {
		t.Error("inner contextInfo lost")
	}
}

func groupStatusTestClient() *Client {
	own := types.NewADJID("2348105300696", 0, 14)
	return &Client{Store: &store.Device{ID: &own, LID: types.NewADJID("31482697482258", 0, 14).ToNonAD()}}
}

// Our own devices' copy of a group status (and every send about one) is a group <message>
// with <meta is_group_status="true">; it must come out flagged, or the worker gate misses it.
func TestParseMessageInfoMetaIsGroupStatus(t *testing.T) {
	cli := groupStatusTestClient()
	group := types.NewJID("120363414701935822", types.GroupServer)
	mk := func(meta waBinary.Attrs) *waBinary.Node {
		return &waBinary.Node{Tag: "message", Attrs: waBinary.Attrs{
			"from": group, "id": "3CEE8D4CEDAA44EDA181A70A2DC4D0E7", "t": "1794039607", "type": "media",
			"participant": types.NewADJID("31482697482258", 0, 9).ToNonAD(), "addressing_mode": "lid",
		}, Content: []waBinary.Node{{Tag: "meta", Attrs: meta}}}
	}
	info, err := cli.parseMessageInfo(mk(waBinary.Attrs{"is_group_status": "true"}))
	if err != nil {
		t.Fatal(err)
	}
	if !info.IsGroupStatus {
		t.Error("meta is_group_status=true not flagged")
	}
	info, err = cli.parseMessageInfo(mk(waBinary.Attrs{"appdata": "default"}))
	if err != nil {
		t.Fatal(err)
	}
	if info.IsGroupStatus {
		t.Error("plain group message flagged as group status")
	}
}

func TestRetryExtraParamsKeepGroupStatusMeta(t *testing.T) {
	gs := wrapGroupStatus(&waE2E.Message{ExtendedTextMessage: &waE2E.ExtendedTextMessage{Text: proto.String("hi")}})
	p := retryExtraParams(gs)
	if p.metaNode == nil || p.metaNode.Attrs["is_group_status"] != "true" {
		t.Fatalf("group status resend lost its meta flag: %+v", p.metaNode)
	}
	if p := retryExtraParams(&waE2E.Message{Conversation: proto.String("hi")}); p.metaNode != nil {
		t.Errorf("plain message resend got a meta node: %+v", p.metaNode)
	}
}
