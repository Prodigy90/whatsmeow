package whatsmeow

import (
	"errors"
	"testing"

	"google.golang.org/protobuf/proto"

	"go.mau.fi/whatsmeow/proto/waCommon"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"

	waBinary "go.mau.fi/whatsmeow/binary"
)

var testChannel = types.NewJID("120363411617137121", types.NewsletterServer)

func plaintextChild(t *testing.T, node waBinary.Node) waBinary.Node {
	t.Helper()
	children := node.GetChildren()
	if len(children) != 1 || children[0].Tag != "plaintext" {
		t.Fatalf("want one <plaintext> child, got %v", node)
	}
	return children[0]
}

// Captured 2026-11-20 (.scratch/channel-status/03): a text channel status is a top-level
// <status to=…@newsletter type="text"><plaintext>proto</plaintext></status>.
func TestNewsletterStatusText(t *testing.T) {
	msg := &waE2E.Message{ExtendedTextMessage: &waE2E.ExtendedTextMessage{Text: proto.String("hello followers")}}
	node, err := buildNewsletterNode(testChannel, "3EB0AAAA", msg, "", true)
	if err != nil {
		t.Fatal(err)
	}
	if node.Tag != "status" {
		t.Fatalf("root tag = %q, want status", node.Tag)
	}
	ag := node.AttrGetter()
	if ag.String("type") != "text" || ag.String("id") != "3EB0AAAA" || ag.JID("to") != testChannel {
		t.Fatalf("attrs = %v", node.Attrs)
	}
	if _, ok := node.Attrs["media_id"]; ok {
		t.Fatal("text status must not carry media_id")
	}
	pt := plaintextChild(t, node)
	if _, ok := pt.Attrs["mediatype"]; ok {
		t.Fatal("text status must not carry mediatype")
	}
	var got waE2E.Message
	if err := proto.Unmarshal(pt.Content.([]byte), &got); err != nil {
		t.Fatal(err)
	}
	if got.GetExtendedTextMessage().GetText() != "hello followers" {
		t.Fatalf("payload = %v", &got)
	}
}

// Captured 2026-11-20: type="media" media_id="wa_channel_video/flat/<hex>", <plaintext mediatype="video">.
func TestNewsletterStatusMedia(t *testing.T) {
	for _, tc := range []struct {
		msg  *waE2E.Message
		want string
	}{
		{&waE2E.Message{VideoMessage: &waE2E.VideoMessage{Caption: proto.String("interesting")}}, "video"},
		{&waE2E.Message{ImageMessage: &waE2E.ImageMessage{}}, "image"},
	} {
		node, err := buildNewsletterNode(testChannel, "3EB0BBBB", tc.msg, "wa_channel_"+tc.want+"/flat/abc", true)
		if err != nil {
			t.Fatal(err)
		}
		ag := node.AttrGetter()
		if node.Tag != "status" || ag.String("type") != "media" || ag.String("media_id") != "wa_channel_"+tc.want+"/flat/abc" {
			t.Fatalf("%s: node = %v", tc.want, node)
		}
		pt := plaintextChild(t, node)
		if mt := pt.AttrGetter().String("mediatype"); mt != tc.want {
			t.Fatalf("mediatype = %q, want %q", mt, tc.want)
		}
	}
}

// Captured 2026-11-20: a delete is the same tag with edit="8" and an empty <plaintext/>.
func TestNewsletterStatusRevoke(t *testing.T) {
	revoke := &waE2E.Message{ProtocolMessage: &waE2E.ProtocolMessage{
		Type: waE2E.ProtocolMessage_REVOKE.Enum(),
		Key:  &waCommon.MessageKey{RemoteJID: proto.String(testChannel.String()), FromMe: proto.Bool(true), ID: proto.String("3EB0CCCC")},
	}}
	node, err := buildNewsletterNode(testChannel, "3EB0CCCC", revoke, "", true)
	if err != nil {
		t.Fatal(err)
	}
	ag := node.AttrGetter()
	if node.Tag != "status" || ag.String("edit") != string(types.EditAttributeAdminRevoke) || ag.String("type") != "text" {
		t.Fatalf("node = %v", node)
	}
	if c := plaintextChild(t, node).Content; c != nil {
		if b, ok := c.([]byte); !ok || len(b) != 0 {
			t.Fatalf("revoke plaintext must be empty, got %v", c)
		}
	}
}

// WhatsApp Web only posts text, image and video to a channel status; other media fails before the wire.
// The feed post path keeps accepting it.
func TestNewsletterStatusRejectsOtherMedia(t *testing.T) {
	doc := &waE2E.Message{DocumentMessage: &waE2E.DocumentMessage{}}
	if _, err := buildNewsletterNode(testChannel, "3EB0DDDD", doc, "h", true); !errors.Is(err, ErrNewsletterStatusMediaType) {
		t.Fatalf("status err = %v, want ErrNewsletterStatusMediaType", err)
	}
	node, err := buildNewsletterNode(testChannel, "3EB0DDDD", doc, "h", false)
	if err != nil || node.Tag != "message" {
		t.Fatalf("feed post: node=%v err=%v", node, err)
	}
}

// Without the flag nothing changes for channel posts.
func TestNewsletterPostStaysMessage(t *testing.T) {
	msg := &waE2E.Message{Conversation: proto.String("feed")}
	node, err := buildNewsletterNode(testChannel, "3EB0EEEE", msg, "", false)
	if err != nil || node.Tag != "message" || node.AttrGetter().String("type") != "text" {
		t.Fatalf("node=%v err=%v", node, err)
	}
}

// Response shape from WAWebMexFetchNewsletterAdminCapabilitiesJobQuery: capability NAMES, not numbers.
func TestParseNewsletterAdminCapabilities(t *testing.T) {
	caps, err := parseNewsletterAdminCapabilities([]byte(`{"xwa2_newsletter_admin":{"capabilities":["INSIGHTS","PHOTO_POLLS","CHANNEL_STATUS_PRODUCER","PINNED_MESSAGES"],"id":"120363411617137121@newsletter"}}`))
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, c := range caps {
		found = found || c == NewsletterCapabilityChannelStatusProducer
	}
	if len(caps) != 4 || !found {
		t.Fatalf("caps = %v", caps)
	}
	caps, err = parseNewsletterAdminCapabilities([]byte(`{"xwa2_newsletter_admin":{"capabilities":[],"id":"x"}}`))
	if err != nil || len(caps) != 0 {
		t.Fatalf("empty: caps=%v err=%v", caps, err)
	}
	if _, err = parseNewsletterAdminCapabilities([]byte(`{"xwa2_newsletter_admin":null}`)); err == nil {
		t.Fatal("a missing admin object must be an error, not an empty capability list")
	}
}
