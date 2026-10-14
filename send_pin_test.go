package whatsmeow

import (
	"testing"

	"google.golang.org/protobuf/proto"

	"go.mau.fi/whatsmeow/proto/waCommon"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
)

// The pin WA Web sent into a group on 2026-10-12 (wa-web-harness capture
// stanzas-121026-193140.jsonl): payload
//
//	pinInChatMessage { key { remoteJid, fromMe: true, id, participant: "<our LID>@lid" },
//	                   type: PIN_FOR_ALL, senderTimestampMs }
//	messageContextInfo { messageAddOnDurationInSecs: 604800 }
//
// and stanza
//
//	<message type="text" edit="2" addressing_mode="lid"><enc v="2" type="skmsg" decrypt-fail="hide"/></message>
//
// with no <reporting> child.
func capturedPin() *waE2E.Message {
	return &waE2E.Message{
		PinInChatMessage: &waE2E.PinInChatMessage{
			Key: &waCommon.MessageKey{
				RemoteJID:   proto.String("120363039512177531@g.us"),
				FromMe:      proto.Bool(true),
				ID:          proto.String("3EB0868E438A24A6AC18DB"),
				Participant: proto.String("31482697482258@lid"),
			},
			Type:              waE2E.PinInChatMessage_PIN_FOR_ALL.Enum(),
			SenderTimestampMS: proto.Int64(1791830161644),
		},
		MessageContextInfo: &waE2E.MessageContextInfo{MessageAddOnDurationInSecs: proto.Uint32(604800)},
	}
}

func TestPinStanzaAttributes(t *testing.T) {
	pin := capturedPin()
	if got := getEditAttribute(pin); got != types.EditAttributePinInChat {
		t.Fatalf("edit attribute = %q, want %q", got, types.EditAttributePinInChat)
	}
	if got := getTypeFromMessage(pin); got != "text" {
		t.Fatalf("message type = %q, want text", got)
	}
	attrs := senderKeyEncAttrs(pin)
	want := map[string]string{"v": "2", "type": "skmsg", "decrypt-fail": "hide"}
	if len(attrs) != len(want) {
		t.Fatalf("skmsg attrs = %v, want exactly %v", attrs, want)
	}
	for k, v := range want {
		if attrs[k] != v {
			t.Fatalf("skmsg attr %s = %v, want %q (all: %v)", k, attrs[k], v, attrs)
		}
	}

	unpin := capturedPin()
	unpin.PinInChatMessage.Type = waE2E.PinInChatMessage_UNPIN_FOR_ALL.Enum()
	unpin.MessageContextInfo.MessageAddOnDurationInSecs = proto.Uint32(0)
	if senderKeyEncAttrs(unpin)["decrypt-fail"] != "hide" {
		t.Fatal("an unpin is a pin-in-chat message too: its skmsg must carry decrypt-fail=hide")
	}
}

// WA Web's admin revoke (edit="8") in the same capture went out with a bare
// skmsg, and plain sends never carry decrypt-fail: only the pin gets it.
func TestSenderKeyEncAttrsLeavesOtherMessagesAlone(t *testing.T) {
	key := &waCommon.MessageKey{RemoteJID: proto.String("120363039512177531@g.us"), FromMe: proto.Bool(false),
		ID: proto.String("3EB0OTHER"), Participant: proto.String("249864101339368@lid")}
	cases := map[string]*waE2E.Message{
		"text":         {Conversation: proto.String("hi")},
		"admin revoke": {ProtocolMessage: &waE2E.ProtocolMessage{Type: waE2E.ProtocolMessage_REVOKE.Enum(), Key: key}},
		"image":        {ImageMessage: &waE2E.ImageMessage{Mimetype: proto.String("image/jpeg")}},
	}
	for name, msg := range cases {
		if v, ok := senderKeyEncAttrs(msg)["decrypt-fail"]; ok {
			t.Fatalf("%s: skmsg has decrypt-fail=%v, want none", name, v)
		}
	}
	if senderKeyEncAttrs(cases["image"])["mediatype"] != "image" {
		t.Fatal("image skmsg lost its mediatype")
	}
}

func TestPinCarriesNoReportingToken(t *testing.T) {
	cli := &Client{SendReportingTokens: true}
	if cli.shouldIncludeReportingToken(capturedPin()) {
		t.Fatal("a pin must not get a reporting token (or the message secret SendMessage adds for one)")
	}
	if !cli.shouldIncludeReportingToken(&waE2E.Message{Conversation: proto.String("hi")}) {
		t.Fatal("plain text should still carry a reporting token")
	}
}
