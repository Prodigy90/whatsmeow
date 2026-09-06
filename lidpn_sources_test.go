package whatsmeow

import (
	"context"
	"sync"
	"testing"

	"go.mau.fi/whatsmeow/appstate"
	waBinary "go.mau.fi/whatsmeow/binary"
	"go.mau.fi/whatsmeow/proto/waServerSync"
	"go.mau.fi/whatsmeow/proto/waSyncAction"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	waLog "go.mau.fi/whatsmeow/util/log"
)

// recordingLIDStore captures PutLIDMapping calls so a test can assert on the
// exact pairs a code path tried to persist.
type recordingLIDStore struct {
	store.LIDStore
	mu  sync.Mutex
	put [][2]types.JID
}

func (r *recordingLIDStore) PutLIDMapping(ctx context.Context, lid, pn types.JID) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.put = append(r.put, [2]types.JID{lid, pn})
	return nil
}

func (r *recordingLIDStore) pairs() [][2]types.JID {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([][2]types.JID(nil), r.put...)
}

func testClient(t *testing.T) (*Client, *recordingLIDStore) {
	t.Helper()
	ownPN := types.NewJID("2340000000000", types.DefaultUserServer)
	rec := &recordingLIDStore{}
	return &Client{
		Store: &store.Device{
			ID:   &ownPN,
			LID:  types.NewJID("999999999999999", types.HiddenUserServer),
			LIDs: rec,
		},
		Log: waLog.Noop,
	}, rec
}

// The exact shape captured off the wire on 2026-09-06: a status broadcast
// receipt carrying participant + participant_pn and NO addressing_mode.
func statusReceiptNode(children ...waBinary.Node) *waBinary.Node {
	n := &waBinary.Node{
		Tag: "receipt",
		Attrs: waBinary.Attrs{
			"from":           types.StatusBroadcastJID,
			"type":           "read",
			"id":             "3C089EAAEBF3B5C36529",
			"participant":    types.NewJID("270578695069938", types.HiddenUserServer),
			"participant_pn": types.NewJID("2348101849778", types.DefaultUserServer),
			"offline":        "0",
			"t":              "1780925302",
		},
	}
	if len(children) > 0 {
		n.Content = children
	}
	return n
}

func TestParseMessageSource_ReceiptPNSurvivesMissingAddressingMode(t *testing.T) {
	cli, _ := testClient(t)
	src, err := cli.parseMessageSource(statusReceiptNode(), false)
	if err != nil {
		t.Fatalf("parseMessageSource: %v", err)
	}
	if src.Sender.Server != types.HiddenUserServer {
		t.Fatalf("expected a LID sender, got %s", src.Sender)
	}
	if src.SenderAlt.User != "2348101849778" || src.SenderAlt.Server != types.DefaultUserServer {
		t.Fatalf("participant_pn was dropped: SenderAlt=%q (want 2348101849778@s.whatsapp.net)", src.SenderAlt.String())
	}
}

// Regression guard: when WhatsApp DOES send addressing_mode=lid, behaviour is unchanged.
func TestParseMessageSource_ExplicitLIDAddressingModeStillReadsPN(t *testing.T) {
	cli, _ := testClient(t)
	node := statusReceiptNode()
	node.Attrs["addressing_mode"] = "lid"
	src, err := cli.parseMessageSource(node, false)
	if err != nil {
		t.Fatalf("parseMessageSource: %v", err)
	}
	if src.SenderAlt.User != "2348101849778" {
		t.Fatalf("SenderAlt=%q, want 2348101849778", src.SenderAlt.String())
	}
}

// A PN participant must read participant_lid, never participant_pn.
func TestParseMessageSource_PNParticipantReadsLIDAttr(t *testing.T) {
	cli, _ := testClient(t)
	node := &waBinary.Node{
		Tag: "receipt",
		Attrs: waBinary.Attrs{
			"from":            types.NewJID("120363000000000000", types.GroupServer),
			"participant":     types.NewJID("2348101849778", types.DefaultUserServer),
			"participant_lid": types.NewJID("270578695069938", types.HiddenUserServer),
			"t":               "1780925302",
		},
	}
	src, err := cli.parseMessageSource(node, false)
	if err != nil {
		t.Fatalf("parseMessageSource: %v", err)
	}
	if src.SenderAlt.Server != types.HiddenUserServer || src.SenderAlt.User != "270578695069938" {
		t.Fatalf("SenderAlt=%q, want 270578695069938@lid", src.SenderAlt.String())
	}
}

func TestHandleReceipt_StoresLIDPNMappingOnce(t *testing.T) {
	cli, rec := testClient(t)
	cli.handleReceipt(context.Background(), statusReceiptNode())
	pairs := rec.pairs()
	if len(pairs) != 1 {
		t.Fatalf("expected exactly 1 mapping write, got %d: %v", len(pairs), pairs)
	}
	if pairs[0][0].User != "270578695069938" || pairs[0][1].User != "2348101849778" {
		t.Fatalf("wrong pair stored: lid=%s pn=%s", pairs[0][0], pairs[0][1])
	}
}

// End-to-end shape check: a grouped receipt (no parent participant attr, per
// parseReceipt's source.IsGroup && source.Sender.IsEmpty()) dispatches one event
// per <user> child, none of them carrying a SenderAlt, and invents no mapping
// from a stray parent participant_pn. This asserts the OUTCOME; the clear itself
// is guarded by TestHandleGroupedReceipt_ClearsInheritedSenderAlt below.
func TestHandleGroupedReceipt_ChildrenDoNotInheritParentSenderAlt(t *testing.T) {
	cli, rec := testClient(t)
	var leaked []types.JID
	var children int
	cli.AddEventHandler(func(evt any) {
		if r, ok := evt.(*events.Receipt); ok {
			children++
			if !r.SenderAlt.IsEmpty() {
				leaked = append(leaked, r.SenderAlt)
			}
		}
	})
	node := &waBinary.Node{
		Tag: "receipt",
		Attrs: waBinary.Attrs{
			"from": types.StatusBroadcastJID,
			"id":   "3C089EAAEBF3B5C36529",
			"t":    "1780925302",
			// no "participant" — this is what makes it a grouped receipt
			"participant_pn": types.NewJID("2348101849778", types.DefaultUserServer),
		},
		Content: []waBinary.Node{{
			Tag:   "participants",
			Attrs: waBinary.Attrs{"message_id": "ABC123"},
			Content: []waBinary.Node{
				{Tag: "user", Attrs: waBinary.Attrs{"jid": types.NewJID("111111111111111", types.HiddenUserServer), "t": "1780925303", "type": "delivery"}},
				{Tag: "user", Attrs: waBinary.Attrs{"jid": types.NewJID("222222222222222", types.HiddenUserServer), "t": "1780925304", "type": "delivery"}},
			},
		}},
	}
	cli.handleReceipt(context.Background(), node)
	if children != 2 {
		t.Fatalf("expected 2 dispatched child receipts, got %d", children)
	}
	if len(leaked) != 0 {
		t.Fatalf("grouped children inherited a SenderAlt they must not have: %v", leaked)
	}
	// And no mapping is invented from a parent with no participant.
	if pairs := rec.pairs(); len(pairs) != 0 {
		t.Fatalf("expected no mapping writes, got %v", pairs)
	}
}

// Direct contract test for handleGroupedReceipt. The test above cannot reach the
// clear: parseMessageSource only fills SenderAlt when Sender is a LID, and the
// grouped path requires Sender to be empty, so the parent of a real grouped
// receipt never carries one today. That makes the clear defence-in-depth rather
// than a live fix — but it is one attribute change away from mattering, so it is
// guarded here at the function boundary, where a populated SenderAlt CAN be fed in.
func TestHandleGroupedReceipt_ClearsInheritedSenderAlt(t *testing.T) {
	cli, _ := testClient(t)
	var seen []types.JID
	cli.AddEventHandler(func(evt any) {
		if r, ok := evt.(*events.Receipt); ok {
			seen = append(seen, r.SenderAlt)
		}
	})
	partial := events.Receipt{MessageSource: types.MessageSource{
		Chat:      types.StatusBroadcastJID,
		IsGroup:   true,
		SenderAlt: types.NewJID("2348101849778", types.DefaultUserServer),
	}}
	participants := &waBinary.Node{
		Tag:   "participants",
		Attrs: waBinary.Attrs{"message_id": "ABC123"},
		Content: []waBinary.Node{
			{Tag: "user", Attrs: waBinary.Attrs{"jid": types.NewJID("111111111111111", types.HiddenUserServer), "t": "1780925303", "type": "delivery"}},
			{Tag: "user", Attrs: waBinary.Attrs{"jid": types.NewJID("222222222222222", types.HiddenUserServer), "t": "1780925304", "type": "delivery"}},
		},
	}
	cli.handleGroupedReceipt(partial, participants, "FALLBACK")
	if len(seen) != 2 {
		t.Fatalf("expected 2 child receipts, got %d", len(seen))
	}
	for i, alt := range seen {
		if !alt.IsEmpty() {
			t.Fatalf("child %d inherited the parent SenderAlt %s; it would be paired with a different LID", i, alt)
		}
	}
}

func TestDispatchAppState_PNForLIDChatStoresMapping(t *testing.T) {
	cli, rec := testClient(t)
	// Wire format observed in prod 2026-09-06: both sides are full JID strings.
	pn := "2349093784721@s.whatsapp.net"
	cli.dispatchAppState(context.Background(), appstate.WAPatchRegular, appstate.Mutation{
		Operation: waServerSync.SyncdMutation_SET,
		Index:     []string{appstate.IndexPNForLIDChat, "163213203263512@lid"},
		Action: &waSyncAction.SyncActionValue{
			PnForLidChatAction: &waSyncAction.PnForLidChatAction{PnJID: &pn},
		},
	}, false)
	pairs := rec.pairs()
	if len(pairs) != 1 {
		t.Fatalf("expected 1 mapping write, got %d: %v", len(pairs), pairs)
	}
	if pairs[0][0].User != "163213203263512" || pairs[0][1].User != "2349093784721" {
		t.Fatalf("wrong pair: lid=%s pn=%s", pairs[0][0], pairs[0][1])
	}
}
