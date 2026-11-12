package whatsmeow

import (
	"context"
	"testing"

	"google.golang.org/protobuf/proto"

	"go.mau.fi/whatsmeow/proto/waHistorySync"
	"go.mau.fi/whatsmeow/types"
)

func hsMapping(pn, lid string) *waHistorySync.PhoneNumberToLIDMapping {
	return &waHistorySync.PhoneNumberToLIDMapping{PnJID: proto.String(pn), LidJID: proto.String(lid)}
}

func hsConv(id, pn, lid string) *waHistorySync.Conversation {
	c := &waHistorySync.Conversation{ID: proto.String(id)}
	if pn != "" {
		c.PnJID = proto.String(pn)
	}
	if lid != "" {
		c.LidJID = proto.String(lid)
	}
	return c
}

func historyFixture() *waHistorySync.HistorySync {
	return &waHistorySync.HistorySync{
		SyncType: waHistorySync.HistorySync_INITIAL_BOOTSTRAP.Enum(),
		PhoneNumberToLidMappings: []*waHistorySync.PhoneNumberToLIDMapping{
			hsMapping("2348011111111@s.whatsapp.net", "111111111111111@lid"),          // full JIDs
			hsMapping("2348022222222", "222222222222222"),                             // bare users
			hsMapping("2348033333333@c.us", "333333333333333:7@lid"),                  // legacy PN, LID with device
			hsMapping("", "444444444444444@lid"),                                      // no PN
			hsMapping("2348055555555@s.whatsapp.net", "2348055555555@s.whatsapp.net"), // PN given as LID
		},
		Conversations: []*waHistorySync.Conversation{
			hsConv("666666666666666@lid", "2348066666666@s.whatsapp.net", ""),                        // LID chat → its pnJid
			hsConv("2348077777777@s.whatsapp.net", "", "777777777777777@lid"),                        // PN chat → its lidJid
			hsConv("888888888888888@lid", "", ""),                                                    // LID chat, no PN
			hsConv("120363000000000000@g.us", "2348099999999@s.whatsapp.net", "999999999999998@lid"), // group: ignored
		},
	}
}

func TestHistoryLIDMappings_ReadsEveryChannelWAWebReads(t *testing.T) {
	pairs, top, fromConvs, invalid := historyLIDMappings(historyFixture())
	if top != 3 || fromConvs != 2 || invalid != 2 {
		t.Fatalf("top=%d fromConvs=%d invalid=%d, want 3/2/2", top, fromConvs, invalid)
	}
	want := map[string]string{
		"111111111111111": "2348011111111",
		"222222222222222": "2348022222222", // bare form: the old ParseJID path stored {Server:"222…"} and dropped it
		"333333333333333": "2348033333333",
		"666666666666666": "2348066666666",
		"777777777777777": "2348077777777",
	}
	if len(pairs) != len(want) {
		t.Fatalf("got %d pairs, want %d: %v", len(pairs), len(want), pairs)
	}
	for _, p := range pairs {
		if p.LID.Server != types.HiddenUserServer || p.PN.Server != types.DefaultUserServer {
			t.Fatalf("pair with wrong servers (PutManyLIDMappings would discard it): %v", p)
		}
		if p.LID.Device != 0 || p.PN.Device != 0 {
			t.Fatalf("pair keeps a device suffix: %v", p)
		}
		if want[p.LID.User] != p.PN.User {
			t.Fatalf("unexpected pair %s → %s", p.LID.User, p.PN.User)
		}
	}
}

func TestStoreHistorySyncLIDMappings_OneBatch(t *testing.T) {
	cli, rec := testClient(t)
	cli.storeHistorySyncLIDMappings(context.Background(), historyFixture())
	if rec.batches != 1 || len(rec.pairs()) != 5 {
		t.Fatalf("batches=%d pairs=%d, want one batch of 5", rec.batches, len(rec.pairs()))
	}

	// The exported internals path (top-level list only) goes through the same parser.
	cli2, rec2 := testClient(t)
	cli2.storeHistoricalPNLIDMappings(context.Background(), []*waHistorySync.PhoneNumberToLIDMapping{
		hsMapping("2348022222222", "222222222222222"),
	})
	got := rec2.pairs()
	if len(got) != 1 || got[0][0].Server != types.HiddenUserServer {
		t.Fatalf("bare mapping via storeHistoricalPNLIDMappings: %v", got)
	}
}

func TestStoreHistorySyncLIDMappings_EmptyChunkWritesNothing(t *testing.T) {
	cli, rec := testClient(t)
	cli.storeHistorySyncLIDMappings(context.Background(), &waHistorySync.HistorySync{
		Conversations: []*waHistorySync.Conversation{hsConv("888888888888888@lid", "", "")},
	})
	if rec.batches != 0 {
		t.Fatalf("an empty chunk must not open a write, got %d batches", rec.batches)
	}
}
