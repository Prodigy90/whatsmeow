package whatsmeow

import (
	"context"
	"testing"

	"go.mau.fi/whatsmeow/appstate"
	"go.mau.fi/whatsmeow/proto/waServerSync"
	"go.mau.fi/whatsmeow/proto/waSyncAction"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
)

type nopContactStore struct{ store.ContactStore }

func (nopContactStore) PutAllContactNames(context.Context, []store.ContactEntry) error { return nil }
func (nopContactStore) PutContactName(context.Context, types.JID, string, string) error {
	return nil
}

func contactMutation(op waServerSync.SyncdMutation_SyncdOperation, index, name, lid string) appstate.Mutation {
	act := &waSyncAction.ContactAction{FullName: &name}
	if lid != "" {
		act.LidJID = &lid
	}
	return appstate.Mutation{
		Operation: op,
		Index:     []string{appstate.IndexContact, index},
		Action:    &waSyncAction.SyncActionValue{ContactAction: act},
	}
}

func contactSyncFixture() []appstate.Mutation {
	set := waServerSync.SyncdMutation_SET
	return []appstate.Mutation{
		contactMutation(set, "2348011111111@s.whatsapp.net", "Ada", "111111111111111@lid"), // full JID
		contactMutation(set, "2348022222222@s.whatsapp.net", "Bayo", "222222222222222"),    // bare user
		contactMutation(set, "2348033333333@s.whatsapp.net", "Chi", ""),                    // not on WhatsApp
		contactMutation(set, "444444444444444@lid", "LID-keyed", "444444444444444@lid"),    // LID index: WA Web skips
		contactMutation(set, "2348055555555@s.whatsapp.net", "Bad", "2348055555555@s.whatsapp.net"),
		contactMutation(waServerSync.SyncdMutation_REMOVE, "2348066666666@s.whatsapp.net", "Gone", "666666666666666@lid"),
	}
}

func assertContactSyncPairs(t *testing.T, rec *recordingLIDStore) {
	t.Helper()
	pairs := rec.pairs()
	want := map[string]string{"111111111111111": "2348011111111", "222222222222222": "2348022222222"}
	if len(pairs) != len(want) {
		t.Fatalf("stored %d mappings, want %d: %v", len(pairs), len(want), pairs)
	}
	for _, p := range pairs {
		if want[p[0].User] != p[1].User || p[0].Server != "lid" || p[1].Server != "s.whatsapp.net" {
			t.Fatalf("unexpected pair lid=%s pn=%s", p[0], p[1])
		}
	}
	if rec.batches != 1 {
		t.Fatalf("want one batched write, got %d", rec.batches)
	}
}

// A fresh link: critical_unblock_low arrives as a full sync. Our worker sets
// EmitAppStateEventsOnFullSync, upstream defaults to the mass-insert path;
// both must keep the LIDs.
func TestContactSyncLID_FullSyncBothPaths(t *testing.T) {
	for _, emit := range []bool{true, false} {
		cli, rec := testClient(t)
		cli.Store.Contacts = nopContactStore{}
		cli.EmitAppStateEventsOnFullSync = emit
		var evts []any
		if err := cli.collectEventsToDispatch(context.Background(), appstate.WAPatchCriticalUnblockLow, contactSyncFixture(), true, &evts); err != nil {
			t.Fatalf("emit=%t: %v", emit, err)
		}
		assertContactSyncPairs(t, rec)
	}
}

func TestContactSyncLID_IncrementalPatch(t *testing.T) {
	cli, rec := testClient(t)
	cli.Store.Contacts = nopContactStore{}
	if err := cli.collectEventsToDispatch(context.Background(), appstate.WAPatchCriticalUnblockLow, contactSyncFixture(), false, nil); err != nil {
		t.Fatal(err)
	}
	assertContactSyncPairs(t, rec)
}

func TestContactSyncLID_OtherCollectionsIgnored(t *testing.T) {
	cli, rec := testClient(t)
	cli.Store.Contacts = nopContactStore{}
	_ = cli.collectEventsToDispatch(context.Background(), appstate.WAPatchRegular, contactSyncFixture()[:1], false, nil)
	if len(rec.pairs()) != 0 {
		t.Fatalf("stored mappings from a non-contact collection: %v", rec.pairs())
	}
}

type recordingContactStore struct {
	store.ContactStore
	puts []string
}

func (r *recordingContactStore) PutAllContactNames(context.Context, []store.ContactEntry) error {
	return nil
}
func (r *recordingContactStore) PutContactName(_ context.Context, jid types.JID, _, full string) error {
	r.puts = append(r.puts, jid.User+"="+full)
	return nil
}

// Upstream #1309 blanks the stored name on a contact remove. Off by default, a
// remove changes nothing and dispatches nothing (pre-#1309); turned on, it blanks
// the name and dispatches events.Contact with a nil Action.
func TestContactRemove_GatedBySwitch(t *testing.T) {
	remove := contactMutation(waServerSync.SyncdMutation_REMOVE, "2348066666666@s.whatsapp.net", "Gone", "")
	for _, on := range []bool{false, true} {
		cli, _ := testClient(t)
		rec := &recordingContactStore{}
		cli.Store.Contacts = rec
		cli.SetHandleContactRemovals(on)
		evt := cli.dispatchAppState(context.Background(), appstate.WAPatchCriticalUnblockLow, remove, false)
		if !on {
			if evt != nil || len(rec.puts) != 0 {
				t.Fatalf("switch off: remove dispatched %v and wrote %v", evt, rec.puts)
			}
			continue
		}
		c, ok := evt.(*events.Contact)
		if !ok || c.Action != nil {
			t.Fatalf("switch on: want events.Contact with nil Action, got %#v", evt)
		}
		if len(rec.puts) != 1 || rec.puts[0] != "2348066666666=" {
			t.Fatalf("switch on: want the name blanked, got %v", rec.puts)
		}
	}
}
