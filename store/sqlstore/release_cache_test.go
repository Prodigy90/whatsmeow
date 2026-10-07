package sqlstore

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"testing"

	"go.mau.fi/whatsmeow/types"
)

// After a release, a lookup must go back to the database. The trap is
// cacheFilled: left set, a miss is answered as "no LID" without a query, which
// would silently break LID addressing for everyone not still in memory.
func TestCachedLIDMapReleaseCacheFallsThroughToDB(t *testing.T) {
	state := &recordingDB{cols: []string{"lid"}, rows: [][]driver.Value{{"111222333"}}}
	db := sql.OpenDB(&recordingConnector{state: state})
	t.Cleanup(func() { _ = db.Close() })
	m := NewCachedLIDMap(NewWithDB(db, "postgres", nil).db)

	m.pnToLIDCache["2348011111111"] = "999"
	m.lidToPNCache["999"] = "2348011111111"
	m.cacheFilled = true

	if n := m.ReleaseCache(); n != 1 {
		t.Fatalf("ReleaseCache() = %d, want 1", n)
	}
	if len(m.pnToLIDCache) != 0 || len(m.lidToPNCache) != 0 {
		t.Fatal("LID maps not emptied")
	}

	pn := types.JID{User: "2348022222222", Server: types.DefaultUserServer}
	lid, err := m.GetLIDForPN(context.Background(), pn)
	if err != nil {
		t.Fatalf("GetLIDForPN: %v", err)
	}
	if len(state.execs) != 1 {
		t.Fatalf("GetLIDForPN ran %d queries after release, want 1 (answered from an empty cache?)", len(state.execs))
	}
	if lid.User != "111222333" {
		t.Fatalf("GetLIDForPN = %q, want the database row", lid.User)
	}
}

func TestSQLStoreReleaseCacheRefillsFromDB(t *testing.T) {
	state := &recordingDB{
		cols: []string{"first_name", "full_name", "push_name", "business_name", "redacted_phone"},
		rows: [][]driver.Value{{"Ada", "Ada Obi", "ada", "", ""}},
	}
	s := newRecordingStore(t, "postgres", state)
	user := types.JID{User: "2348011111111", Server: types.DefaultUserServer}
	s.contactCache[user] = &types.ContactInfo{Found: true, FullName: "stale"}

	if n := s.ReleaseCache(); n != 1 {
		t.Fatalf("ReleaseCache() = %d, want 1", n)
	}
	info, err := s.GetContact(context.Background(), user)
	if err != nil {
		t.Fatalf("GetContact: %v", err)
	}
	if len(state.execs) != 1 || info.FullName != "Ada Obi" {
		t.Fatalf("GetContact = %q after %d queries, want the database row after 1", info.FullName, len(state.execs))
	}
}

// GetAllContacts must not leave the whole address book in contactCache.
func TestGetAllContactsDoesNotFillCache(t *testing.T) {
	state := &recordingDB{
		cols: []string{"their_jid", "first_name", "full_name", "push_name", "business_name", "redacted_phone"},
		rows: [][]driver.Value{
			{"2348011111111@s.whatsapp.net", "Ada", "Ada Obi", "ada", "", ""},
			{"2348022222222@s.whatsapp.net", "Bayo", "Bayo Ade", "bayo", "", ""},
		},
	}
	s := newRecordingStore(t, "postgres", state)
	all, err := s.GetAllContacts(context.Background())
	if err != nil {
		t.Fatalf("GetAllContacts: %v", err)
	}
	if len(all) != 2 {
		t.Fatalf("GetAllContacts returned %d contacts, want 2", len(all))
	}
	if len(s.contactCache) != 0 {
		t.Fatalf("contactCache holds %d entries after GetAllContacts, want 0", len(s.contactCache))
	}
}
