package sqlstore

import (
	"context"
	"testing"

	"go.mau.fi/whatsmeow/store"
)

// DoAppStateTxn must open ONE transaction before the version write, and the MAC
// writes (which open their own DoTxn) must join it rather than start another.
func TestDoAppStateTxnWrapsVersionAndMACWrites(t *testing.T) {
	withTestArrayWrapper(t)
	state := &recordingDB{}
	s := newRecordingStore(t, "postgres", state)
	ctx := context.Background()

	err := s.DoAppStateTxn(ctx, func(ctx context.Context) error {
		if err := s.PutAppStateVersion(ctx, "critical_unblock_low", 6, [128]byte{}); err != nil {
			return err
		}
		if err := s.DeleteAppStateMutationMACs(ctx, "critical_unblock_low", [][]byte{{1}}); err != nil {
			return err
		}
		return s.PutAppStateMutationMACs(ctx, "critical_unblock_low", 6, []store.AppStateMutationMAC{{IndexMAC: []byte{2}, ValueMAC: []byte{3}}})
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(state.beginAt) != 1 || state.beginAt[0] != 0 {
		t.Fatalf("transactions began at exec positions %v, want exactly one, before the first write", state.beginAt)
	}
	if len(state.execs) < 3 {
		t.Fatalf("got %d statements, want the version, delete and insert writes", len(state.execs))
	}
}
