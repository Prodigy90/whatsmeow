package appstate

import (
	"context"
	"errors"
	"maps"
	"slices"
	"testing"

	"go.mau.fi/whatsmeow/store"
	waLog "go.mau.fi/whatsmeow/util/log"
)

// txnMemAppState adds rollback-on-error transactions to memAppState and can be told
// to fail the mutation MAC insert, the last of the three writes storeMACs makes.
type txnMemAppState struct {
	*memAppState
	failMACInsert bool
}

var errMACInsert = errors.New("mac insert failed")

func (m *txnMemAppState) PutAppStateMutationMACs(ctx context.Context, name string, version uint64, mutations []store.AppStateMutationMAC) error {
	if m.failMACInsert {
		return errMACInsert
	}
	return m.memAppState.PutAppStateMutationMACs(ctx, name, version, mutations)
}

func (m *txnMemAppState) DoAppStateTxn(ctx context.Context, fn func(ctx context.Context) error) error {
	version, hash := maps.Clone(m.version), maps.Clone(m.hash)
	macs := make(map[string][]memMAC, len(m.macs))
	for k, v := range m.macs {
		macs[k] = slices.Clone(v)
	}
	if err := fn(ctx); err != nil {
		m.version, m.hash, m.macs = version, hash, macs
		return err
	}
	return nil
}

// A failed MAC insert must not leave the new version and hash behind: a hash that
// is ahead of its MACs makes every later patch for the collection fail to verify.
func TestStoreMACsIsAtomic(t *testing.T) {
	ctx := context.Background()
	mem := &txnMemAppState{memAppState: newMemAppState()}
	proc := NewProcessor(&store.Device{AppStateKeys: mem, AppState: mem}, waLog.Noop)

	var oldHash [128]byte
	oldHash[0] = 1
	if err := mem.PutAppStateVersion(ctx, string(WAPatchCriticalUnblockLow), 5, oldHash); err != nil {
		t.Fatal(err)
	}

	mem.failMACInsert = true
	var newHash [128]byte
	newHash[0] = 2
	out := &patchOutput{AddedMACs: []store.AppStateMutationMAC{{IndexMAC: []byte{1}, ValueMAC: []byte{2}}}}
	err := proc.storeMACs(ctx, WAPatchCriticalUnblockLow, HashState{Version: 6, Hash: newHash}, out)
	if !errors.Is(err, errMACInsert) {
		t.Fatalf("storeMACs error = %v, want the MAC insert failure", err)
	}

	version, hash, _ := mem.GetAppStateVersion(ctx, string(WAPatchCriticalUnblockLow))
	if version != 5 || hash != oldHash {
		t.Fatalf("after a failed MAC insert the stored state is v%d (hash[0]=%d), want v5 (hash[0]=1)", version, hash[0])
	}
}
