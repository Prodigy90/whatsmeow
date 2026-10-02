package whatsmeow

import (
	"context"
	"runtime"
	"sync"
	"testing"

	"go.mau.fi/libsignal/ecc"
	"go.mau.fi/libsignal/keys/identity"
	"go.mau.fi/libsignal/keys/prekey"
	"go.mau.fi/libsignal/protocol"
	"go.mau.fi/libsignal/session"
	"go.mau.fi/libsignal/util/optional"

	waBinary "go.mau.fi/whatsmeow/binary"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/util/keys"
	waLog "go.mau.fi/whatsmeow/util/log"
)

type memSessionStore struct {
	store.SessionStore
	mu sync.Mutex
	m  map[string][]byte
}

func (s *memSessionStore) GetSession(_ context.Context, a string) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.m[a], nil
}
func (s *memSessionStore) HasSession(_ context.Context, a string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.m[a]
	return ok, nil
}
func (s *memSessionStore) GetManySessions(_ context.Context, as []string) (map[string][]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string][]byte, len(as))
	for _, a := range as {
		out[a] = s.m[a]
	}
	return out, nil
}
func (s *memSessionStore) PutSession(_ context.Context, a string, b []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.m[a] = b
	return nil
}
func (s *memSessionStore) PutManySessions(_ context.Context, m map[string][]byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for a, b := range m {
		s.m[a] = b
	}
	return nil
}
func (s *memSessionStore) MigrateManyPNsToLIDs(context.Context, map[types.JID]types.JID) error {
	return nil
}

type memIdentityStore struct {
	store.IdentityStore
	mu sync.Mutex
	m  map[string][32]byte
}

func (s *memIdentityStore) PutIdentity(_ context.Context, a string, k [32]byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.m[a] = k
	return nil
}
func (s *memIdentityStore) PutManyIdentities(_ context.Context, m map[string][32]byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for a, k := range m {
		s.m[a] = k
	}
	return nil
}
func (s *memIdentityStore) GetManyIdentities(_ context.Context, as []string) (map[string]*[32]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]*[32]byte, len(as))
	for _, a := range as {
		if k, ok := s.m[a]; ok {
			out[a] = &k
		}
	}
	return out, nil
}
func (s *memIdentityStore) IsTrustedIdentity(context.Context, string, [32]byte) (bool, error) {
	return true, nil
}

type fixedLIDStore struct {
	store.LIDStore
	pnToLID map[types.JID]types.JID
}

func (s *fixedLIDStore) GetManyLIDsForPNs(_ context.Context, pns []types.JID) (map[types.JID]types.JID, error) {
	out := make(map[types.JID]types.JID, len(pns))
	for _, pn := range pns {
		if lid, ok := s.pnToLID[pn]; ok {
			out[pn] = lid
		}
	}
	return out, nil
}

// TestEncryptForDevicesSerializesSharedSession covers .scratch/whatsmeow-sync-2026-10
// issue 01: one physical device listed in both PN and LID form resolves to a
// single LID Signal session. The parallel fan-out used to give each JID its own
// goroutine, so two goroutines advanced the same session record at once (the
// 2026-10-02 -race reports on a self-send). Encrypted one after the other, the
// two messages must carry consecutive chain counters; concurrently they can
// read the same chain key and repeat a counter, which the recipient rejects.
func TestEncryptForDevicesSerializesSharedSession(t *testing.T) {
	// The old per-JID fan-out was capped at GOMAXPROCS workers; on a 1-CPU
	// runner it ran sequentially and this test passed against the bug.
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(4))
	ctx := context.Background()
	ownPN := types.NewJID("2340000000000", types.DefaultUserServer)
	pnDev := types.JID{User: "2348000000001", Server: types.DefaultUserServer}
	lidDev := types.JID{User: "111111111111111", Server: types.HiddenUserServer}

	for run := 0; run < 25; run++ {
		dev := &store.Device{
			ID:             &ownPN,
			LID:            types.NewJID("999999999999999", types.HiddenUserServer),
			IdentityKey:    keys.NewKeyPair(),
			RegistrationID: 4242,
			Log:            waLog.Noop,
			Sessions:       &memSessionStore{m: map[string][]byte{}},
			Identities:     &memIdentityStore{m: map[string][32]byte{}},
			LIDs:           &fixedLIDStore{pnToLID: map[types.JID]types.JID{pnDev: lidDev}},
		}
		cli := &Client{Store: dev, Log: waLog.Noop}

		remoteIdentity := keys.NewKeyPair()
		signed := remoteIdentity.CreateSignedPreKey(1)
		oneTime := keys.NewPreKey(7)
		bundle := prekey.NewBundle(1234, 0,
			optional.NewOptionalUint32(oneTime.KeyID), signed.KeyID,
			ecc.NewDjbECPublicKey(*oneTime.Pub), ecc.NewDjbECPublicKey(*signed.Pub), *signed.Signature,
			identity.NewKey(ecc.NewDjbECPublicKey(*remoteIdentity.Pub)))
		builder := session.NewBuilderFromSignal(dev, lidDev.SignalAddress(), pbSerializer)
		if err := builder.ProcessBundle(ctx, bundle); err != nil {
			t.Fatalf("establish session: %v", err)
		}

		nodes, _, err := cli.encryptMessageForDevices(ctx, []types.JID{pnDev, lidDev}, "MSGID", []byte("hello"), nil, waBinary.Attrs{})
		if err != nil {
			t.Fatalf("encryptMessageForDevices: %v", err)
		}
		if len(nodes) != 2 {
			t.Fatalf("run %d: got %d <to> nodes, want 2 (one per wire JID, as upstream sends)", run, len(nodes))
		}
		seen := map[uint32]bool{}
		for _, n := range nodes {
			enc := n.GetChildren()[0]
			msg, err := protocol.NewPreKeySignalMessageFromBytes(enc.Content.([]byte), pbSerializer.PreKeySignalMessage, pbSerializer.SignalMessage)
			if err != nil {
				t.Fatalf("run %d: parse pkmsg for %v: %v", run, n.Attrs["jid"], err)
			}
			c := msg.WhisperMessage().Counter()
			if seen[c] {
				t.Fatalf("run %d: two messages share chain counter %d — the shared session was advanced concurrently", run, c)
			}
			seen[c] = true
		}
	}
}

func newEncryptTestDevice(pnDev, lidDev types.JID) (*Client, *store.Device) {
	ownPN := types.NewJID("2340000000000", types.DefaultUserServer)
	dev := &store.Device{
		ID:             &ownPN,
		LID:            types.NewJID("999999999999999", types.HiddenUserServer),
		IdentityKey:    keys.NewKeyPair(),
		RegistrationID: 4242,
		Log:            waLog.Noop,
		Sessions:       &memSessionStore{m: map[string][]byte{}},
		Identities:     &memIdentityStore{m: map[string][32]byte{}},
		LIDs:           &fixedLIDStore{pnToLID: map[types.JID]types.JID{pnDev: lidDev}},
	}
	return &Client{Store: dev, Log: waLog.Noop}, dev
}

// remoteBundle is a prekey bundle for one fake remote device, using one-time
// prekey preKeyID so a test can tell which bundle a session was built from.
func remoteBundle(remoteIdentity *keys.KeyPair, preKeyID uint32) *prekey.Bundle {
	signed := remoteIdentity.CreateSignedPreKey(1)
	oneTime := keys.NewPreKey(preKeyID)
	return prekey.NewBundle(1234, 0,
		optional.NewOptionalUint32(oneTime.KeyID), signed.KeyID,
		ecc.NewDjbECPublicKey(*oneTime.Pub), ecc.NewDjbECPublicKey(*signed.Pub), *signed.Signature,
		identity.NewKey(ecc.NewDjbECPublicKey(*remoteIdentity.Pub)))
}

func pkmsgOf(t *testing.T, r encResult) *protocol.PreKeySignalMessage {
	t.Helper()
	if r.err != nil {
		t.Fatalf("encrypt failed: %v", r.err)
	}
	enc := r.node.GetChildren()[0]
	msg, err := protocol.NewPreKeySignalMessageFromBytes(enc.Content.([]byte), pbSerializer.PreKeySignalMessage, pbSerializer.SignalMessage)
	if err != nil {
		t.Fatalf("parse pkmsg: %v", err)
	}
	return msg
}

func TestGroupDevicesBySession(t *testing.T) {
	pnA := types.JID{User: "2348000000001", Server: types.DefaultUserServer}
	lidA := types.JID{User: "111111111111111", Server: types.HiddenUserServer}
	pnB := types.JID{User: "2348000000002", Server: types.DefaultUserServer}
	own := types.JID{User: "2340000000000", Server: types.DefaultUserServer, Device: 1}

	all := []types.JID{pnA, pnB, own, lidA}
	plaintexts := [][]byte{{1}, {1}, nil, {1}} // own device has nothing to encrypt
	addrs := []string{lidA.SignalAddress().String(), pnB.SignalAddress().String(), own.SignalAddress().String(), lidA.SignalAddress().String()}
	// addrToJID keeps the LAST JID per address: lidA for A's session. That JID
	// holds the prekey bundle, so it must be encrypted first in its group.
	addrToJID := map[string]types.JID{addrs[0]: lidA, addrs[1]: pnB, addrs[2]: own}

	groups, skipped := groupDevicesBySession(plaintexts, addrs, all, addrToJID)
	if len(skipped) != 1 || skipped[0] != 2 {
		t.Fatalf("skipped = %v, want [2]", skipped)
	}
	want := [][]int{{3, 0}, {1}}
	if len(groups) != len(want) {
		t.Fatalf("groups = %v, want %v", groups, want)
	}
	for g := range want {
		if len(groups[g]) != len(want[g]) || groups[g][0] != want[g][0] || groups[g][len(want[g])-1] != want[g][len(want[g])-1] {
			t.Fatalf("groups = %v, want %v (bundle holder first)", groups, want)
		}
	}
}

// TestEncryptSessionGroupBuildsMissingSessionOnce: no session yet, the bundle is
// keyed under the LID JID (the holder). The holder must build the session and
// the PN sibling must reuse it, so both get a node with consecutive counters.
func TestEncryptSessionGroupBuildsMissingSessionOnce(t *testing.T) {
	ctx := context.Background()
	pnDev := types.JID{User: "2348000000001", Server: types.DefaultUserServer}
	lidDev := types.JID{User: "111111111111111", Server: types.HiddenUserServer}
	cli, _ := newEncryptTestDevice(pnDev, lidDev)

	all := []types.JID{pnDev, lidDev}
	addr := lidDev.SignalAddress().String()
	addrs := []string{addr, addr}
	plaintexts := [][]byte{[]byte("hi"), []byte("hi")}
	enc := map[types.JID]types.JID{pnDev: lidDev, lidDev: lidDev}
	bundles := map[types.JID]*prekey.Bundle{lidDev: remoteBundle(keys.NewKeyPair(), 7)}
	groups, _ := groupDevicesBySession(plaintexts, addrs, all, map[string]types.JID{addr: lidDev})
	if len(groups) != 1 {
		t.Fatalf("groups = %v, want one session group", groups)
	}

	results := make([]encResult, len(all))
	cli.encryptSessionGroup(ctx, groups[0], all, plaintexts, addrs, enc, bundles, map[string]bool{addr: false}, waBinary.Attrs{}, results)

	a, b := pkmsgOf(t, results[0]), pkmsgOf(t, results[1])
	if a.WhisperMessage().Counter() == b.WhisperMessage().Counter() {
		t.Fatalf("both messages carry counter %d", a.WhisperMessage().Counter())
	}
	if a.BaseKey().Serialize() == nil || string(a.BaseKey().Serialize()) != string(b.BaseKey().Serialize()) {
		t.Fatal("the two messages were built on different sessions: the sibling re-processed the bundle")
	}
}

// TestEncryptSessionGroupKeepsSessionThatAppearedDuringFetch covers ticket 03:
// a session was established (from one-time prekey 7) while the locks were
// released for the prekey fetch, and the fetched bundle (prekey 9) is stale. The
// re-read says the session exists, so the bundle must not be processed over it.
func TestEncryptSessionGroupKeepsSessionThatAppearedDuringFetch(t *testing.T) {
	ctx := context.Background()
	pnDev := types.JID{User: "2348000000001", Server: types.DefaultUserServer}
	lidDev := types.JID{User: "111111111111111", Server: types.HiddenUserServer}
	cli, dev := newEncryptTestDevice(pnDev, lidDev)

	remote := keys.NewKeyPair()
	if err := session.NewBuilderFromSignal(dev, lidDev.SignalAddress(), pbSerializer).ProcessBundle(ctx, remoteBundle(remote, 7)); err != nil {
		t.Fatalf("establish session: %v", err)
	}

	all := []types.JID{lidDev}
	addr := lidDev.SignalAddress().String()
	results := make([]encResult, 1)
	cli.encryptSessionGroup(ctx, []int{0}, all, [][]byte{[]byte("hi")}, []string{addr},
		map[types.JID]types.JID{lidDev: lidDev},
		map[types.JID]*prekey.Bundle{lidDev: remoteBundle(remote, 9)},
		map[string]bool{addr: true}, waBinary.Attrs{}, results)

	if id := pkmsgOf(t, results[0]).PreKeyID(); id == nil || id.Value != 7 {
		t.Fatalf("message built on prekey %v, want 7: the stale bundle overwrote the existing session", id)
	}
}
