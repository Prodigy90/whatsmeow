package whatsmeow

import (
	"context"
	"testing"
	"time"

	"go.mau.fi/libsignal/ecc"
	"go.mau.fi/libsignal/keys/identity"
	"go.mau.fi/libsignal/keys/prekey"
	"go.mau.fi/libsignal/session"
	"go.mau.fi/libsignal/util/optional"

	waBinary "go.mau.fi/whatsmeow/binary"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/util/keys"
	waLog "go.mau.fi/whatsmeow/util/log"
)

type slowFlushSessionStore struct {
	*memSessionStore
	delay time.Duration
}

func (s *slowFlushSessionStore) PutManySessions(ctx context.Context, m map[string][]byte) error {
	time.Sleep(s.delay)
	return s.memSessionStore.PutManySessions(ctx, m)
}

func (s *slowFlushSessionStore) PutSession(ctx context.Context, a string, b []byte) error {
	time.Sleep(s.delay)
	return s.memSessionStore.PutSession(ctx, a, b)
}

// TestEncryptForDevicesReportsSessionFlush checks that the session flush time
// reaches the caller: the worker's status-send timing log reads it from
// MessageDebugTimings.SessionFlush to tell store writes apart from crypto.
func TestEncryptForDevicesReportsSessionFlush(t *testing.T) {
	ctx := context.Background()
	const delay = 50 * time.Millisecond
	ownPN := types.NewJID("2340000000000", types.DefaultUserServer)
	lidDev := types.JID{User: "111111111111111", Server: types.HiddenUserServer}
	dev := &store.Device{
		ID:             &ownPN,
		LID:            types.NewJID("999999999999999", types.HiddenUserServer),
		IdentityKey:    keys.NewKeyPair(),
		RegistrationID: 4242,
		Log:            waLog.Noop,
		Sessions:       &slowFlushSessionStore{memSessionStore: &memSessionStore{m: map[string][]byte{}}, delay: delay},
		Identities:     &memIdentityStore{m: map[string][32]byte{}},
		LIDs:           &fixedLIDStore{pnToLID: map[types.JID]types.JID{}},
	}
	cli := &Client{Store: dev, Log: waLog.Noop}

	remoteIdentity := keys.NewKeyPair()
	signed := remoteIdentity.CreateSignedPreKey(1)
	oneTime := keys.NewPreKey(7)
	bundle := prekey.NewBundle(1234, 0,
		optional.NewOptionalUint32(oneTime.KeyID), signed.KeyID,
		ecc.NewDjbECPublicKey(*oneTime.Pub), ecc.NewDjbECPublicKey(*signed.Pub), *signed.Signature,
		identity.NewKey(ecc.NewDjbECPublicKey(*remoteIdentity.Pub)))
	if err := session.NewBuilderFromSignal(dev, lidDev.SignalAddress(), pbSerializer).ProcessBundle(ctx, bundle); err != nil {
		t.Fatalf("establish session: %v", err)
	}

	var flush time.Duration
	nodes, _, err := cli.encryptMessageForDevicesTimed(ctx, []types.JID{lidDev}, "MSGID", []byte("hello"), nil, waBinary.Attrs{}, &flush)
	if err != nil {
		t.Fatalf("encryptMessageForDevicesTimed: %v", err)
	}
	if len(nodes) != 1 {
		t.Fatalf("got %d <to> nodes, want 1", len(nodes))
	}
	if flush < delay {
		t.Fatalf("flush reported as %s, want at least the store's %s write delay", flush, delay)
	}
}
