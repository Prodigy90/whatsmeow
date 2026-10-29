package whatsmeow

import (
	"context"
	"errors"
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

// slowFlushSessionStore delays (and optionally fails) the batch write the
// flush uses; single PutSession calls from test setup go straight through.
type slowFlushSessionStore struct {
	*memSessionStore
	delay time.Duration
	err   error
}

func (s *slowFlushSessionStore) PutManySessions(ctx context.Context, m map[string][]byte) error {
	time.Sleep(s.delay)
	if s.err != nil {
		return s.err
	}
	return s.memSessionStore.PutManySessions(ctx, m)
}

// TestEncryptForDevicesReportsSessionFlush checks that the session flush time
// reaches the caller: the worker's status-send timing log reads it from
// MessageDebugTimings.SessionFlush to tell store writes apart from crypto.
func TestEncryptForDevicesReportsSessionFlush(t *testing.T) {
	t.Run("saved", func(t *testing.T) { testReportsSessionFlush(t, nil) })
	// A failed or timed-out flush is the case the timing exists to show.
	t.Run("failed", func(t *testing.T) { testReportsSessionFlush(t, errors.New("store down")) })
}

func testReportsSessionFlush(t *testing.T, storeErr error) {
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
		Sessions:       &slowFlushSessionStore{memSessionStore: &memSessionStore{m: map[string][]byte{}}, delay: delay, err: storeErr},
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
	nodes, _, err := cli.encryptMessageForDevicesTimed(ctx, []types.JID{lidDev}, "MSGID", []byte("hello"), nil, waBinary.Attrs{}, &flush, nil)
	if storeErr != nil {
		if !errors.Is(err, storeErr) {
			t.Fatalf("got err %v, want the store's %v", err, storeErr)
		}
	} else {
		if err != nil {
			t.Fatalf("encryptMessageForDevicesTimed: %v", err)
		}
		if len(nodes) != 1 {
			t.Fatalf("got %d <to> nodes, want 1", len(nodes))
		}
	}
	if flush < delay {
		t.Fatalf("flush reported as %s, want at least the store's %s write delay", flush, delay)
	}
}
