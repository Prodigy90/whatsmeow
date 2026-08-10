// Copyright (c) 2026 Tulir Asokan
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package store

import (
	"context"
	"testing"

	"github.com/rs/zerolog"

	"go.mau.fi/libsignal/ecc"
	"go.mau.fi/libsignal/kdf"
	"go.mau.fi/libsignal/keys/chain"
	"go.mau.fi/libsignal/keys/identity"
	"go.mau.fi/libsignal/keys/root"
	"go.mau.fi/libsignal/protocol"
	"go.mau.fi/libsignal/state/record"
	"go.mau.fi/whatsmeow/types"
	waLog "go.mau.fi/whatsmeow/util/log"
)

// iterSessionStore is a fake SessionStore that serves session blobs through the
// streaming sessionIterator/sessionLoader interfaces. GetSession/GetManySessions
// panic so the tests prove the streaming paths are actually taken.
type iterSessionStore struct {
	sessions map[string][]byte // nil value = row exists with NULL blob
}

var _ SessionStore = (*iterSessionStore)(nil)
var _ sessionIterator = (*iterSessionStore)(nil)
var _ sessionLoader = (*iterSessionStore)(nil)

func (f *iterSessionStore) IterateSessions(ctx context.Context, addresses []string, callback func(string, []byte) error) error {
	for _, addr := range addresses {
		blob, ok := f.sessions[addr]
		if !ok {
			continue
		}
		if err := callback(addr, blob); err != nil {
			return err
		}
	}
	return nil
}

func (f *iterSessionStore) IterateSession(ctx context.Context, address string, callback func([]byte) error) (bool, error) {
	blob, ok := f.sessions[address]
	if !ok {
		return false, nil
	}
	return true, callback(blob)
}

func (f *iterSessionStore) GetSession(context.Context, string) ([]byte, error) {
	panic("GetSession called on streaming store")
}

func (f *iterSessionStore) GetManySessions(context.Context, []string) (map[string][]byte, error) {
	panic("GetManySessions called on streaming store")
}

func (f *iterSessionStore) HasSession(ctx context.Context, address string) (bool, error) {
	_, ok := f.sessions[address]
	return ok, nil
}

func (f *iterSessionStore) ContainsManySessions(ctx context.Context, addresses []string) (map[string]bool, error) {
	result := make(map[string]bool)
	for _, addr := range addresses {
		if blob, ok := f.sessions[addr]; ok && blob != nil {
			result[addr] = true
		}
	}
	return result, nil
}

func (f *iterSessionStore) PutSession(ctx context.Context, address string, session []byte) error {
	f.sessions[address] = session
	return nil
}

func (f *iterSessionStore) PutManySessions(ctx context.Context, sessions map[string][]byte) error {
	for addr, sess := range sessions {
		f.sessions[addr] = sess
	}
	return nil
}

func (f *iterSessionStore) DeleteAllSessions(context.Context, string) error { return nil }
func (f *iterSessionStore) DeleteSession(context.Context, string) error     { return nil }
func (f *iterSessionStore) MigratePNToLID(context.Context, types.JID, types.JID) error {
	return nil
}
func (f *iterSessionStore) MigrateManyPNsToLIDs(context.Context, map[types.JID]types.JID) error {
	return nil
}

// serializedTestSession fabricates a minimal valid session record: a session with
// identity keys, a sender chain, and a root key — just enough for Serialize and
// NewSessionFromBytes to round-trip.
func serializedTestSession(t *testing.T) []byte {
	t.Helper()
	keyPair, err := ecc.GenerateKeyPair()
	if err != nil {
		t.Fatalf("GenerateKeyPair: %v", err)
	}
	idKey := identity.NewKey(keyPair.PublicKey())
	sess := record.NewSession(SignalProtobufSerializer.Session, SignalProtobufSerializer.State)
	state := sess.SessionState()
	state.SetLocalIdentityKey(idKey)
	state.SetRemoteIdentityKey(idKey)
	state.SetSenderBaseKey(keyPair.PublicKey().Serialize())
	state.SetRootKey(root.NewKey(kdf.DeriveSecrets, make([]byte, 32)))
	state.SetSenderChain(keyPair, chain.NewKey(kdf.DeriveSecrets, make([]byte, 32), 0))
	return sess.Serialize()
}

func TestWithCachedSessionsStreamingPath(t *testing.T) {
	valid := serializedTestSession(t)
	d := &Device{
		Log: waLog.Noop,
		Sessions: &iterSessionStore{sessions: map[string][]byte{
			"warm:1":    valid,
			"null:1":    nil,
			"corrupt:1": []byte("\xff\xff not a session"),
		}},
	}

	ctx := zerolog.Nop().WithContext(context.Background())
	addresses := []string{"warm:1", "null:1", "corrupt:1", "missing:1"}
	existing, cctx, err := d.WithCachedSessions(ctx, addresses)
	if err != nil {
		t.Fatalf("WithCachedSessions: %v", err)
	}

	if !existing["warm:1"] {
		t.Errorf("warm:1 should be reported as existing")
	}
	if existing["null:1"] {
		t.Errorf("null:1 (NULL blob) should be cold")
	}
	if existing["missing:1"] {
		t.Errorf("missing:1 should be cold")
	}
	if _, reported := existing["corrupt:1"]; reported {
		t.Errorf("corrupt:1 should be absent from the existing map (parity with map path)")
	}

	// Cache contents: warm has a record, cold addresses have fresh records,
	// corrupt is absent entirely (so it is not silently reset).
	if sess := getCachedSession(cctx, "warm:1"); sess == nil {
		t.Errorf("warm:1 should be cached")
	}
	if sess := getCachedSession(cctx, "missing:1"); sess == nil {
		t.Errorf("missing:1 should have a fresh cached record")
	}
	cache := getSessionCache(cctx)
	if _, ok := cache.Get("corrupt:1"); ok {
		t.Errorf("corrupt:1 must not be in the cache")
	}
}

func TestLoadSessionStreamingPath(t *testing.T) {
	valid := serializedTestSession(t)
	d := &Device{
		Log: waLog.Noop,
		Sessions: &iterSessionStore{sessions: map[string][]byte{
			"15550000001:1": valid,
			"15550000002:1": nil,
		}},
	}
	ctx := context.Background()

	sess, err := d.LoadSession(ctx, protocol.NewSignalAddress("15550000001", 1))
	if err != nil || sess == nil {
		t.Fatalf("LoadSession(existing): sess=%v err=%v", sess, err)
	}

	// NULL blob row and missing row both yield a fresh session, not an error.
	sess, err = d.LoadSession(ctx, protocol.NewSignalAddress("15550000002", 1))
	if err != nil || sess == nil {
		t.Fatalf("LoadSession(null blob): sess=%v err=%v", sess, err)
	}
	sess, err = d.LoadSession(ctx, protocol.NewSignalAddress("15550000099", 1))
	if err != nil || sess == nil {
		t.Fatalf("LoadSession(missing): sess=%v err=%v", sess, err)
	}
}

func TestLoadSessionStreamingCorruptBlobErrors(t *testing.T) {
	d := &Device{
		Log: waLog.Noop,
		Sessions: &iterSessionStore{sessions: map[string][]byte{
			"15550000001:1": []byte("\xff\xff not a session"),
		}},
	}
	_, err := d.LoadSession(context.Background(), protocol.NewSignalAddress("15550000001", 1))
	if err == nil {
		t.Fatalf("LoadSession on corrupt blob should error")
	}
}
