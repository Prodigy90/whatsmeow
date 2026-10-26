package appstate

import (
	"bytes"
	"context"
	"testing"

	"google.golang.org/protobuf/proto"

	"go.mau.fi/whatsmeow/proto/waServerSync"
)

// A row stored before index hashes were kept, under an earlier key, has neither a
// hash nor this key's index MAC. The primary still subtracts it when the index is
// SET under a new key, so we must find it by recomputing its index MAC under every
// stored key. This is the state every pre-upgrade row is in.
func TestDecodePatchSupersedesLegacyRowUnderRotatedKey(t *testing.T) {
	proc, mem := newTestProcessor(t)
	keyA, keyB := addTestKey(t, mem, 1), addTestKey(t, mem, 2)
	oldL := encryptSet(t, keyA, indexL, markRead(1))
	m := encryptSet(t, keyA, indexM, archive(true))
	before := phoneHash(valueMACOf(oldL), valueMACOf(m))
	seedState(t, mem, 79, before, false, oldL, m)

	newL := encryptSet(t, keyB, indexL, markRead(2))
	after := phoneHash(valueMACOf(m), valueMACOf(newL))
	patch := patchOf(80, after, keyB, newL)

	_, state, err := proc.DecodePatches(context.Background(), &PatchList{Name: WAPatchRegularLow, Patches: []*waServerSync.SyncdPatch{patch}}, HashState{Version: 79, Hash: before}, true)
	if err != nil {
		t.Fatalf("DecodePatches: %v", err)
	}
	if state.Hash != after {
		t.Fatal("legacy record under the old key was not subtracted")
	}
	if mem.has(string(WAPatchRegularLow), indexMACOf(oldL)) {
		t.Error("superseded legacy record was kept in the store")
	}
	if !mem.has(string(WAPatchRegularLow), indexMACOf(m)) {
		t.Error("an unrelated record was removed")
	}
}

// Our own SET under a new key must subtract the legacy record too, or the primary
// cannot verify the patch (the founder's 2026-10-03 contact rename).
func TestEncodePatchSubtractsLegacyRowUnderRotatedKey(t *testing.T) {
	proc, mem := newTestProcessor(t)
	keyA, keyB := addTestKey(t, mem, 1), addTestKey(t, mem, 2)
	oldL := encryptSet(t, keyA, indexL, markRead(1))
	before := phoneHash(valueMACOf(oldL))
	seedState(t, mem, 81, before, false, oldL)

	raw, err := proc.EncodePatch(context.Background(), keyB.id, HashState{Version: 81, Hash: before}, PatchInfo{
		Type:      WAPatchRegularLow,
		Mutations: []MutationInfo{{Index: indexL, Version: 3, Value: markRead(2)}},
	})
	if err != nil {
		t.Fatalf("EncodePatch: %v", err)
	}
	var patch waServerSync.SyncdPatch
	if err = proto.Unmarshal(raw, &patch); err != nil {
		t.Fatal(err)
	}
	after := phoneHash(valueMACOf(patch.GetMutations()[0]))
	want := (&HashState{Version: 82, Hash: after}).generateSnapshotMAC(WAPatchRegularLow, keyB.keys.SnapshotMAC)
	if !bytes.Equal(patch.GetSnapshotMAC(), want) {
		t.Fatal("snapshot MAC of the encoded patch keeps the legacy record written under the old key")
	}
}
