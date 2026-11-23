// Copyright (c) 2026 Tulir Asokan
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package appstate

import (
	"context"
	"errors"
	"testing"

	"go.mau.fi/whatsmeow/proto/waServerSync"
)

// These cover how two behaviours combine in validatePatch: this fork's retry
// with the same-index-MAC rule (for patches made by older companions) runs
// first, and only if that also fails does upstream's tolerance (#1319) accept
// a patch whose LTHash mismatches but whose patch MAC is valid.

// rotatedKeyFixture seeds v10 with one record for indexL under keyA. A SET of
// indexL under keyB then has a collapse plan (it supersedes a record written
// under another key), so the legacy retry is reachable.
func rotatedKeyFixture(t *testing.T, seedHash bool) (*Processor, *memAppState, testKey, *waServerSync.SyncdMutation, HashState) {
	t.Helper()
	proc, mem := newTestProcessor(t)
	keyA, keyB := addTestKey(t, mem, 1), addTestKey(t, mem, 2)
	oldL := encryptSet(t, keyA, indexL, markRead(1))
	before := HashState{Version: 10}
	if seedHash {
		before.Hash = phoneHash(valueMACOf(oldL))
	}
	seedState(t, mem, before.Version, before.Hash, true, oldL)
	return proc, mem, keyB, oldL, before
}

func decodeOne(proc *Processor, patch *waServerSync.SyncdPatch, initial HashState) (HashState, error) {
	_, state, err := proc.DecodePatches(context.Background(), &PatchList{Name: WAPatchRegularLow, Patches: []*waServerSync.SyncdPatch{patch}}, initial, true)
	return state, err
}

// A patch made with the legacy rule still verifies through the legacy retry, and
// its state is what we keep. If the tolerance swallowed it first, the state would
// be the collapsed hash instead and the superseded record would be dropped.
func TestValidatePatchLegacyRetryRunsBeforeTolerance(t *testing.T) {
	proc, mem, keyB, oldL, before := rotatedKeyFixture(t, true)
	newL := encryptSet(t, keyB, indexL, markRead(2))
	legacyHash := phoneHash(valueMACOf(oldL), valueMACOf(newL))
	state, err := decodeOne(proc, patchOf(11, legacyHash, keyB, newL), before)
	if err != nil {
		t.Fatalf("DecodePatches: %v", err)
	}
	if state.Hash != legacyHash {
		t.Fatal("state is not the legacy hash: the legacy retry did not run")
	}
	if !mem.has(string(WAPatchRegularLow), indexMACOf(oldL)) {
		t.Error("record still counted by the legacy hash was dropped")
	}
}

// Neither rule matches the patch's snapshot MAC, but the patch MAC is valid and
// local state is consecutive and non-empty: accepted, as WhatsApp Web does. The
// state keeps the every-record subtraction (the collapse plan), not the legacy one.
func TestValidatePatchToleratesMismatchWithValidPatchMAC(t *testing.T) {
	proc, mem, keyB, oldL, before := rotatedKeyFixture(t, true)
	newL := encryptSet(t, keyB, indexL, markRead(2))
	unrelated := phoneHash([]byte("drifted state the server signed over"))
	state, err := decodeOne(proc, patchOf(11, unrelated, keyB, newL), before)
	if err != nil {
		t.Fatalf("DecodePatches: %v", err)
	}
	if state.Version != 11 {
		t.Fatalf("version = %d, want 11", state.Version)
	}
	if state.Hash != phoneHash(valueMACOf(newL)) {
		t.Fatal("tolerated state does not follow the collapse plan")
	}
	if mem.has(string(WAPatchRegularLow), indexMACOf(oldL)) {
		t.Error("superseded record kept: the collapse plan was not used")
	}
	if !mem.has(string(WAPatchRegularLow), indexMACOf(newL)) {
		t.Error("new record missing from the store")
	}
}

// The tolerance only applies when local state is plausibly intact.
func TestValidatePatchToleranceNeedsIntactLocalState(t *testing.T) {
	unrelated := phoneHash([]byte("drifted state the server signed over"))

	t.Run("skipped version", func(t *testing.T) {
		proc, _, keyB, _, before := rotatedKeyFixture(t, true)
		newL := encryptSet(t, keyB, indexL, markRead(2))
		_, err := decodeOne(proc, patchOf(12, unrelated, keyB, newL), before)
		if !errors.Is(err, ErrMismatchingLTHash) {
			t.Fatalf("expected ErrMismatchingLTHash, got %v", err)
		}
	})
	t.Run("empty local hash", func(t *testing.T) {
		proc, _, keyB, _, before := rotatedKeyFixture(t, false)
		newL := encryptSet(t, keyB, indexL, markRead(2))
		_, err := decodeOne(proc, patchOf(11, unrelated, keyB, newL), before)
		if !errors.Is(err, ErrMismatchingLTHash) {
			t.Fatalf("expected ErrMismatchingLTHash, got %v", err)
		}
	})
}

// A mismatching LTHash is never excused when the patch MAC is wrong too.
func TestValidatePatchToleranceRequiresPatchMAC(t *testing.T) {
	proc, _, keyB, _, before := rotatedKeyFixture(t, true)
	newL := encryptSet(t, keyB, indexL, markRead(2))
	patch := patchOf(11, phoneHash([]byte("drifted")), keyB, newL)
	patch.PatchMAC = append([]byte(nil), patch.PatchMAC...)
	patch.PatchMAC[0] ^= 0xff
	_, err := decodeOne(proc, patch, before)
	if !errors.Is(err, ErrMismatchingPatchMAC) {
		t.Fatalf("expected ErrMismatchingPatchMAC, got %v", err)
	}
}

// The tolerance swallows what used to be an ErrMismatchingLTHash error, and that
// error was the only thing that told a caller the collection had drifted and
// needed repair. OnLTHashTolerated replaces it: it must fire exactly when the
// tolerance accepts a mismatching patch, and never for a patch that verified
// cleanly or one that was refused.
func TestValidatePatchToleranceReportsDrift(t *testing.T) {
	type call struct {
		name    WAPatchName
		version uint64
	}
	record := func(proc *Processor) *[]call {
		var calls []call
		proc.OnLTHashTolerated = func(name WAPatchName, version uint64) {
			calls = append(calls, call{name, version})
		}
		return &calls
	}

	t.Run("tolerated", func(t *testing.T) {
		proc, _, keyB, _, before := rotatedKeyFixture(t, true)
		calls := record(proc)
		newL := encryptSet(t, keyB, indexL, markRead(2))
		if _, err := decodeOne(proc, patchOf(11, phoneHash([]byte("drifted")), keyB, newL), before); err != nil {
			t.Fatalf("DecodePatches: %v", err)
		}
		if len(*calls) != 1 || (*calls)[0] != (call{WAPatchRegularLow, 11}) {
			t.Fatalf("OnLTHashTolerated calls = %+v, want one for regular_low v11", *calls)
		}
	})
	t.Run("clean patch", func(t *testing.T) {
		proc, _, keyB, oldL, before := rotatedKeyFixture(t, true)
		calls := record(proc)
		newL := encryptSet(t, keyB, indexL, markRead(2))
		legacyHash := phoneHash(valueMACOf(oldL), valueMACOf(newL))
		if _, err := decodeOne(proc, patchOf(11, legacyHash, keyB, newL), before); err != nil {
			t.Fatalf("DecodePatches: %v", err)
		}
		if len(*calls) != 0 {
			t.Fatalf("OnLTHashTolerated fired for a patch that verified: %+v", *calls)
		}
	})
	t.Run("refused patch", func(t *testing.T) {
		proc, _, keyB, _, before := rotatedKeyFixture(t, true)
		calls := record(proc)
		newL := encryptSet(t, keyB, indexL, markRead(2))
		if _, err := decodeOne(proc, patchOf(12, phoneHash([]byte("drifted")), keyB, newL), before); !errors.Is(err, ErrMismatchingLTHash) {
			t.Fatalf("expected ErrMismatchingLTHash, got %v", err)
		}
		if len(*calls) != 0 {
			t.Fatalf("OnLTHashTolerated fired for a refused patch: %+v", *calls)
		}
	})
}
