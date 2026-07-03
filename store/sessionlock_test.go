// Copyright (c) 2026 Tulir Asokan
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package store

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestLockSessionMutualExclusion proves LockSession serializes access to a
// single address — the property the whole #1168 fix relies on. If the lock
// leaked, more than one goroutine would be "inside" the critical section at
// once and maxActive would exceed 1.
func TestLockSessionMutualExclusion(t *testing.T) {
	d := &Device{}
	var active, maxActive int32
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			unlock := d.LockSession("addr-A")
			n := atomic.AddInt32(&active, 1)
			for {
				old := atomic.LoadInt32(&maxActive)
				if n <= old || atomic.CompareAndSwapInt32(&maxActive, old, n) {
					break
				}
			}
			time.Sleep(time.Millisecond) // force overlap if the lock were broken
			atomic.AddInt32(&active, -1)
			unlock()
		}()
	}
	wg.Wait()
	if maxActive != 1 {
		t.Fatalf("LockSession allowed %d concurrent holders of one address, want 1", maxActive)
	}
}

// TestLockSessionDifferentAddressesConcurrent proves distinct addresses don't
// block each other — unrelated chats must stay parallel (the reason the lock is
// per-address, not global).
func TestLockSessionDifferentAddressesConcurrent(t *testing.T) {
	d := &Device{}
	start := make(chan struct{})
	reached := make(chan struct{}, 2)
	var wg sync.WaitGroup
	for _, addr := range []string{"A", "B"} {
		wg.Add(1)
		go func(a string) {
			defer wg.Done()
			unlock := d.LockSession(a)
			defer unlock()
			<-start // hold my own lock, then wait for the go-signal
			reached <- struct{}{}
		}(addr)
	}
	time.Sleep(10 * time.Millisecond) // let both acquire their (different) locks
	close(start)
	timeout := time.After(2 * time.Second)
	for i := 0; i < 2; i++ {
		select {
		case <-reached:
		case <-timeout:
			t.Fatal("different-address locks blocked each other")
		}
	}
	wg.Wait()
}

// TestLockSessionsNoDeadlockOverlapping hammers LockSessions with overlapping
// address sets supplied in OPPOSITE input orders. Locking in input order would
// deadlock ({A,B,C} vs {C,B,A}); sorting before acquisition prevents it.
func TestLockSessionsNoDeadlockOverlapping(t *testing.T) {
	d := &Device{}
	done := make(chan struct{})
	go func() {
		var wg sync.WaitGroup
		for i := 0; i < 200; i++ {
			wg.Add(2)
			go func() { defer wg.Done(); u := d.LockSessions([]string{"A", "B", "C"}); u() }()
			go func() { defer wg.Done(); u := d.LockSessions([]string{"C", "B", "A"}); u() }()
		}
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("LockSessions deadlocked on overlapping sets in different input orders")
	}
}

// TestLockSessionsDeduplicates guards the slices.Compact step: locking the same
// address twice on one sync.Mutex without dedup would self-deadlock.
func TestLockSessionsDeduplicates(t *testing.T) {
	d := &Device{}
	done := make(chan struct{})
	go func() {
		unlock := d.LockSessions([]string{"X", "X", "X"})
		unlock()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("LockSessions self-deadlocked on duplicate addresses (missing dedup)")
	}
}

// TestLockSessionsEmpty: the no-op releaser for an empty set must be safe to call.
func TestLockSessionsEmpty(t *testing.T) {
	d := &Device{}
	unlock := d.LockSessions(nil)
	unlock()
	unlock = d.LockSessions([]string{})
	unlock()
}
