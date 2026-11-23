// Copyright (c) 2026 Tulir Asokan
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package whatsmeow

import "testing"

// Under the tolerated-LTHash rule a drifted collection keeps pace with the
// server, so its repair snapshot arrives at the SAME version. Refusing equal
// versions (the pre-tolerance rule) would make recovery a no-op for exactly the
// collections that need it; accepting older ones would roll a collection back.
func TestShouldApplyRecovery(t *testing.T) {
	cases := []struct {
		name             string
		current, recover uint64
		drifted          bool
		want             bool
	}{
		{"newer snapshot, not drifted", 10, 11, false, true},
		{"newer snapshot, drifted", 10, 11, true, true},
		{"same version, not drifted", 10, 10, false, false},
		{"same version, drifted", 10, 10, true, true},
		{"older snapshot, not drifted", 10, 9, false, false},
		{"older snapshot, drifted", 10, 9, true, false},
	}
	for _, c := range cases {
		if got := shouldApplyRecovery(c.current, c.recover, c.drifted); got != c.want {
			t.Errorf("%s: shouldApplyRecovery(%d, %d, %t) = %t, want %t", c.name, c.current, c.recover, c.drifted, got, c.want)
		}
	}
}

// A client starts with the LTHash tolerance off (behaving as before upstream #1319)
// and SetTolerateLTHashMismatch is what reaches the processor that decodes patches.
func TestClientLTHashToleranceDefaultsOff(t *testing.T) {
	cli, _, _ := statusRoutingClient(t)
	if cli.appStateProc.TolerateLTHashMismatch() {
		t.Fatal("a new client tolerates LTHash mismatches; the fork default must be off")
	}
	cli.SetTolerateLTHashMismatch(true)
	if !cli.appStateProc.TolerateLTHashMismatch() {
		t.Fatal("SetTolerateLTHashMismatch(true) did not reach the app state processor")
	}
	cli.SetTolerateLTHashMismatch(false)
	if cli.appStateProc.TolerateLTHashMismatch() {
		t.Fatal("SetTolerateLTHashMismatch(false) did not turn the tolerance back off")
	}
}
