package whatsmeow

import (
	"strconv"
	"testing"

	"go.mau.fi/whatsmeow/types"
)

// TestUsyncDeviceChunks locks the ban-critical invariant: GetUserDevices must
// never put more than maxUsyncUsersPerQuery (500) <user> nodes in a single
// usync IQ. A single oversized usync (we observed 2K-21.8K) precedes a 403
// account lock empirically, so this guards the split the resolver relies on.
func TestUsyncDeviceChunks(t *testing.T) {
	mk := func(n int) []types.JID {
		jids := make([]types.JID, n)
		for i := range jids {
			jids[i] = types.JID{User: strconv.Itoa(i), Server: types.DefaultUserServer}
		}
		return jids
	}

	for _, n := range []int{0, 1, 499, 500, 501, 1000, 1001, 21800} {
		chunks := usyncDeviceChunks(mk(n))

		// 1. No chunk may exceed the cap.
		total := 0
		for i, c := range chunks {
			if len(c) > maxUsyncUsersPerQuery {
				t.Fatalf("n=%d: chunk %d has %d users, exceeds cap %d", n, i, len(c), maxUsyncUsersPerQuery)
			}
			if len(c) == 0 {
				t.Fatalf("n=%d: chunk %d is empty", n, i)
			}
			total += len(c)
		}

		// 2. Chunks must cover every input JID exactly once, in order.
		if total != n {
			t.Fatalf("n=%d: chunks cover %d jids, want %d", n, total, n)
		}
		want := mk(n)
		idx := 0
		for _, c := range chunks {
			for _, jid := range c {
				if jid != want[idx] {
					t.Fatalf("n=%d: order broken at %d: got %s want %s", n, idx, jid, want[idx])
				}
				idx++
			}
		}

		// 3. Chunk count must be ceil(n/cap).
		wantChunks := (n + maxUsyncUsersPerQuery - 1) / maxUsyncUsersPerQuery
		if len(chunks) != wantChunks {
			t.Fatalf("n=%d: got %d chunks, want %d", n, len(chunks), wantChunks)
		}
	}
}
