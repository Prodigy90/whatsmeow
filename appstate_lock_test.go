package whatsmeow

import (
	"context"
	"testing"
	"time"

	"go.mau.fi/whatsmeow/appstate"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types"
)

// A send must read the stored version and encode against it while holding
// appStateSyncLock, or a concurrent fetch can move the version/MACs underneath it
// and the patch goes out with a snapshot MAC no other device can verify.
func TestEncodeAppStatePatchWaitsForSyncLock(t *testing.T) {
	cli := &Client{Store: store.NoopDevice}
	cli.appStateSyncLock.Lock()

	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _, _ = cli.encodeAppStatePatch(context.Background(), appstate.BuildContact(testContactJID(), "x", true))
	}()

	select {
	case <-done:
		cli.appStateSyncLock.Unlock()
		t.Fatal("encodeAppStatePatch ran while appStateSyncLock was held")
	case <-time.After(100 * time.Millisecond):
	}
	cli.appStateSyncLock.Unlock()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("encodeAppStatePatch did not finish after the lock was released")
	}
}

// Two SendAppState calls must not overlap: the second waits for the first.
func TestSendAppStateIsSerialised(t *testing.T) {
	cli := &Client{Store: store.NoopDevice}
	cli.appStateSendLock.Lock()

	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = cli.SendAppState(context.Background(), appstate.BuildContact(testContactJID(), "x", true))
	}()
	select {
	case <-done:
		cli.appStateSendLock.Unlock()
		t.Fatal("SendAppState ran while another send held appStateSendLock")
	case <-time.After(100 * time.Millisecond):
	}
	cli.appStateSendLock.Unlock()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("SendAppState did not finish after the lock was released")
	}
}

func testContactJID() types.JID { return types.NewJID("15550000000", types.DefaultUserServer) }
