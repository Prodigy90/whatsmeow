// Copyright (c) 2026 Tulir Asokan
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package whatsmeow

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/util/cbcutil"
)

// Expected values come from an independent Python implementation of WAMediaCryptoSidecar
// (hmac.new(key, buf[n*65536 : n*65536+16+65536], sha256)[:10] over iv || encrypted), not
// from this package.
func TestCalculateStreamingSidecarReferenceVectors(t *testing.T) {
	macKey := sha256.Sum256([]byte("mac"))
	ivFull := sha256.Sum256([]byte("iv"))
	iv := ivFull[:16]
	stream := func(n int) []byte {
		var out []byte
		for i := uint32(0); len(out) < n; i++ {
			var b [4]byte
			binary.BigEndian.PutUint32(b[:], i)
			h := sha256.Sum256(b[:])
			out = append(out, h[:]...)
		}
		return out[:n]
	}
	cases := []struct {
		encLen  int
		chunks  int
		sha256H string
	}{
		{26, 1, "6d7bda0ec448b55517af16edce74a1ce2622759d089be6fda84bfc1b41de739d"},
		{65526, 1, "143d9765e3eb1bc9fcb8f2ee5ffc1bc6b09c738b0801956501ed309d832caa1e"},
		{65536, 1, "293d7606ea1cb4d108c0afb74754c2e1e418ab4a3dea623b374535c43fb68e09"},
		{65542, 2, "e75f8e6867aadf2833eedb6ba6bca59f2af49c8db69957ccbb1a0f78bb117b58"},
		{131072, 2, "0a7926741a8f1eb305d587ed128ffe07f138bed0e26bb999b40b006a8076c1ed"},
		{200010, 4, "e5376c5cd504bdad62028bb5e61a2e5602cc3c3f6a2fd77f23ee6622b7337fc2"},
	}
	for _, tc := range cases {
		sidecar, err := calculateStreamingSidecar(macKey[:], iv, bytes.NewReader(stream(tc.encLen)))
		if err != nil {
			t.Fatalf("len %d: %v", tc.encLen, err)
		}
		if len(sidecar) != tc.chunks*sidecarMACSize {
			t.Errorf("len %d: got %d sidecar bytes, want %d", tc.encLen, len(sidecar), tc.chunks*sidecarMACSize)
		}
		got := sha256.Sum256(sidecar)
		if hex.EncodeToString(got[:]) != tc.sha256H {
			t.Errorf("len %d: sidecar sha256 %x, want %s", tc.encLen, got, tc.sha256H)
		}
	}
}

// Both upload paths must hand calculateStreamingSidecar the same bytes they upload.
func TestStreamingSidecarMatchesBetweenUploadPaths(t *testing.T) {
	mediaKey := bytes.Repeat([]byte{7}, 32)
	iv, cipherKey, macKey, _ := getMediaKeys(mediaKey, MediaVideo)
	plaintext := bytes.Repeat([]byte("wasbot-video"), 30000) // ~330KB, 6 chunks

	ciphertext, err := cbcutil.Encrypt(cipherKey, iv, plaintext)
	if err != nil {
		t.Fatal(err)
	}
	h := hmac.New(sha256.New, macKey)
	h.Write(iv)
	h.Write(ciphertext)
	inMemory := append(ciphertext, h.Sum(nil)[:10]...)

	var streamed bytes.Buffer
	_, _, _, uploadSize, err := cbcutil.EncryptStream(cipherKey, iv, macKey, bytes.NewReader(plaintext), &streamed)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(inMemory, streamed.Bytes()) || uint64(len(inMemory)) != uploadSize {
		t.Fatal("encrypt paths disagree; test premise broken")
	}
	a, err := calculateStreamingSidecar(macKey, iv, bytes.NewReader(inMemory))
	if err != nil {
		t.Fatal(err)
	}
	b, err := calculateStreamingSidecar(macKey, iv, &streamed)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(a, b) || len(a) != 60 {
		t.Fatalf("sidecars differ or wrong size: %d vs %d bytes", len(a), len(b))
	}
}

// TestStreamingSidecarMatchesNativeSample checks against a sidecar a WhatsApp phone produced.
// The fixture is too large to commit; set WHATSMEOW_SIDECAR_FIXTURE to a directory holding
// video.enc (the downloaded encrypted file), media_key.b64 and sidecar.b64.
func TestStreamingSidecarMatchesNativeSample(t *testing.T) {
	dir := os.Getenv("WHATSMEOW_SIDECAR_FIXTURE")
	if dir == "" {
		t.Skip("WHATSMEOW_SIDECAR_FIXTURE not set")
	}
	read := func(name string) []byte {
		b, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	decode := func(name string) []byte {
		b, err := base64.StdEncoding.DecodeString(strings.TrimSpace(string(read(name))))
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	iv, _, macKey, _ := getMediaKeys(decode("media_key.b64"), MediaVideo)
	got, err := calculateStreamingSidecar(macKey, iv, bytes.NewReader(read("video.enc")))
	if err != nil {
		t.Fatal(err)
	}
	want := decode("sidecar.b64")
	if !bytes.Equal(got, want) {
		t.Fatalf("sidecar mismatch: got %d bytes, want %d", len(got), len(want))
	}
}

func TestUploadWithKeyRejectsBadKeyLength(t *testing.T) {
	_, err := (&Client{}).UploadWithKey(context.Background(), []byte("x"), MediaVideoThumbnail, make([]byte, 31))
	if err == nil {
		t.Fatal("expected an error for a 31-byte media key")
	}
}

func TestMessageThumbnailMediaTypes(t *testing.T) {
	for msg, want := range map[DownloadableThumbnail]string{
		&waE2E.ImageMessage{}: "thumbnail-image",
		&waE2E.VideoMessage{}: "thumbnail-video",
	} {
		mediaType := classToThumbnailMediaType[msg.ProtoReflect().Descriptor().Name()]
		if got := mediaTypeToMMSType[mediaType]; got != want {
			t.Errorf("%T: mms type %q, want %q", msg, got, want)
		}
	}
}
