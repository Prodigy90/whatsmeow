// Copyright (c) 2026 Prodigy90
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package whatsmeow

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"

	"golang.org/x/crypto/hkdf"

	waBinary "go.mau.fi/whatsmeow/binary"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/util/keys"
	waLog "go.mau.fi/whatsmeow/util/log"
)

// encryptLikePrimary builds an <encryption-metadata> node the way the primary does
// (whatsapp-rust wacore/src/mobile/primary/iq.rs derive_companion_pem_key), so the
// decrypt path is tested against an independent statement of the recipe.
func encryptLikePrimary(t *testing.T, advSecret, noisePub []byte, plaintext string) waBinary.Node {
	t.Helper()
	key := make([]byte, 32)
	if _, err := io.ReadFull(hkdf.New(sha256.New, advSecret, noisePub, []byte("Canonical Ent Companion Nonce Encrypt")), key); err != nil {
		t.Fatal(err)
	}
	block, _ := aes.NewCipher(key)
	gcm, _ := cipher.NewGCM(block)
	nonce := make([]byte, gcm.NonceSize())
	_, _ = rand.Read(nonce)
	sealed := gcm.Seal(nil, nonce, []byte(plaintext), nil)
	ct, tag := sealed[:len(sealed)-16], sealed[len(sealed)-16:]
	return waBinary.Node{Tag: "pair-success", Content: []waBinary.Node{
		{Tag: "jurisdiction"},
		{Tag: "encryption-metadata", Attrs: waBinary.Attrs{"version": "1", "algorithm": "aes-256-gcm"}, Content: []waBinary.Node{
			{Tag: "encrypted_key", Content: []byte{0xAA}},
			{Tag: "nonce", Content: nonce},
			{Tag: "encrypted_data", Content: ct},
			{Tag: "auth_tag", Content: tag},
		}},
		{Tag: "device-identity", Content: []byte{1}},
	}}
}

func testKeys(t *testing.T) ([]byte, *keys.KeyPair) {
	adv := make([]byte, 32)
	_, _ = rand.Read(adv)
	return adv, keys.NewKeyPair()
}

func TestDecryptCanonicalNonce(t *testing.T) {
	adv, noise := testKeys(t)
	node := encryptLikePrimary(t, adv, noise.Pub[:], `{"nonce":"n-123","fbid":100012345678901,"timestamp":1793937134}`)
	meta, err := parseCanonicalEncryptionMetadata(node)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := decryptCanonicalNonce(adv, noise.Pub[:], meta)
	if err != nil {
		t.Fatal(err)
	}
	// fbid must survive as an exact integer; float64 would round a 15-digit id.
	if payload.Nonce != "n-123" || payload.FBID.String() != "100012345678901" || payload.AccessToken != "" {
		t.Fatalf("unexpected payload %+v", payload)
	}
	// Wrong key material (e.g. salt with the 0x05 prefix, or swapped ikm/salt) must fail.
	prefixed := append([]byte{0x05}, noise.Pub[:]...)
	if _, err = decryptCanonicalNonce(adv, prefixed, meta); err == nil {
		t.Fatal("decrypt with 33-byte prefixed salt should fail")
	}
	if _, err = decryptCanonicalNonce(noise.Pub[:], adv, meta); err == nil {
		t.Fatal("decrypt with swapped ikm/salt should fail")
	}
}

func TestDecryptCanonicalNonceMissingFields(t *testing.T) {
	adv, noise := testKeys(t)
	node := encryptLikePrimary(t, adv, noise.Pub[:], `{"timestamp":1}`)
	meta, _ := parseCanonicalEncryptionMetadata(node)
	if _, err := decryptCanonicalNonce(adv, noise.Pub[:], meta); err != errCanonicalBadPayload {
		t.Fatalf("want errCanonicalBadPayload, got %v", err)
	}
}

func TestParseCanonicalNoMetadata(t *testing.T) {
	if _, err := parseCanonicalEncryptionMetadata(waBinary.Node{Tag: "pair-success"}); err != errCanonicalNoMetadata {
		t.Fatalf("want errCanonicalNoMetadata, got %v", err)
	}
}

func TestExtractCanonicalPageTokens(t *testing.T) {
	html := `<script>requireLazy(["ServerJS"],function(){(new ServerJS()).handle({"define":[["LSD",[],{"token":"AVq1x_Zk"},323],["SprinkleConfig",[],{"param_name":"jazoest","version":2,"should_randomize":false},2111]]})});</script>`
	tok, err := extractCanonicalPageTokens(html)
	if err != nil {
		t.Fatal(err)
	}
	if tok.LSD != "AVq1x_Zk" || tok.SprinkleParam != "jazoest" {
		t.Fatalf("unexpected tokens %+v", tok)
	}
	sum := 0
	for _, r := range "AVq1x_Zk" {
		sum += int(r)
	}
	if got, want := tok.sprinkleValue(), "2"+strconv.Itoa(sum); got != want {
		t.Fatalf("jazoest = %s, want %s", got, want)
	}
	if _, err = extractCanonicalPageTokens("<html></html>"); err == nil {
		t.Fatal("expected error without LSD")
	}
}

// fakeWAWeb serves the bootstrap page and records the token POST.
type fakeWAWeb struct {
	posts     []*http.Request
	postForms []url.Values
	status    string
	httpCode  int
}

func (f *fakeWAWeb) server(t *testing.T) *httptest.Server {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/":
			http.SetCookie(w, &http.Cookie{Name: "wa_lang_pref", Value: "en", Path: "/"})
			_, _ = io.WriteString(w, `["LSD",[],{"token":"TOK"},1]`)
		case r.Method == http.MethodPost && r.URL.Path == "/auth/token/":
			_ = r.ParseForm()
			f.posts = append(f.posts, r)
			f.postForms = append(f.postForms, r.PostForm)
			if f.httpCode != 0 {
				w.WriteHeader(f.httpCode)
				return
			}
			_, _ = io.WriteString(w, `for (;;);{"__ar":1,"payload":{"status":"`+f.status+`"}}`)
		default:
			http.NotFound(w, r)
		}
	}))
	oldBase, oldToken := canonicalBaseURL, canonicalTokenURL
	canonicalBaseURL, canonicalTokenURL = srv.URL+"/", srv.URL+"/auth/token/"
	t.Cleanup(func() { canonicalBaseURL, canonicalTokenURL = oldBase, oldToken; srv.Close() })
	return srv
}

func newCanonicalTestClient(adv []byte, noise *keys.KeyPair, mode CanonicalRegistrationMode) *Client {
	cli := &Client{Store: &store.Device{AdvSecretKey: adv, NoiseKey: noise}, Log: waLog.Noop, preLoginHTTP: &http.Client{}}
	cli.CanonicalRegistration = mode
	return cli
}

func TestCanonicalExchangeRequestShape(t *testing.T) {
	adv, noise := testKeys(t)
	fake := &fakeWAWeb{status: "success"}
	fake.server(t)
	cli := newCanonicalTestClient(adv, noise, CanonicalRegistrationExchange)
	node := encryptLikePrimary(t, adv, noise.Pub[:], `{"nonce":"abc","fbid":"777","access_token":"tkn"}`)

	cli.handleCanonicalRegistration(context.Background(), node, 35)

	if len(fake.posts) != 1 {
		t.Fatalf("want 1 POST, got %d", len(fake.posts))
	}
	q := fake.posts[0].URL.Query()
	if q.Get("nonce") != "abc" || q.Get("user_id") != "777" || q.Get("device_id") != "35" || q.Get("access_token") != "tkn" {
		t.Fatalf("credentials must be in the query like WAXWhatsAppWebAuthControllerRouteBuilder: %v", q)
	}
	form := fake.postForms[0]
	if form.Get("lsd") != "TOK" || form.Get("__a") != "1" || form.Get("jazoest") == "" {
		t.Fatalf("unexpected form %v", form)
	}
	if fake.posts[0].Header.Get("X-FB-LSD") != "TOK" {
		t.Fatal("missing X-FB-LSD header")
	}
	if c, err := fake.posts[0].Cookie("wa_lang_pref"); err != nil || c.Value != "en" {
		t.Fatal("bootstrap cookies must be sent on the POST")
	}
}

func TestCanonicalModesThatMustNotPost(t *testing.T) {
	adv, noise := testKeys(t)
	for _, mode := range []CanonicalRegistrationMode{CanonicalRegistrationOff, CanonicalRegistrationLogOnly} {
		fake := &fakeWAWeb{status: "success"}
		fake.server(t)
		cli := newCanonicalTestClient(adv, noise, mode)
		cli.handleCanonicalRegistration(context.Background(), encryptLikePrimary(t, adv, noise.Pub[:], `{"nonce":"a","fbid":"1"}`), 2)
		if len(fake.posts) != 0 {
			t.Fatalf("mode %d made %d POSTs; it must make none", mode, len(fake.posts))
		}
	}
}

func TestCanonicalExchangeFailureStatusIsNotRetried(t *testing.T) {
	adv, noise := testKeys(t)
	fake := &fakeWAWeb{status: "failure"}
	fake.server(t)
	cli := newCanonicalTestClient(adv, noise, CanonicalRegistrationExchange)
	payload := &canonicalNoncePayload{Nonce: "a", FBID: "1"}
	if _, err := cli.exchangeCanonicalNonce(context.Background(), payload, 2); err == nil {
		t.Fatal("non-success status must be an error")
	}
	if len(fake.posts) != 1 {
		t.Fatalf("a definite server answer must not be retried, got %d POSTs", len(fake.posts))
	}
}

func TestCanonicalExchangeRetriesOnceOn5xx(t *testing.T) {
	adv, noise := testKeys(t)
	fake := &fakeWAWeb{httpCode: http.StatusBadGateway}
	fake.server(t)
	cli := newCanonicalTestClient(adv, noise, CanonicalRegistrationExchange)
	if _, err := cli.exchangeCanonicalNonce(context.Background(), &canonicalNoncePayload{Nonce: "a", FBID: "1"}, 2); err == nil {
		t.Fatal("5xx must be an error")
	}
	if len(fake.posts) != 2 {
		t.Fatalf("want exactly one retry (2 POSTs) like fetchFromXController, got %d", len(fake.posts))
	}
}
