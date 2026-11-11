// Copyright (c) 2026 Prodigy90
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package whatsmeow

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"golang.org/x/crypto/hkdf"

	waBinary "go.mau.fi/whatsmeow/binary"
)

// CanonicalRegistrationMode controls what the client does with the
// <encryption-metadata> block the primary attaches to pair-success.
//
// WhatsApp Web (WAWebHandlePairSuccess → WAWebHandleCanonicalRegistration →
// WAWebCanonicalTokenExchange) decrypts that block and redeems the nonce inside
// it at https://web.whatsapp.com/auth/token/ BEFORE replying pair-device-sign.
// Upstream whatsmeow ignores the block entirely.
type CanonicalRegistrationMode int

const (
	// CanonicalRegistrationOff ignores the block, like upstream whatsmeow.
	CanonicalRegistrationOff CanonicalRegistrationMode = iota
	// CanonicalRegistrationLogOnly decrypts the block and logs what it found
	// (never the token or nonce values) but makes no HTTP request.
	CanonicalRegistrationLogOnly
	// CanonicalRegistrationExchange decrypts the block and redeems the nonce the
	// way WhatsApp Web does, before confirming the pairing. A failed exchange is
	// logged and pairing continues, as in WhatsApp Web.
	CanonicalRegistrationExchange
)

// CanonicalOutcome* are the one-word results of handleCanonicalRegistration,
// recorded per link so the two cohorts (success vs failure) can be compared
// against the post-link logout rate. They are plain strings so the events
// package can carry one without importing whatsmeow.
const (
	CanonicalOutcomeOff            = "off"             // feature disabled
	CanonicalOutcomeNoMetadata     = "no_metadata"     // server sent no <encryption-metadata>
	CanonicalOutcomeDecryptFailed  = "decrypt_failed"  // block present but decrypt/parse failed
	CanonicalOutcomeLogged         = "log_only"        // decrypted, no exchange (log mode)
	CanonicalOutcomeExchangeFailed = "exchange_failed" // exchange attempted, server did not say success
	CanonicalOutcomeSuccess        = "success"         // /auth/token/ returned status=success
)

// Variables rather than constants so tests can point them at a local server.
var (
	canonicalBaseURL  = "https://web.whatsapp.com/"
	canonicalTokenURL = "https://web.whatsapp.com/auth/token/"
)

const (
	canonicalHKDFInfo = "Canonical Ent Companion Nonce Encrypt"
	// This whole operation sits between pair-success and our pair-device-sign reply,
	// so it holds up pairing. WhatsApp Web's own gap there is ~0.22s and the measured
	// exchange was 1.25s; cap the budget low so a slow/hung server can't stall the
	// link past WhatsApp's own IQ timeout. One retry, short delay.
	canonicalExchangeBudget = 4 * time.Second
	canonicalRetryDelay     = 1 * time.Second
)

// CanonicalUserAgent is sent on the bootstrap GET and the token POST. It should
// look like the browser the client claims to be in its ClientPayload.
var CanonicalUserAgent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36"

// canonicalEncryptionMetadata is the parsed <encryption-metadata> child of pair-success
// (WASmaxInMdAESEncryptionMetadataMixin). encrypted_key is not used on this path.
type canonicalEncryptionMetadata struct {
	Nonce         []byte
	EncryptedData []byte
	AuthTag       []byte
}

// canonicalNoncePayload is the decrypted JSON (WAWebHandleCanonicalRegistration.parseNoncePayload).
type canonicalNoncePayload struct {
	Nonce       string      `json:"nonce"`
	FBID        json.Number `json:"fbid"`
	Timestamp   json.Number `json:"timestamp"`
	AccessToken string      `json:"access_token"`
}

var (
	errCanonicalNoMetadata = errors.New("no encryption-metadata in pair-success")
	errCanonicalBadPayload = errors.New("decrypted nonce payload missing nonce or fbid")
)

func parseCanonicalEncryptionMetadata(pairSuccess waBinary.Node) (*canonicalEncryptionMetadata, error) {
	node, ok := pairSuccess.GetOptionalChildByTag("encryption-metadata")
	if !ok {
		return nil, errCanonicalNoMetadata
	}
	if alg := node.AttrGetter().OptionalString("algorithm"); alg != "" && alg != "aes-256-gcm" {
		return nil, fmt.Errorf("unsupported encryption-metadata algorithm %q", alg)
	}
	get := func(tag string) []byte {
		child, _ := node.GetOptionalChildByTag(tag)
		b, _ := child.Content.([]byte)
		return b
	}
	meta := &canonicalEncryptionMetadata{
		Nonce:         get("nonce"),
		EncryptedData: get("encrypted_data"),
		AuthTag:       get("auth_tag"),
	}
	if len(meta.Nonce) == 0 || len(meta.EncryptedData) == 0 || len(meta.AuthTag) == 0 {
		return nil, errors.New("encryption-metadata is missing nonce, encrypted_data or auth_tag")
	}
	return meta, nil
}

// decryptCanonicalNonce mirrors WAWebHandleCanonicalRegistration.decryptNonce:
// key = HKDF-SHA256(ikm = ADV secret, salt = noise static public key, info, 32),
// AES-256-GCM with iv = <nonce>, ciphertext = <encrypted_data> || <auth_tag>.
func decryptCanonicalNonce(advSecret, noisePub []byte, meta *canonicalEncryptionMetadata) (*canonicalNoncePayload, error) {
	key := make([]byte, 32)
	if _, err := io.ReadFull(hkdf.New(sha256.New, advSecret, noisePub, []byte(canonicalHKDFInfo)), key); err != nil {
		return nil, fmt.Errorf("derive key: %w", err)
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCMWithNonceSize(block, len(meta.Nonce))
	if err != nil {
		return nil, err
	}
	plaintext, err := gcm.Open(nil, meta.Nonce, concatBytes(meta.EncryptedData, meta.AuthTag), nil)
	if err != nil {
		return nil, fmt.Errorf("decrypt: %w", err)
	}
	var payload canonicalNoncePayload
	dec := json.NewDecoder(bytes.NewReader(plaintext))
	dec.UseNumber()
	if err = dec.Decode(&payload); err != nil {
		return nil, fmt.Errorf("parse payload: %w", err)
	}
	if payload.Nonce == "" || payload.FBID == "" {
		return nil, errCanonicalBadPayload
	}
	return &payload, nil
}

// handleCanonicalRegistration is called from completePair before pair-device-sign is
// sent. It never returns an error: like WhatsApp Web, a failure here must not stop
// pairing. It returns a CanonicalOutcome* string, recorded per link.
func (cli *Client) handleCanonicalRegistration(ctx context.Context, pairSuccess waBinary.Node, deviceID uint16) string {
	mode := cli.CanonicalRegistration
	if mode == CanonicalRegistrationOff {
		return CanonicalOutcomeOff
	}
	log := cli.Log.Sub("Canonical")
	meta, err := parseCanonicalEncryptionMetadata(pairSuccess)
	if err != nil {
		log.Infof("Skipping canonical registration: %v", err)
		return CanonicalOutcomeNoMetadata
	}
	payload, err := decryptCanonicalNonce(cli.Store.AdvSecretKey, cli.Store.NoiseKey.Pub[:], meta)
	if err != nil {
		log.Warnf("Failed to decrypt canonical nonce: %v", err)
		return CanonicalOutcomeDecryptFailed
	}
	log.Infof("Decrypted canonical nonce (nonce_len=%d has_access_token=%t has_timestamp=%t device_id=%d mode=%d)",
		len(payload.Nonce), payload.AccessToken != "", payload.Timestamp != "", deviceID, mode)
	if mode != CanonicalRegistrationExchange {
		return CanonicalOutcomeLogged
	}
	ctx, cancel := context.WithTimeout(ctx, canonicalExchangeBudget)
	defer cancel()
	start := time.Now()
	status, err := cli.exchangeCanonicalNonce(ctx, payload, deviceID)
	if err != nil {
		log.Warnf("Canonical token exchange failed after %s: %v", time.Since(start).Round(time.Millisecond), err)
		return CanonicalOutcomeExchangeFailed
	}
	log.Infof("Canonical token exchange finished in %s with status %q", time.Since(start).Round(time.Millisecond), status)
	return CanonicalOutcomeSuccess
}

var (
	canonicalLSDPatterns = []*regexp.Regexp{
		regexp.MustCompile(`\["LSD",\[\],\{"token":"([^"]+)"\}`),
		regexp.MustCompile(`"lsd":\{"token":"([^"]+)"`),
		regexp.MustCompile(`"LSD"[\s\S]{0,64}?"token":"([^"]+)"`),
	}
	canonicalSprinklePattern = regexp.MustCompile(`\["SprinkleConfig",\[\],\{"param_name":"([^"]+)","version":(\d+),"should_randomize":(true|false)`)
)

type canonicalPageTokens struct {
	LSD            string
	SprinkleParam  string
	SprinkleVer    string
	SprinkleRandom bool
}

func extractCanonicalPageTokens(html string) (*canonicalPageTokens, error) {
	tok := &canonicalPageTokens{SprinkleParam: "jazoest", SprinkleVer: "2"}
	for _, re := range canonicalLSDPatterns {
		if m := re.FindStringSubmatch(html); m != nil {
			tok.LSD = m[1]
			break
		}
	}
	if tok.LSD == "" {
		return nil, errors.New("no LSD token in web.whatsapp.com page")
	}
	if m := canonicalSprinklePattern.FindStringSubmatch(html); m != nil {
		tok.SprinkleParam, tok.SprinkleVer, tok.SprinkleRandom = m[1], m[2], m[3] == "true"
	}
	return tok, nil
}

// sprinkleValue mirrors DTSGUtils.getNumericValue.
func (t *canonicalPageTokens) sprinkleValue() string {
	sum := 0
	for _, r := range t.LSD {
		sum += int(r)
	}
	if t.SprinkleRandom {
		return strconv.Itoa(sum)
	}
	return t.SprinkleVer + strconv.Itoa(sum)
}

func (cli *Client) canonicalHTTPClient() (*http.Client, error) {
	jar, err := cookiejar.New(nil)
	if err != nil {
		return nil, err
	}
	hc := &http.Client{Jar: jar, Timeout: canonicalExchangeBudget}
	if cli.preLoginHTTP != nil {
		hc.Transport = cli.preLoginHTTP.Transport
	}
	return hc, nil
}

// exchangeCanonicalNonce mirrors WAWebCanonicalTokenExchange: the credentials go in the
// query (WAXWhatsAppWebAuthControllerRouteBuilder.buildUri), the form body carries the
// page-session params from getAsyncParams("POST"). We send the subset a non-logged-in
// web.whatsapp.com page has (__a, __user=0, lsd, jazoest) plus X-FB-LSD.
func (cli *Client) exchangeCanonicalNonce(ctx context.Context, payload *canonicalNoncePayload, deviceID uint16) (string, error) {
	hc, err := cli.canonicalHTTPClient()
	if err != nil {
		return "", err
	}
	tokens, err := cli.fetchCanonicalPageTokens(ctx, hc)
	if err != nil {
		return "", fmt.Errorf("bootstrap: %w", err)
	}
	query := url.Values{}
	query.Set("access_token", payload.AccessToken)
	query.Set("nonce", payload.Nonce)
	query.Set("user_id", payload.FBID.String())
	query.Set("device_id", strconv.Itoa(int(deviceID)))
	body := url.Values{}
	body.Set("__a", "1")
	body.Set("__user", "0")
	body.Set("lsd", tokens.LSD)
	body.Set(tokens.SprinkleParam, tokens.sprinkleValue())
	reqURL := canonicalTokenURL + "?" + query.Encode()
	encodedBody := body.Encode()

	var lastErr error
	for attempt := 0; attempt < 2; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return "", ctx.Err()
			case <-time.After(canonicalRetryDelay):
			}
		}
		status, retryable, err := cli.postCanonicalToken(ctx, hc, reqURL, encodedBody, tokens.LSD)
		if err == nil {
			return status, nil
		}
		lastErr = err
		if !retryable {
			break
		}
	}
	return "", lastErr
}

func (cli *Client) fetchCanonicalPageTokens(ctx context.Context, hc *http.Client) (*canonicalPageTokens, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, canonicalBaseURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", CanonicalUserAgent)
	req.Header.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
	req.Header.Set("Accept-Language", "en-US,en;q=0.9")
	req.Header.Set("Sec-Fetch-Site", "none")
	req.Header.Set("Sec-Fetch-Mode", "navigate")
	req.Header.Set("Sec-Fetch-Dest", "document")
	req.Header.Set("Upgrade-Insecure-Requests", "1")
	resp, err := hc.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("GET / returned HTTP %d", resp.StatusCode)
	}
	html, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return nil, err
	}
	return extractCanonicalPageTokens(string(html))
}

type canonicalTokenResponse struct {
	Payload *struct {
		Status string `json:"status"`
		Error  string `json:"error"`
	} `json:"payload"`
	Status string `json:"status"`
	Error  any    `json:"error"`
}

// postCanonicalToken returns the server's payload.status. A non-"success" status is
// returned as an error (not retryable); network and 5xx errors are retryable.
func (cli *Client) postCanonicalToken(ctx context.Context, hc *http.Client, reqURL, body, lsd string) (string, bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, reqURL, strings.NewReader(body))
	if err != nil {
		return "", false, redactCanonicalURLError(err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("User-Agent", CanonicalUserAgent)
	req.Header.Set("X-FB-LSD", lsd)
	req.Header.Set("Origin", "https://web.whatsapp.com")
	req.Header.Set("Referer", "https://web.whatsapp.com/")
	req.Header.Set("Sec-Fetch-Site", "same-origin")
	req.Header.Set("Sec-Fetch-Mode", "cors")
	req.Header.Set("Sec-Fetch-Dest", "empty")
	resp, err := hc.Do(req)
	if err != nil {
		return "", true, redactCanonicalURLError(err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return "", true, err
	}
	if resp.StatusCode >= 500 {
		return "", true, fmt.Errorf("POST /auth/token/ returned HTTP %d", resp.StatusCode)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", false, fmt.Errorf("POST /auth/token/ returned HTTP %d: %s", resp.StatusCode, truncateForLog(raw))
	}
	var parsed canonicalTokenResponse
	trimmed := bytes.TrimPrefix(bytes.TrimSpace(raw), []byte("for (;;);"))
	if err = json.Unmarshal(trimmed, &parsed); err != nil {
		return "", false, fmt.Errorf("parse /auth/token/ response: %w (body %s)", err, truncateForLog(raw))
	}
	status := parsed.Status
	if parsed.Payload != nil {
		status = parsed.Payload.Status
	}
	if status != "success" {
		return status, false, fmt.Errorf("/auth/token/ status %q (body %s)", status, truncateForLog(raw))
	}
	return status, false, nil
}

// redactCanonicalURLError strips the request URL from a *url.Error before it can reach
// a log. The nonce and access token travel in the /auth/token/ query string, so the URL
// that net/http embeds in its error message would leak them. Other errors pass through.
func redactCanonicalURLError(err error) error {
	var ue *url.Error
	if errors.As(err, &ue) {
		return fmt.Errorf("%s /auth/token/: %w", ue.Op, ue.Err)
	}
	return err
}

// truncateForLog keeps response bodies short in logs. The response does not echo
// the nonce or token in the shapes WhatsApp Web parses, but cap it regardless.
func truncateForLog(b []byte) string {
	const maxLen = 300
	if len(b) > maxLen {
		return string(b[:maxLen]) + "…"
	}
	return string(b)
}

// pairSuccessChildTags lists the child tags of pair-success (no content) so logs show
// whether the server sent encryption-metadata to this client.
func pairSuccessChildTags(pairSuccess waBinary.Node) []string {
	children := pairSuccess.GetChildren()
	tags := make([]string, 0, len(children))
	for _, child := range children {
		tags = append(tags, child.Tag)
	}
	return tags
}
