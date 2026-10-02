// Copyright (c) 2024 Tulir Asokan
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
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"

	"go.mau.fi/util/random"

	"go.mau.fi/whatsmeow/socket"
	"go.mau.fi/whatsmeow/util/cbcutil"
)

// UploadResponse contains the data from the attachment upload, which can be put into a message to send the attachment.
type UploadResponse struct {
	URL        string `json:"url"`
	DirectPath string `json:"direct_path"`
	Handle     string `json:"handle"`
	ObjectID   string `json:"object_id"`

	MediaKey      []byte `json:"-"`
	FileEncSHA256 []byte `json:"-"`
	FileSHA256    []byte `json:"-"`
	FileLength    uint64 `json:"-"`

	// StreamingSidecar is set for MediaVideo uploads. Copy it to VideoMessage.StreamingSidecar
	// so recipients can verify and play the video while it is still downloading.
	StreamingSidecar []byte `json:"-"`
}

// Upload uploads the given attachment to WhatsApp servers.
//
// You should copy the fields in the response to the corresponding fields in a protobuf message.
//
// For example, to send an image:
//
//	resp, err := cli.Upload(context.Background(), yourImageBytes, whatsmeow.MediaImage)
//	// handle error
//
//	imageMsg := &waE2E.ImageMessage{
//		Caption:  proto.String("Hello, world!"),
//		Mimetype: proto.String("image/png"), // replace this with the actual mime type
//		// you can also optionally add other fields like ContextInfo and JpegThumbnail here
//
//		URL:           &resp.URL,
//		DirectPath:    &resp.DirectPath,
//		MediaKey:      resp.MediaKey,
//		FileEncSHA256: resp.FileEncSHA256,
//		FileSHA256:    resp.FileSHA256,
//		FileLength:    &resp.FileLength,
//	}
//	_, err = cli.SendMessage(context.Background(), targetJID, &waE2E.Message{
//		ImageMessage: imageMsg,
//	})
//	// handle error again
//
// The same applies to the other message types like DocumentMessage, just replace the struct type and Message field name.
func (cli *Client) Upload(ctx context.Context, plaintext []byte, appInfo MediaType) (resp UploadResponse, err error) {
	return cli.UploadWithKey(ctx, plaintext, appInfo, random.Bytes(32))
}

// UploadWithKey is otherwise identical to [Upload], but encrypts with the given 32-byte media key
// instead of generating a new one.
//
// This is needed for thumbnails that are uploaded separately from their message's media
// (MediaImageThumbnail, MediaVideoThumbnail): they must use the same MediaKey as the image or
// video, and the response fields map to ThumbnailDirectPath, ThumbnailSHA256 and ThumbnailEncSHA256.
func (cli *Client) UploadWithKey(ctx context.Context, plaintext []byte, appInfo MediaType, mediaKey []byte) (resp UploadResponse, err error) {
	if len(mediaKey) != 32 {
		err = fmt.Errorf("media key must be 32 bytes, got %d", len(mediaKey))
		return
	}
	resp.FileLength = uint64(len(plaintext))
	resp.MediaKey = mediaKey

	plaintextSHA256 := sha256.Sum256(plaintext)
	resp.FileSHA256 = plaintextSHA256[:]

	iv, cipherKey, macKey, _ := getMediaKeys(resp.MediaKey, appInfo)

	var ciphertext []byte
	ciphertext, err = cbcutil.Encrypt(cipherKey, iv, plaintext)
	if err != nil {
		err = fmt.Errorf("failed to encrypt file: %w", err)
		return
	}

	h := hmac.New(sha256.New, macKey)
	h.Write(iv)
	h.Write(ciphertext)
	dataToUpload := append(ciphertext, h.Sum(nil)[:10]...)

	dataHash := sha256.Sum256(dataToUpload)
	resp.FileEncSHA256 = dataHash[:]

	if appInfo == MediaVideo {
		resp.StreamingSidecar, err = calculateStreamingSidecar(macKey, iv, bytes.NewReader(dataToUpload))
		if err != nil {
			err = fmt.Errorf("failed to calculate streaming sidecar: %w", err)
			return
		}
	}

	err = cli.rawUpload(ctx, bytes.NewReader(dataToUpload), uint64(len(dataToUpload)), resp.FileEncSHA256, appInfo, false, &resp)
	return
}

// UploadReader uploads the given attachment to WhatsApp servers.
//
// This is otherwise identical to [Upload], but it reads the plaintext from an [io.Reader] instead of a byte slice.
// A temporary file is required for the encryption process. If tempFile is nil, a temporary file will be created
// and deleted after the upload.
//
// To use only one file, pass the same file as both plaintext and tempFile. This will cause the file to be overwritten with encrypted data.
func (cli *Client) UploadReader(ctx context.Context, plaintext io.Reader, tempFile io.ReadWriteSeeker, appInfo MediaType) (resp UploadResponse, err error) {
	resp.MediaKey = random.Bytes(32)
	iv, cipherKey, macKey, _ := getMediaKeys(resp.MediaKey, appInfo)
	if tempFile == nil {
		tempFile, err = os.CreateTemp("", "whatsmeow-upload-*")
		if err != nil {
			err = fmt.Errorf("failed to create temporary file: %w", err)
			return
		}
		defer func() {
			tempFileFile := tempFile.(*os.File)
			_ = tempFileFile.Close()
			_ = os.Remove(tempFileFile.Name())
		}()
	}
	var uploadSize uint64
	resp.FileSHA256, resp.FileEncSHA256, resp.FileLength, uploadSize, err = cbcutil.EncryptStream(cipherKey, iv, macKey, plaintext, tempFile)
	if err != nil {
		err = fmt.Errorf("failed to encrypt file: %w", err)
		return
	}
	_, err = tempFile.Seek(0, io.SeekStart)
	if err != nil {
		err = fmt.Errorf("failed to seek to start of temporary file: %w", err)
		return
	}
	if appInfo == MediaVideo {
		resp.StreamingSidecar, err = calculateStreamingSidecar(macKey, iv, io.LimitReader(tempFile, int64(uploadSize)))
		if err != nil {
			err = fmt.Errorf("failed to calculate streaming sidecar: %w", err)
			return
		}
		_, err = tempFile.Seek(0, io.SeekStart)
		if err != nil {
			err = fmt.Errorf("failed to seek to start of temporary file: %w", err)
			return
		}
	}
	err = cli.rawUpload(ctx, tempFile, uploadSize, resp.FileEncSHA256, appInfo, false, &resp)
	return
}

const (
	sidecarChunkSize = 64 * 1024
	sidecarMACSize   = 10
)

// calculateStreamingSidecar computes the video streaming sidecar the same way WhatsApp Web does
// (WAMediaCryptoSidecar): over the buffer iv || ciphertext || mac, chunk n is the HMAC-SHA256 of
// bytes [n*64KiB, (n+1)*64KiB+16), truncated to 10 bytes. Consecutive windows overlap by the
// 16-byte IV size, and there are ceil(len(ciphertext || mac) / 64KiB) of them.
//
// encrypted must yield exactly the uploaded bytes (ciphertext followed by the 10-byte mac).
func calculateStreamingSidecar(macKey, iv []byte, encrypted io.Reader) ([]byte, error) {
	window := make([]byte, 0, sidecarChunkSize+len(iv))
	window = append(window, iv...)
	var sidecar []byte
	h := hmac.New(sha256.New, macKey)
	emit := func() {
		h.Reset()
		h.Write(window)
		sidecar = append(sidecar, h.Sum(nil)[:sidecarMACSize]...)
	}
	for {
		n, err := io.ReadFull(encrypted, window[len(window):cap(window)])
		window = window[:len(window)+n]
		if len(window) == cap(window) {
			emit()
			// Keep the trailing IV-sized overlap as the start of the next window.
			window = window[:copy(window, window[sidecarChunkSize:])]
		}
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			break
		} else if err != nil {
			return nil, err
		}
	}
	if len(window) > len(iv) {
		emit()
	}
	return sidecar, nil
}

// UploadNewsletter uploads the given attachment to WhatsApp servers without encrypting it first.
//
// Newsletter media works mostly the same way as normal media, with a few differences:
// * Since it's unencrypted, there's no MediaKey or FileEncSHA256 fields.
// * There's a "media handle" that needs to be passed in SendRequestExtra.
//
// Example:
//
//	resp, err := cli.UploadNewsletter(context.Background(), yourImageBytes, whatsmeow.MediaImage)
//	// handle error
//
//	imageMsg := &waE2E.ImageMessage{
//		// Caption, mime type and other such fields work like normal
//		Caption:  proto.String("Hello, world!"),
//		Mimetype: proto.String("image/png"),
//
//		// URL and direct path are also there like normal media
//		URL:        &resp.URL,
//		DirectPath: &resp.DirectPath,
//		FileSHA256: resp.FileSHA256,
//		FileLength: &resp.FileLength,
//		// Newsletter media isn't encrypted, so the media key and file enc sha fields are not applicable
//	}
//	_, err = cli.SendMessage(context.Background(), newsletterJID, &waE2E.Message{
//		ImageMessage: imageMsg,
//	}, whatsmeow.SendRequestExtra{
//		// Unlike normal media, newsletters also include a "media handle" in the send request.
//		MediaHandle: resp.Handle,
//	})
//	// handle error again
func (cli *Client) UploadNewsletter(ctx context.Context, data []byte, appInfo MediaType) (resp UploadResponse, err error) {
	resp.FileLength = uint64(len(data))
	hash := sha256.Sum256(data)
	resp.FileSHA256 = hash[:]
	err = cli.rawUpload(ctx, bytes.NewReader(data), resp.FileLength, resp.FileSHA256, appInfo, true, &resp)
	return
}

// UploadNewsletterReader uploads the given attachment to WhatsApp servers without encrypting it first.
//
// This is otherwise identical to [UploadNewsletter], but it reads the plaintext from an [io.Reader] instead of a byte slice.
// Unlike [UploadReader], this does not require a temporary file. However, the data needs to be hashed first,
// so an [io.ReadSeeker] is required to be able to read the data twice.
func (cli *Client) UploadNewsletterReader(ctx context.Context, data io.ReadSeeker, appInfo MediaType) (resp UploadResponse, err error) {
	hasher := sha256.New()
	var fileLength int64
	fileLength, err = io.Copy(hasher, data)
	if err != nil {
		err = fmt.Errorf("failed to hash data: %w", err)
		return
	}
	resp.FileLength = uint64(fileLength)
	resp.FileSHA256 = hasher.Sum(nil)
	_, err = data.Seek(0, io.SeekStart)
	if err != nil {
		err = fmt.Errorf("failed to seek to start of data: %w", err)
		return
	}
	err = cli.rawUpload(ctx, data, resp.FileLength, resp.FileSHA256, appInfo, true, &resp)
	return
}

func (cli *Client) rawUpload(ctx context.Context, dataToUpload io.Reader, uploadSize uint64, fileHash []byte, appInfo MediaType, newsletter bool, resp *UploadResponse) error {
	mediaConn, err := cli.refreshMediaConn(ctx, false)
	if err != nil {
		return fmt.Errorf("failed to refresh media connections: %w", err)
	}

	token := base64.URLEncoding.EncodeToString(fileHash)
	q := url.Values{
		"auth":  []string{mediaConn.Auth},
		"token": []string{token},
	}
	mmsType := mediaTypeToMMSType[appInfo]
	uploadPrefix := "mms"
	if cli.MessengerConfig != nil {
		uploadPrefix = "wa-msgr/mms"
		// Messenger upload only allows voice messages, not audio files
		if mmsType == "audio" {
			mmsType = "ptt"
		}
	}
	if newsletter {
		mmsType = fmt.Sprintf("newsletter-%s", mmsType)
		uploadPrefix = "newsletter"
	}
	var host string
	// Hacky hack to prefer last option (rupload.facebook.com) for messenger uploads.
	// For some reason, the primary host doesn't work, even though it has the <upload/> tag.
	if cli.MessengerConfig != nil {
		host = mediaConn.Hosts[len(mediaConn.Hosts)-1].Hostname
	} else {
		host = mediaConn.Hosts[0].Hostname
	}
	uploadURL := url.URL{
		Scheme:   "https",
		Host:     host,
		Path:     fmt.Sprintf("/%s/%s/%s", uploadPrefix, mmsType, token),
		RawQuery: q.Encode(),
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, uploadURL.String(), dataToUpload)
	if err != nil {
		return fmt.Errorf("failed to prepare request: %w", err)
	}

	req.ContentLength = int64(uploadSize)
	req.Header.Set("Origin", socket.Origin)
	req.Header.Set("Referer", socket.Origin+"/")

	httpResp, err := cli.mediaHTTP.Do(req)
	if err != nil {
		err = fmt.Errorf("failed to execute request: %w", err)
	} else if httpResp.StatusCode != http.StatusOK {
		err = fmt.Errorf("upload failed with status code %d", httpResp.StatusCode)
	} else if err = json.NewDecoder(httpResp.Body).Decode(&resp); err != nil {
		err = fmt.Errorf("failed to parse upload response: %w", err)
	}
	if httpResp != nil {
		_ = httpResp.Body.Close()
	}
	return err
}

// DeleteMedia deletes the media at the given direct path from WhatsApp servers.
//
// This is only used for things like history syncs, which should be deleted after processing.
func (cli *Client) DeleteMedia(ctx context.Context, appInfo MediaType, directPath string, encFileHash []byte, encHandle string) error {
	if directPath == "" {
		return nil
	}
	mediaConn, err := cli.refreshMediaConn(ctx, false)
	if err != nil {
		return fmt.Errorf("failed to refresh media connections: %w", err)
	}

	queryStart := strings.IndexByte(directPath, '?')
	if queryStart > 0 {
		directPath = directPath[:queryStart]
	}

	token := base64.URLEncoding.EncodeToString(encFileHash)
	query := url.Values{
		"token": []string{token},
		"d_md":  []string{base64.RawURLEncoding.EncodeToString([]byte(directPath))},
		"auth":  []string{mediaConn.Auth},
	}
	if encHandle != "" {
		query.Set("e_handle", encHandle)
	}
	deleteURL := url.URL{
		Scheme:   "https",
		Host:     mediaConn.Hosts[0].Hostname,
		Path:     fmt.Sprintf("/mms/%s/%s", mediaTypeToMMSType[appInfo], token),
		RawQuery: query.Encode(),
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, deleteURL.String(), nil)
	if err != nil {
		return fmt.Errorf("failed to prepare request: %w", err)
	}

	req.Header.Set("Origin", socket.Origin)
	req.Header.Set("Referer", socket.Origin+"/")
	if cmn := cli.Store.CompanionMetaNonce; cmn != "" && encHandle != "" {
		req.Header.Set("Companion_User_Secret", cli.Store.CompanionMetaNonce)
	}

	httpResp, err := cli.mediaHTTP.Do(req)
	if err != nil {
		err = fmt.Errorf("failed to execute request: %w", err)
	} else if httpResp.StatusCode < 200 || httpResp.StatusCode >= 300 {
		err = fmt.Errorf("media delete failed with status code %d", httpResp.StatusCode)
	}
	if httpResp != nil {
		_ = httpResp.Body.Close()
	}
	return err
}
