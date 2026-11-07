// Copyright (c) 2026 Tulir Asokan
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package whatsmeow

import (
	"context"
	"strings"

	"go.mau.fi/whatsmeow/proto/waHistorySync"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types"
)

// parseUserLID parses a LID given either as a bare user ("123") or as a JID
// ("123@lid", "123:4@lid"). WA Web's createUserLidOrThrow accepts both;
// types.ParseJID turns a bare user into {Server: "123"}, which
// PutManyLIDMappings then discards as invalid without a trace.
func parseUserLID(s string) (types.JID, bool) {
	if s == "" {
		return types.JID{}, false
	}
	if !strings.ContainsRune(s, '@') {
		return types.NewJID(s, types.HiddenUserServer), true
	}
	j, err := types.ParseJID(s)
	if err != nil || j.Server != types.HiddenUserServer || j.User == "" {
		return types.JID{}, false
	}
	return j.ToNonAD(), true
}

// parseUserPN parses a phone-number user given bare ("234…"), as
// "234…@s.whatsapp.net" or as the legacy "234…@c.us".
func parseUserPN(s string) (types.JID, bool) {
	if s == "" {
		return types.JID{}, false
	}
	if !strings.ContainsRune(s, '@') {
		return types.NewJID(s, types.DefaultUserServer), true
	}
	j, err := types.ParseJID(s)
	if err != nil || j.User == "" {
		return types.JID{}, false
	}
	if j.Server == types.LegacyUserServer {
		j.Server = types.DefaultUserServer
	}
	if j.Server != types.DefaultUserServer {
		return types.JID{}, false
	}
	return j.ToNonAD(), true
}

// historyLIDMappings collects every LID↔PN pair a history-sync chunk carries,
// the way WA Web does:
//   - the top-level phoneNumberToLidMappings list (WAWebHandleHistorySyncChunk,
//     WAWebHistorySyncBackendWorkerV2);
//   - per conversation: a LID chat's pnJid, or a PN chat's lidJid
//     (WAWebHistoryMsgHandlerAction, "LIDxPN mappings obtained from
//     conversations"). WA Web reads these on the initial bootstrap; the
//     fields mean the same thing in every chunk, so we read them on all.
func historyLIDMappings(hs *waHistorySync.HistorySync) (pairs []store.LIDMapping, top, fromConvs, invalid int) {
	for _, m := range hs.GetPhoneNumberToLidMappings() {
		lid, okL := parseUserLID(m.GetLidJID())
		pn, okP := parseUserPN(m.GetPnJID())
		if !okL || !okP {
			invalid++
			continue
		}
		pairs = append(pairs, store.LIDMapping{LID: lid, PN: pn})
		top++
	}
	for _, conv := range hs.GetConversations() {
		chat, err := types.ParseJID(conv.GetID())
		if err != nil {
			continue
		}
		switch chat.Server {
		case types.HiddenUserServer:
			if pn, ok := parseUserPN(conv.GetPnJID()); ok {
				pairs = append(pairs, store.LIDMapping{LID: chat.ToNonAD(), PN: pn})
				fromConvs++
			}
		case types.DefaultUserServer, types.LegacyUserServer:
			if lid, ok := parseUserLID(conv.GetLidJID()); ok {
				pn, _ := parseUserPN(conv.GetID())
				pairs = append(pairs, store.LIDMapping{LID: lid, PN: pn})
				fromConvs++
			}
		}
	}
	return
}

// storeHistorySyncLIDMappings stores every mapping in a history-sync chunk
// and logs the counts through cli.Log, so a caller's logger sees them (the
// old path logged through zerolog.Ctx, which most callers never set).
func (cli *Client) storeHistorySyncLIDMappings(ctx context.Context, hs *waHistorySync.HistorySync) {
	pairs, top, fromConvs, invalid := historyLIDMappings(hs)
	if len(pairs) == 0 && invalid == 0 {
		return
	}
	if err := cli.Store.LIDs.PutManyLIDMappings(ctx, pairs); err != nil {
		cli.Log.Warnf("Failed to store %d LID mappings from history sync (%s chunk %d): %v",
			len(pairs), hs.GetSyncType(), hs.GetChunkOrder(), err)
		return
	}
	cli.Log.Debugf("Stored LID mappings from history sync (%s chunk %d): %d top-level, %d from conversations, %d invalid",
		hs.GetSyncType(), hs.GetChunkOrder(), top, fromConvs, invalid)
}
