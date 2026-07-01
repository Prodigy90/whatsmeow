// Copyright (c) 2021 Tulir Asokan
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package whatsmeow

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"runtime/debug"
	"slices"
	"strings"

	"google.golang.org/protobuf/proto"

	waBinary "go.mau.fi/whatsmeow/binary"
	"go.mau.fi/whatsmeow/proto/waHistorySync"
	"go.mau.fi/whatsmeow/proto/waVnameCert"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
)

const (
	BusinessMessageLinkPrefix       = "https://wa.me/message/"
	ContactQRLinkPrefix             = "https://wa.me/qr/"
	BusinessMessageLinkDirectPrefix = "https://api.whatsapp.com/message/"
	ContactQRLinkDirectPrefix       = "https://api.whatsapp.com/qr/"
	NewsletterLinkPrefix            = "https://whatsapp.com/channel/"
)

func stripQuery(link string) string {
	if idx := strings.Index(link, "?"); idx > 0 {
		return link[:idx]
	}
	return link
}

func stripURLPrefix(link string, prefixes ...string) string {
	for _, prefix := range prefixes {
		unprefixed, ok := strings.CutPrefix(link, prefix)
		if ok {
			return stripQuery(unprefixed)
		}
	}
	return link
}

// ResolveBusinessMessageLink resolves a business message short link and returns the target JID, business name and
// text to prefill in the input field (if any).
//
// The links look like https://wa.me/message/<code> or https://api.whatsapp.com/message/<code>. You can either provide
// the full link, or just the <code> part.
func (cli *Client) ResolveBusinessMessageLink(ctx context.Context, code string) (*types.BusinessMessageLinkTarget, error) {
	resp, err := cli.sendIQ(ctx, infoQuery{
		Namespace: "w:qr",
		Type:      iqGet,
		// WhatsApp android doesn't seem to have a "to" field for this one at all, not sure why but it works
		Content: []waBinary.Node{{
			Tag: "qr",
			Attrs: waBinary.Attrs{
				"code": stripURLPrefix(code, BusinessMessageLinkPrefix, BusinessMessageLinkDirectPrefix),
			},
		}},
	})
	if errors.Is(err, ErrIQNotFound) {
		return nil, wrapIQError(ErrBusinessMessageLinkNotFound, err)
	} else if err != nil {
		return nil, err
	}
	qrChild, ok := resp.GetOptionalChildByTag("qr")
	if !ok {
		return nil, &ElementMissingError{Tag: "qr", In: "response to business message link query"}
	}
	var target types.BusinessMessageLinkTarget
	ag := qrChild.AttrGetter()
	target.JID = ag.JID("jid")
	target.PushName = ag.String("notify")
	messageChild, ok := qrChild.GetOptionalChildByTag("message")
	if ok {
		messageBytes, _ := messageChild.Content.([]byte)
		target.Message = string(messageBytes)
	}
	businessChild, ok := qrChild.GetOptionalChildByTag("business")
	if ok {
		bag := businessChild.AttrGetter()
		target.IsSigned = bag.OptionalBool("is_signed")
		target.VerifiedName = bag.OptionalString("verified_name")
		target.VerifiedLevel = bag.OptionalString("verified_level")
	}
	return &target, ag.Error()
}

// ResolveContactQRLink resolves a link from a contact share QR code and returns the target JID and push name.
//
// The links look like https://wa.me/qr/<code> or https://api.whatsapp.com/qr/<code>. You can either provide
// the full link, or just the <code> part.
func (cli *Client) ResolveContactQRLink(ctx context.Context, code string) (*types.ContactQRLinkTarget, error) {
	resp, err := cli.sendIQ(ctx, infoQuery{
		Namespace: "w:qr",
		Type:      iqGet,
		Content: []waBinary.Node{{
			Tag: "qr",
			Attrs: waBinary.Attrs{
				"code": stripURLPrefix(code, ContactQRLinkPrefix, ContactQRLinkDirectPrefix),
			},
		}},
	})
	if errors.Is(err, ErrIQNotFound) {
		return nil, wrapIQError(ErrContactQRLinkNotFound, err)
	} else if err != nil {
		return nil, err
	}
	qrChild, ok := resp.GetOptionalChildByTag("qr")
	if !ok {
		return nil, &ElementMissingError{Tag: "qr", In: "response to contact link query"}
	}
	var target types.ContactQRLinkTarget
	ag := qrChild.AttrGetter()
	target.JID = ag.JID("jid")
	target.PushName = ag.OptionalString("notify")
	target.Type = ag.String("type")
	return &target, ag.Error()
}

// GetContactQRLink gets your own contact share QR link that can be resolved using ResolveContactQRLink
// (or scanned with the official apps when encoded as a QR code).
//
// If the revoke parameter is set to true, it will ask the server to revoke the previous link and generate a new one.
func (cli *Client) GetContactQRLink(ctx context.Context, revoke bool) (string, error) {
	action := "get"
	if revoke {
		action = "revoke"
	}
	resp, err := cli.sendIQ(ctx, infoQuery{
		Namespace: "w:qr",
		Type:      iqSet,
		Content: []waBinary.Node{{
			Tag: "qr",
			Attrs: waBinary.Attrs{
				"type":   "contact",
				"action": action,
			},
		}},
	})
	if err != nil {
		return "", err
	}
	qrChild, ok := resp.GetOptionalChildByTag("qr")
	if !ok {
		return "", &ElementMissingError{Tag: "qr", In: "response to own contact link fetch"}
	}
	ag := qrChild.AttrGetter()
	return ag.String("code"), ag.Error()
}

// SetStatusMessage updates the current user's status text, which is shown in the "About" section in the user profile.
//
// This is different from the ephemeral status broadcast messages. Use SendMessage to types.StatusBroadcastJID to send
// such messages.
func (cli *Client) SetStatusMessage(ctx context.Context, msg string) error {
	_, err := cli.sendIQ(ctx, infoQuery{
		Namespace: "status",
		Type:      iqSet,
		To:        types.ServerJID,
		Content: []waBinary.Node{{
			Tag:     "status",
			Content: msg,
		}},
	})
	return err
}

// IsOnWhatsApp checks if the given phone numbers are registered on WhatsApp.
// The phone numbers should be in international format, including the `+` prefix.
func (cli *Client) IsOnWhatsApp(ctx context.Context, phones []string) ([]types.IsOnWhatsAppResponse, error) {
	jids := make([]types.JID, len(phones))
	for i := range jids {
		jids[i] = types.NewJID(phones[i], types.LegacyUserServer)
	}
	list, err := cli.usync(ctx, jids, "query", "interactive", []waBinary.Node{
		{Tag: "business", Content: []waBinary.Node{{Tag: "verified_name"}}},
		{Tag: "contact"},
	})
	if err != nil {
		return nil, err
	}
	output := make([]types.IsOnWhatsAppResponse, 0, len(jids))
	querySuffix := "@" + types.LegacyUserServer
	for _, child := range list.GetChildren() {
		jid, jidOK := child.Attrs["jid"].(types.JID)
		if child.Tag != "user" || !jidOK {
			continue
		}
		var info types.IsOnWhatsAppResponse
		info.JID = jid
		info.VerifiedName, err = parseVerifiedName(child.GetChildByTag("business"))
		if err != nil {
			cli.Log.Warnf("Failed to parse %s's verified name details: %v", jid, err)
		}
		contactNode := child.GetChildByTag("contact")
		info.IsIn = contactNode.AttrGetter().String("type") == "in"
		contactQuery, _ := contactNode.Content.([]byte)
		info.Query = strings.TrimSuffix(string(contactQuery), querySuffix)
		output = append(output, info)
	}
	return output, nil
}

// GetUserInfo gets basic user info (avatar, status, verified business name, device list).
func (cli *Client) GetUserInfo(ctx context.Context, jids []types.JID) (map[types.JID]types.UserInfo, error) {
	list, err := cli.usync(ctx, jids, "full", "background", []waBinary.Node{
		{Tag: "business", Content: []waBinary.Node{{Tag: "verified_name"}}},
		{Tag: "status"},
		{Tag: "picture"},
		{Tag: "devices", Attrs: waBinary.Attrs{"version": "2"}},
		{Tag: "lid"},
	}, UsyncQueryExtras{
		IncludePrivacyToken: true,
	})
	if err != nil {
		return nil, err
	}
	respData := make(map[types.JID]types.UserInfo, len(jids))
	mappings := make([]store.LIDMapping, 0, len(jids))
	for _, child := range list.GetChildren() {
		jid, jidOK := child.Attrs["jid"].(types.JID)
		if child.Tag != "user" || !jidOK {
			continue
		}
		var info types.UserInfo
		verifiedName, err := parseVerifiedName(child.GetChildByTag("business"))
		if err != nil {
			cli.Log.Warnf("Failed to parse %s's verified name details: %v", jid, err)
		}
		status, _ := child.GetChildByTag("status").Content.([]byte)
		info.Status = string(status)
		info.PictureID, _ = child.GetChildByTag("picture").Attrs["id"].(string)
		info.Devices = parseDeviceList(jid, child.GetChildByTag("devices"))

		lidTag := child.GetChildByTag("lid")
		info.LID = lidTag.AttrGetter().OptionalJIDOrEmpty("val")

		if !info.LID.IsEmpty() {
			mappings = append(mappings, store.LIDMapping{PN: jid, LID: info.LID})
		}

		if verifiedName != nil {
			cli.updateBusinessName(ctx, jid, info.LID, nil, verifiedName.Details.GetVerifiedName())
		}
		respData[jid] = info
	}

	err = cli.Store.LIDs.PutManyLIDMappings(ctx, mappings)
	if err != nil {
		// not worth returning on the error, instead just post a log
		cli.Log.Errorf("Failed to place LID mappings from USync call")
	}

	return respData, nil
}

func (cli *Client) GetBotListV2(ctx context.Context) ([]types.BotListInfo, error) {
	resp, err := cli.sendIQ(ctx, infoQuery{
		To:        types.ServerJID,
		Namespace: "bot",
		Type:      iqGet,
		Content: []waBinary.Node{
			{Tag: "bot", Attrs: waBinary.Attrs{"v": "2"}},
		},
	})
	if err != nil {
		return nil, err
	}
	botNode, ok := resp.GetOptionalChildByTag("bot")
	if !ok {
		return nil, &ElementMissingError{Tag: "bot", In: "response to bot list query"}
	}

	var list []types.BotListInfo

	for _, section := range botNode.GetChildrenByTag("section") {
		if section.AttrGetter().String("type") == "all" {
			for _, bot := range section.GetChildrenByTag("bot") {
				ag := bot.AttrGetter()
				list = append(list, types.BotListInfo{
					PersonaID: ag.String("persona_id"),
					BotJID:    ag.JID("jid"),
				})
			}
		}
	}

	return list, nil
}

func (cli *Client) GetBotProfiles(ctx context.Context, botInfo []types.BotListInfo) ([]types.BotProfileInfo, error) {
	jids := make([]types.JID, len(botInfo))
	for i, bot := range botInfo {
		jids[i] = bot.BotJID
	}

	list, err := cli.usync(ctx, jids, "query", "interactive", []waBinary.Node{
		{Tag: "bot", Content: []waBinary.Node{{Tag: "profile", Attrs: waBinary.Attrs{"v": "1"}}}},
	}, UsyncQueryExtras{
		BotListInfo: botInfo,
	})

	if err != nil {
		return nil, err
	}

	var profiles []types.BotProfileInfo
	for _, user := range list.GetChildren() {
		jid := user.AttrGetter().JID("jid")
		bot := user.GetChildByTag("bot")
		profile := bot.GetChildByTag("profile")
		name := string(profile.GetChildByTag("name").Content.([]byte))
		attributes := string(profile.GetChildByTag("attributes").Content.([]byte))
		description := string(profile.GetChildByTag("description").Content.([]byte))
		category := string(profile.GetChildByTag("category").Content.([]byte))
		_, isDefault := profile.GetOptionalChildByTag("default")
		personaID := profile.AttrGetter().String("persona_id")
		commandsNode := profile.GetChildByTag("commands")
		commandDescription := string(commandsNode.GetChildByTag("description").Content.([]byte))
		var commands []types.BotProfileCommand
		for _, commandNode := range commandsNode.GetChildrenByTag("command") {
			commands = append(commands, types.BotProfileCommand{
				Name:        string(commandNode.GetChildByTag("name").Content.([]byte)),
				Description: string(commandNode.GetChildByTag("description").Content.([]byte)),
			})
		}

		promptsNode := profile.GetChildByTag("prompts")
		var prompts []string
		for _, promptNode := range promptsNode.GetChildrenByTag("prompt") {
			prompts = append(
				prompts,
				fmt.Sprintf(
					"%s %s",
					string(promptNode.GetChildByTag("emoji").Content.([]byte)),
					string(promptNode.GetChildByTag("text").Content.([]byte)),
				),
			)
		}

		profiles = append(profiles, types.BotProfileInfo{
			JID:                 jid,
			Name:                name,
			Attributes:          attributes,
			Description:         description,
			Category:            category,
			IsDefault:           isDefault,
			Prompts:             prompts,
			PersonaID:           personaID,
			Commands:            commands,
			CommandsDescription: commandDescription,
		})
	}

	return profiles, nil
}

func (cli *Client) parseBusinessProfile(node *waBinary.Node) (*types.BusinessProfile, error) {
	profileNode := node.GetChildByTag("profile")
	jid, ok := profileNode.AttrGetter().GetJID("jid", true)
	if !ok {
		return nil, errors.New("missing jid in business profile")
	}
	address, _ := profileNode.GetChildByTag("address").Content.([]byte)
	email, _ := profileNode.GetChildByTag("email").Content.([]byte)
	businessHour := profileNode.GetChildByTag("business_hours")
	businessHourTimezone := businessHour.AttrGetter().String("timezone")
	businessHoursConfigs := businessHour.GetChildren()
	businessHours := make([]types.BusinessHoursConfig, 0)
	for _, config := range businessHoursConfigs {
		if config.Tag != "business_hours_config" {
			continue
		}
		dow := config.AttrGetter().String("day_of_week")
		mode := config.AttrGetter().String("mode")
		openTime := config.AttrGetter().String("open_time")
		closeTime := config.AttrGetter().String("close_time")
		businessHours = append(businessHours, types.BusinessHoursConfig{
			DayOfWeek: dow,
			Mode:      mode,
			OpenTime:  openTime,
			CloseTime: closeTime,
		})
	}
	categoriesNode := profileNode.GetChildByTag("categories")
	categories := make([]types.Category, 0)
	for _, category := range categoriesNode.GetChildren() {
		if category.Tag != "category" {
			continue
		}
		id := category.AttrGetter().String("id")
		name, _ := category.Content.([]byte)
		categories = append(categories, types.Category{
			ID:   id,
			Name: string(name),
		})
	}
	profileOptionsNode := profileNode.GetChildByTag("profile_options")
	profileOptions := make(map[string]string)
	for _, option := range profileOptionsNode.GetChildren() {
		optValueBytes, _ := option.Content.([]byte)
		profileOptions[option.Tag] = string(optValueBytes)
		// TODO parse bot_fields
	}
	return &types.BusinessProfile{
		JID:                   jid,
		Email:                 string(email),
		Address:               string(address),
		Categories:            categories,
		ProfileOptions:        profileOptions,
		BusinessHoursTimeZone: businessHourTimezone,
		BusinessHours:         businessHours,
	}, nil
}

// GetBusinessProfile gets the profile info of a WhatsApp business account
func (cli *Client) GetBusinessProfile(ctx context.Context, jid types.JID) (*types.BusinessProfile, error) {
	resp, err := cli.sendIQ(ctx, infoQuery{
		Type:      iqGet,
		To:        types.ServerJID,
		Namespace: "w:biz",
		Content: []waBinary.Node{{
			Tag: "business_profile",
			Attrs: waBinary.Attrs{
				"v": "244",
			},
			Content: []waBinary.Node{{
				Tag: "profile",
				Attrs: waBinary.Attrs{
					"jid": jid,
				},
			}},
		}},
	})
	if err != nil {
		return nil, err
	}
	node, ok := resp.GetOptionalChildByTag("business_profile")
	if !ok {
		return nil, &ElementMissingError{Tag: "business_profile", In: "response to business profile query"}
	}
	return cli.parseBusinessProfile(&node)
}

func (cli *Client) GetUserDevicesContext(ctx context.Context, jids []types.JID) ([]types.JID, error) {
	return cli.GetUserDevices(ctx, jids)
}

// maxUsyncUsersPerQuery caps the number of <user> nodes in a single usync IQ.
// Native WhatsApp Web never exceeds 500 <user> nodes per usync query
// (empirically captured); a single oversized usync is held by the server ~8-11s
// and then drops the socket, immediately followed by a 403 account lock. Larger
// inputs are split into sequential ≤500-user queries. Mirrors the chunking
// getFBIDDevices already does for Messenger JIDs.
const maxUsyncUsersPerQuery = 500

// usyncDeviceChunks splits a JID list into consecutive chunks no larger than
// maxUsyncUsersPerQuery, preserving order. Returns nil for an empty input.
func usyncDeviceChunks(jids []types.JID) [][]types.JID {
	if len(jids) == 0 {
		return nil
	}
	chunks := make([][]types.JID, 0, (len(jids)+maxUsyncUsersPerQuery-1)/maxUsyncUsersPerQuery)
	for chunk := range slices.Chunk(jids, maxUsyncUsersPerQuery) {
		chunks = append(chunks, chunk)
	}
	return chunks
}

// GetUserDevices gets the list of devices that the given user has. The input should be a list of
// regular JIDs, and the output will be a list of AD JIDs. The local device will not be included in
// the output even if the user's JID is included in the input. All other devices will be included.
func (cli *Client) GetUserDevices(ctx context.Context, jids []types.JID) ([]types.JID, error) {
	return cli.getUserDevices(ctx, jids, "message")
}

// getUserDevices is GetUserDevices with a parameterized usync `context` attr: sends
// use "message"; the streaming pre-warmer passes "background" to mirror native WA
// Web's roster-warm context.
func (cli *Client) getUserDevices(ctx context.Context, jids []types.JID, usyncContext string) ([]types.JID, error) {
	devices, _, err := cli.getUserDevicesReportingSync(ctx, jids, usyncContext)
	return devices, err
}

// getUserDevicesReportingSync is getUserDevices plus a flag reporting whether the
// call issued a usync (or FB device) IQ over the wire, versus resolving every input
// JID from the in-memory / persistent device cache. The streaming pre-warmer uses
// this to skip its inter-chunk pacing delay for cache-only chunks: that delay exists
// solely to space out wire usync bursts (the 403 precursor), so a chunk that emits
// no IQ needs no pacing. Without this an all-warm pre-send pass slept ~6s per ≤500
// contacts (e.g. 96s for an 8.3k-contact roster) while warming nothing.
func (cli *Client) getUserDevicesReportingSync(ctx context.Context, jids []types.JID, usyncContext string) (resolved []types.JID, hitWire bool, err error) {
	if cli == nil {
		return nil, false, ErrClientIsNil
	}
	cli.userDevicesCacheLock.Lock()
	defer cli.userDevicesCacheLock.Unlock()

	var devices, jidsToSync, fbJIDsToSync []types.JID
	for _, jid := range jids {
		cached, ok := cli.userDevicesCache[jid]
		if ok && len(cached.devices) > 0 {
			devices = append(devices, cached.devices...)
		} else if jid.Server == types.MessengerServer {
			fbJIDsToSync = append(fbJIDsToSync, jid)
		} else if jid.IsBot() {
			// Bot JIDs do not have devices, the usync query is empty
			devices = append(devices, jid)
		} else {
			jidsToSync = append(jidsToSync, jid)
		}
	}

	// Read-on-miss: resolve cache-miss JIDs from the persistent device-list store
	// (which survives restarts) before falling back to a cold usync. Runs under the
	// cache lock — consistent with the PutManyLIDMappings DB write below, and far
	// cheaper than the usync it avoids. No-op when no store is wired (in-memory only).
	jidsToSync = cli.loadDeviceListsFromStoreLocked(ctx, jidsToSync, &devices)

	if len(jidsToSync) > 0 {
		hitWire = true
		jidsWithDevices := make(map[types.JID]bool, len(jidsToSync))

		// Harvest LID mappings inline with the device query. WA returns `<lid val="...@lid">`
		// children on `<user>` nodes when a PN has a LID partner — same shape GetUserInfo
		// parses at the IQ-response level. Capturing here closes the gap where send-path
		// GetUserDevices calls would otherwise pass through this mapping data without
		// persisting it, leaving Store.LIDs cold for LIDs we just routed a message to.
		// Baileys does the equivalent in messages-send.ts (storeLIDPNMappings filter on a.lid).
		lidMappings := make([]store.LIDMapping, 0)

		// usync() caps each IQ at ≤maxUsyncUsersPerQuery and merges the chunked
		// response, so jidsToSync may be any size here.
		list, syncErr := cli.usync(ctx, jidsToSync, "query", usyncContext, []waBinary.Node{
			{Tag: "devices", Attrs: waBinary.Attrs{"version": "2"}},
		})
		if syncErr != nil {
			return nil, hitWire, syncErr
		}
		persistEntries := make([]store.DeviceListEntry, 0, len(jidsToSync))
		cli.foldUsyncDevices(list, &devices, jidsWithDevices, &lidMappings, &persistEntries)

		if len(lidMappings) > 0 {
			if err := cli.Store.LIDs.PutManyLIDMappings(ctx, lidMappings); err != nil {
				cli.Log.Errorf("Failed to store LID mappings from GetUserDevices usync: %v", err)
			} else {
				cli.Log.Infof("GetUserDevices: harvested %d LID mappings from %d input JIDs", len(lidMappings), len(jidsToSync))
			}
		}

		// Write-through the freshly resolved device lists to the persistent store
		// (best-effort; under the cache lock, like the LID write above).
		cli.persistDeviceLists(ctx, persistEntries)

		// Fire callback for JIDs that returned 0 devices (not on WhatsApp)
		if cli.OnNoDeviceContacts != nil {
			var noDeviceJIDs []types.JID
			for _, jid := range jidsToSync {
				if !jidsWithDevices[jid.ToNonAD()] {
					noDeviceJIDs = append(noDeviceJIDs, jid)
				}
			}
			if len(noDeviceJIDs) > 0 {
				go cli.tryOnNoDeviceContacts(ctx, noDeviceJIDs)
			}
		}
	}

	if len(fbJIDsToSync) > 0 {
		hitWire = true
		userDevices, fbErr := cli.getFBIDDevices(ctx, fbJIDsToSync)
		if fbErr != nil {
			return nil, hitWire, fbErr
		}
		devices = append(devices, userDevices...)
	}

	return devices, hitWire, nil
}

// foldUsyncDevices parses one usync device-query response <list> into the device
// cache and the given accumulators (devices resolved, JIDs that returned ≥1
// device, harvested PN→LID mappings). The caller MUST hold userDevicesCacheLock.
func (cli *Client) foldUsyncDevices(list *waBinary.Node, devices *[]types.JID, jidsWithDevices map[types.JID]bool, lidMappings *[]store.LIDMapping, persist *[]store.DeviceListEntry) {
	for _, user := range list.GetChildren() {
		jid, jidOK := user.Attrs["jid"].(types.JID)
		if user.Tag != "user" || !jidOK {
			continue
		}
		userDevices := parseDeviceList(jid, user.GetChildByTag("devices"))
		dhash := participantListHashV2(userDevices)
		cli.userDevicesCache[jid] = deviceCache{devices: userDevices, dhash: dhash}
		*devices = append(*devices, userDevices...)
		if len(userDevices) > 0 {
			jidsWithDevices[jid.ToNonAD()] = true
			// Collect for write-through to the optional persistent device cache.
			// Skip 0-device results — the read path treats them as a miss anyway.
			if persist != nil {
				*persist = append(*persist, store.DeviceListEntry{TheirJID: jid.ToNonAD(), Devices: userDevices, DHash: dhash})
			}
		}
		lidTag := user.GetChildByTag("lid")
		if lid := lidTag.AttrGetter().OptionalJIDOrEmpty("val"); !lid.IsEmpty() {
			*lidMappings = append(*lidMappings, store.LIDMapping{PN: jid, LID: lid})
		}
	}
}

// persistDeviceLists write-throughs resolved device-list entries to the optional
// DeviceListStore. Best-effort cache maintenance: nil-safe, only logs on error,
// never alters the caller's control flow. Does NOT touch userDevicesCache, so it
// is safe to call with or without userDevicesCacheLock held.
func (cli *Client) persistDeviceLists(ctx context.Context, entries []store.DeviceListEntry) {
	if len(entries) == 0 || cli.Store == nil || cli.Store.DeviceLists == nil {
		return
	}
	ourJID := cli.getOwnID()
	if ourJID.IsEmpty() {
		return
	}
	if err := cli.Store.DeviceLists.PutManyDeviceLists(ctx, ourJID.ToNonAD(), entries); err != nil {
		cli.Log.Warnf("Failed to persist %d device-list cache entries: %v", len(entries), err)
	} else {
		cli.Log.Infof("device_cache_write: persisted %d device-list entries to store", len(entries))
	}
}

// forgetDeviceList drops one contact's persisted device list when the in-memory
// cache is invalidated (device-list-changed notification, send-time phash
// mismatch) so a stale row can't outlive the eviction. Best-effort + nil-safe;
// does NOT touch userDevicesCache.
func (cli *Client) forgetDeviceList(ctx context.Context, theirJID types.JID) {
	if cli.Store == nil || cli.Store.DeviceLists == nil {
		return
	}
	ourJID := cli.getOwnID()
	if ourJID.IsEmpty() {
		return
	}
	if err := cli.Store.DeviceLists.DeleteDeviceList(ctx, ourJID.ToNonAD(), theirJID.ToNonAD()); err != nil {
		cli.Log.Warnf("Failed to delete device-list cache for %s: %v", theirJID, err)
	} else {
		cli.Log.Infof("device_cache_invalidate: dropped persisted device list for %s", theirJID)
	}
}

// loadDeviceListsFromStoreLocked resolves cache-miss JIDs from the optional
// persistent store, folding hits into the in-memory cache and the devices
// accumulator, and returns the JIDs that are STILL unresolved (and need a usync).
// The CALLER MUST HOLD userDevicesCacheLock. Nil-safe: returns jidsToSync
// unchanged when no store is wired or on any store error — a send/lookup must
// never fail because the cache backend is unavailable.
func (cli *Client) loadDeviceListsFromStoreLocked(ctx context.Context, jidsToSync []types.JID, devices *[]types.JID) []types.JID {
	if len(jidsToSync) == 0 || cli.Store == nil || cli.Store.DeviceLists == nil {
		return jidsToSync
	}
	ourJID := cli.getOwnID()
	if ourJID.IsEmpty() {
		return jidsToSync
	}
	found, err := cli.Store.DeviceLists.GetManyDeviceLists(ctx, ourJID.ToNonAD(), jidsToSync)
	if err != nil {
		cli.Log.Warnf("Failed to load %d device lists from cache store: %v", len(jidsToSync), err)
		return jidsToSync
	}
	if len(found) == 0 {
		return jidsToSync
	}
	remaining := make([]types.JID, 0, len(jidsToSync))
	for _, jid := range jidsToSync {
		if entry, ok := found[jid.ToNonAD()]; ok && len(entry.Devices) > 0 {
			cli.userDevicesCache[jid] = deviceCache{devices: entry.Devices, dhash: entry.DHash}
			*devices = append(*devices, entry.Devices...)
		} else {
			remaining = append(remaining, jid)
		}
	}
	if applied := len(jidsToSync) - len(remaining); applied > 0 {
		cli.Log.Infof("device_cache_read: resolved %d/%d device lists from persistent store, %d still need usync", applied, len(jidsToSync), len(remaining))
	}
	return remaining
}

// evictDeviceCache removes the given contacts' device lists from the in-memory
// cache. Used by the streaming pre-warmer after a chunk is resolved + persisted, so
// peak memory stays O(chunk): the lists live in the persistent store and reload on
// demand at send time (read-on-miss). No-op without a persistent store (DeviceLists
// nil, i.e. DEVICE_CACHE_PERSIST_ENABLED off) — evicting then would just force the
// next send to re-usync, so the cache is kept and prewarm peak heap reverts to
// O(roster). The pre-warmer's flat-memory guarantee therefore depends on B1a.
func (cli *Client) evictDeviceCache(jids []types.JID) {
	if cli.Store == nil || cli.Store.DeviceLists == nil {
		return
	}
	cli.userDevicesCacheLock.Lock()
	defer cli.userDevicesCacheLock.Unlock()
	for _, jid := range jids {
		delete(cli.userDevicesCache, jid)
	}
}

// tryOnNoDeviceContacts invokes the OnNoDeviceContacts callback under panic recovery and
// (optionally) bounded by cli.noDeviceContactsSema. Application callbacks are user code,
// so a panic here must not crash the worker; we log with a full stack trace for diagnosis.
func (cli *Client) tryOnNoDeviceContacts(ctx context.Context, jids []types.JID) {
	defer func() {
		if r := recover(); r != nil {
			cli.Log.Errorf("OnNoDeviceContacts callback panicked: %v\n%s", r, debug.Stack())
		}
	}()
	if cli.noDeviceContactsSema != nil {
		if err := cli.noDeviceContactsSema.Acquire(ctx, 1); err != nil {
			return
		}
		defer cli.noDeviceContactsSema.Release(1)
	}
	cli.OnNoDeviceContacts(jids)
}

type GetProfilePictureParams struct {
	Preview     bool
	ExistingID  string
	IsCommunity bool
	// This is a common group ID that you share with the target
	CommonGID types.JID
	// use this to query the profile photo of a group you don't have joined, but you have an invite code for
	InviteCode string
	// Persona ID when getting profile of Meta AI bots
	PersonaID string
}

// GetProfilePictureInfo gets the URL where you can download a WhatsApp user's profile picture or group's photo.
//
// Optionally, you can pass the last known profile picture ID.
// If the profile picture hasn't changed, this will return nil with no error.
//
// To get a community photo, you should pass `IsCommunity: true`, as otherwise you may get a 401 error.
func (cli *Client) GetProfilePictureInfo(ctx context.Context, jid types.JID, params *GetProfilePictureParams) (*types.ProfilePictureInfo, error) {
	if cli == nil {
		return nil, ErrClientIsNil
	}
	attrs := waBinary.Attrs{
		"query": "url",
	}
	var target, to types.JID
	if params == nil {
		params = &GetProfilePictureParams{}
	}
	if params.Preview {
		attrs["type"] = "preview"
	} else {
		attrs["type"] = "image"
	}
	if params.ExistingID != "" {
		attrs["id"] = params.ExistingID
	}
	if params.InviteCode != "" {
		attrs["invite"] = params.InviteCode
	}

	var expectWrapped bool
	var content []waBinary.Node
	namespace := "w:profile:picture"
	if params.IsCommunity {
		target = types.EmptyJID
		namespace = "w:g2"
		to = jid
		attrs["parent_group_jid"] = jid
		expectWrapped = true
		content = []waBinary.Node{{
			Tag: "pictures",
			Content: []waBinary.Node{{
				Tag:   "picture",
				Attrs: attrs,
			}},
		}}
	} else {
		to = types.ServerJID
		target = jid

		if !params.CommonGID.IsEmpty() {
			attrs["common_gid"] = params.CommonGID
		}

		if params.PersonaID != "" {
			attrs["persona_id"] = params.PersonaID
		}

		var pictureContent []waBinary.Node
		if token, _ := cli.ensureTCToken(ctx, jid); token != nil {
			pictureContent = []waBinary.Node{{
				Tag:     "tctoken",
				Content: token,
			}}
		}

		content = []waBinary.Node{{
			Tag:     "picture",
			Attrs:   attrs,
			Content: pictureContent,
		}}
	}
	resp, err := cli.sendIQ(ctx, infoQuery{
		Namespace: namespace,
		Type:      "get",
		To:        to,
		Target:    target,
		Content:   content,
	})
	if errors.Is(err, ErrIQNotAuthorized) {
		return nil, wrapIQError(ErrProfilePictureUnauthorized, err)
	} else if errors.Is(err, ErrIQNotFound) {
		return nil, wrapIQError(ErrProfilePictureNotSet, err)
	} else if err != nil {
		return nil, err
	}
	if expectWrapped {
		pics, ok := resp.GetOptionalChildByTag("pictures")
		if !ok {
			return nil, &ElementMissingError{Tag: "pictures", In: "response to profile picture query"}
		}
		resp = &pics
	}
	picture, ok := resp.GetOptionalChildByTag("picture")
	if !ok {
		if params.ExistingID != "" {
			return nil, nil
		}
		return nil, &ElementMissingError{Tag: "picture", In: "response to profile picture query"}
	}
	var info types.ProfilePictureInfo
	ag := picture.AttrGetter()
	if ag.OptionalInt("status") == 304 {
		return nil, nil
	} else if ag.OptionalInt("status") == 204 {
		return nil, ErrProfilePictureNotSet
	}
	info.ID = ag.String("id")
	info.URL = ag.String("url")
	info.Type = ag.String("type")
	info.DirectPath = ag.String("direct_path")
	info.Hash, _ = base64.StdEncoding.DecodeString(ag.OptionalString("hash"))
	if !ag.OK() {
		return &info, ag.Error()
	}
	return &info, nil
}

func (cli *Client) handleHistoricalPushNames(ctx context.Context, names []*waHistorySync.Pushname) {
	if cli.Store.Contacts == nil {
		return
	}
	cli.Log.Infof("Updating contact store with %d push names from history sync", len(names))
	for _, user := range names {
		if user.GetPushname() == "-" {
			continue
		}
		var changed bool
		if jid, err := types.ParseJID(user.GetID()); err != nil {
			cli.Log.Warnf("Failed to parse user ID '%s' in push name history sync: %v", user.GetID(), err)
		} else if changed, _, err = cli.Store.Contacts.PutPushName(ctx, jid, user.GetPushname()); err != nil {
			cli.Log.Warnf("Failed to store push name of %s from history sync: %v", jid, err)
		} else if changed {
			cli.Log.Debugf("Got push name %s for %s in history sync", user.GetPushname(), jid)
		}
	}
}

func (cli *Client) updatePushName(ctx context.Context, user, userAlt types.JID, messageInfo *types.MessageInfo, name string) {
	if cli.Store.Contacts == nil {
		return
	}
	user = user.ToNonAD()
	changed, previousName, err := cli.Store.Contacts.PutPushName(ctx, user, name)
	if err != nil {
		cli.Log.Errorf("Failed to save push name of %s in device store: %v", user, err)
	} else if changed {
		userAlt = userAlt.ToNonAD()
		if userAlt.IsEmpty() {
			userAlt, _ = cli.Store.GetAltJID(ctx, user)
		}
		if !userAlt.IsEmpty() {
			_, _, err = cli.Store.Contacts.PutPushName(ctx, userAlt, name)
			if err != nil {
				cli.Log.Errorf("Failed to save push name of %s in device store: %v", userAlt, err)
			}
		}
		cli.Log.Debugf("Push name of %s changed from %s to %s, dispatching event", user, previousName, name)
		cli.dispatchEvent(&events.PushName{
			JID:         user,
			JIDAlt:      userAlt,
			Message:     messageInfo,
			OldPushName: previousName,
			NewPushName: name,
		})
	}
}

func (cli *Client) updateBusinessName(ctx context.Context, user, userAlt types.JID, messageInfo *types.MessageInfo, name string) {
	if cli.Store.Contacts == nil {
		return
	}
	changed, previousName, err := cli.Store.Contacts.PutBusinessName(ctx, user, name)
	if err != nil {
		cli.Log.Errorf("Failed to save business name of %s in device store: %v", user, err)
	} else if changed {
		userAlt = userAlt.ToNonAD()
		if userAlt.IsEmpty() {
			userAlt, _ = cli.Store.GetAltJID(ctx, user)
		}
		if !userAlt.IsEmpty() {
			_, _, err = cli.Store.Contacts.PutBusinessName(ctx, userAlt, name)
			if err != nil {
				cli.Log.Errorf("Failed to save push name of %s in device store: %v", userAlt, err)
			}
		}
		cli.Log.Debugf("Business name of %s changed from %s to %s, dispatching event", user, previousName, name)
		cli.dispatchEvent(&events.BusinessName{
			JID:             user,
			Message:         messageInfo,
			OldBusinessName: previousName,
			NewBusinessName: name,
		})
	}
}

func parseVerifiedName(businessNode waBinary.Node) (*types.VerifiedName, error) {
	if businessNode.Tag != "business" {
		return nil, nil
	}
	verifiedNameNode, ok := businessNode.GetOptionalChildByTag("verified_name")
	if !ok {
		return nil, nil
	}
	return parseVerifiedNameContent(verifiedNameNode)
}

func parseVerifiedNameContent(verifiedNameNode waBinary.Node) (*types.VerifiedName, error) {
	rawCert, ok := verifiedNameNode.Content.([]byte)
	if !ok {
		return nil, nil
	}

	var cert waVnameCert.VerifiedNameCertificate
	err := proto.Unmarshal(rawCert, &cert)
	if err != nil {
		return nil, err
	}
	var certDetails waVnameCert.VerifiedNameCertificate_Details
	err = proto.Unmarshal(cert.GetDetails(), &certDetails)
	if err != nil {
		return nil, err
	}
	return &types.VerifiedName{
		Certificate: &cert,
		Details:     &certDetails,
	}, nil
}

func parseDeviceList(user types.JID, deviceNode waBinary.Node) []types.JID {
	deviceList := deviceNode.GetChildByTag("device-list")
	if deviceNode.Tag != "devices" || deviceList.Tag != "device-list" {
		return nil
	}
	children := deviceList.GetChildren()
	devices := make([]types.JID, 0, len(children))
	for _, device := range children {
		deviceID, ok := device.AttrGetter().GetInt64("id", true)
		isHosted := device.AttrGetter().Bool("is_hosted")
		if device.Tag != "device" || !ok {
			continue
		}
		user.Device = uint16(deviceID)
		if isHosted {
			hostedUser := user
			if user.Server == types.HiddenUserServer {
				hostedUser.Server = types.HostedLIDServer
			} else {
				hostedUser.Server = types.HostedServer
			}
			devices = append(devices, hostedUser)
		} else {
			devices = append(devices, user)
		}
	}
	return devices
}

func parseFBDeviceList(user types.JID, deviceList waBinary.Node) deviceCache {
	children := deviceList.GetChildren()
	devices := make([]types.JID, 0, len(children))
	for _, device := range children {
		deviceID, ok := device.AttrGetter().GetInt64("id", true)
		if device.Tag != "device" || !ok {
			continue
		}
		user.Device = uint16(deviceID)
		devices = append(devices, user)
		// TODO take identities here too?
	}
	// TODO do something with the icdc blob?
	return deviceCache{
		devices: devices,
		dhash:   deviceList.AttrGetter().String("dhash"),
	}
}

func (cli *Client) getFBIDDevicesInternal(ctx context.Context, jids []types.JID) (*waBinary.Node, error) {
	users := make([]waBinary.Node, len(jids))
	for i, jid := range jids {
		users[i].Tag = "user"
		users[i].Attrs = waBinary.Attrs{"jid": jid}
		// TODO include dhash for users
	}
	resp, err := cli.sendIQ(ctx, infoQuery{
		Namespace: "fbid:devices",
		Type:      iqGet,
		To:        types.ServerJID,
		Content: []waBinary.Node{{
			Tag:     "users",
			Content: users,
		}},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to send usync query: %w", err)
	} else if list, ok := resp.GetOptionalChildByTag("users"); !ok {
		return nil, &ElementMissingError{Tag: "users", In: "response to fbid devices query"}
	} else {
		return &list, err
	}
}

func (cli *Client) getFBIDDevices(ctx context.Context, jids []types.JID) ([]types.JID, error) {
	var devices []types.JID
	for chunk := range slices.Chunk(jids, 15) {
		list, err := cli.getFBIDDevicesInternal(ctx, chunk)
		if err != nil {
			return nil, err
		}
		for _, user := range list.GetChildren() {
			jid, jidOK := user.Attrs["jid"].(types.JID)
			if user.Tag != "user" || !jidOK {
				continue
			}
			userDevices := parseFBDeviceList(jid, user.GetChildByTag("devices"))
			cli.userDevicesCache[jid] = userDevices
			devices = append(devices, userDevices.devices...)
		}
	}
	return devices, nil
}

type UsyncQueryExtras struct {
	BotListInfo         []types.BotListInfo
	IncludePrivacyToken bool
}

// usync resolves a usync query, automatically splitting jids into chunks no
// larger than maxUsyncUsersPerQuery so no single IQ exceeds the size WhatsApp
// tolerates — a larger usync is held ~8-11s and then drops the socket, followed
// by a 403 account lock. This is the single cap for ALL usync callers
// (GetUserDevices, GetUserInfo, IsOnWhatsApp, the pre-warmer, …). Multi-chunk
// responses are merged into one <list> node (every caller iterates its <user>
// children, none reads list-level attrs). Each IQ is logged (usync_send) for
// ban attribution.
func (cli *Client) usync(ctx context.Context, jids []types.JID, mode, context string, query []waBinary.Node, extra ...UsyncQueryExtras) (*waBinary.Node, error) {
	if cli == nil {
		return nil, ErrClientIsNil
	}
	if len(extra) > 1 {
		return nil, errors.New("only one extra parameter may be provided to usync()")
	}

	chunks := usyncDeviceChunks(jids)
	if len(chunks) <= 1 {
		cli.Log.Infof("usync_send mode=%s context=%s users=%d index=1 total=1", mode, context, len(jids))
		return cli.usyncOnce(ctx, jids, mode, context, query, extra...)
	}
	merged := make([]waBinary.Node, 0, len(jids))
	for i, chunk := range chunks {
		cli.Log.Infof("usync_send mode=%s context=%s users=%d index=%d total=%d", mode, context, len(chunk), i+1, len(chunks))
		list, err := cli.usyncOnce(ctx, chunk, mode, context, query, extra...)
		if err != nil {
			return nil, err
		}
		merged = append(merged, list.GetChildren()...)
	}
	return &waBinary.Node{Tag: "list", Content: merged}, nil
}

// usyncOnce sends exactly one usync IQ. Callers must keep len(jids) ≤
// maxUsyncUsersPerQuery — use usync() for the automatic cap + chunking.
func (cli *Client) usyncOnce(ctx context.Context, jids []types.JID, mode, context string, query []waBinary.Node, extra ...UsyncQueryExtras) (*waBinary.Node, error) {
	var extras UsyncQueryExtras
	if len(extra) == 1 {
		extras = extra[0]
	}

	userList := make([]waBinary.Node, len(jids))
	for i, jid := range jids {
		userList[i].Tag = "user"
		jid = jid.ToNonAD()

		switch jid.Server {
		case types.LegacyUserServer:
			userList[i].Content = []waBinary.Node{{
				Tag:     "contact",
				Content: jid.String(),
			}}
		case types.DefaultUserServer, types.HiddenUserServer:
			// NOTE: You can pass in an LID with a JID (<lid jid=...> user node)
			// Not sure if you can just put the LID in the jid tag here (works for <devices> queries mainly)
			userList[i].Attrs = waBinary.Attrs{"jid": jid}
			if jid.IsBot() {
				var personaID string
				for _, bot := range extras.BotListInfo {
					if bot.BotJID.User == jid.User {
						personaID = bot.PersonaID
					}
				}
				userList[i].Content = []waBinary.Node{{
					Tag: "bot",
					Content: []waBinary.Node{{
						Tag:   "profile",
						Attrs: waBinary.Attrs{"persona_id": personaID},
					}},
				}}
			} else if extras.IncludePrivacyToken {
				token, err := cli.ensureTCToken(ctx, jid)
				if err != nil {
					cli.Log.Warnf("Failed to get privacy token for usync status query to %s: %v", jid, err)
				} else if len(token) > 0 {
					userList[i].Content = []waBinary.Node{{
						Tag:     "tctoken",
						Content: token,
					}}
				}
			}
		default:
			return nil, fmt.Errorf("unknown user server '%s'", jid.Server)
		}
	}
	resp, err := cli.sendIQ(ctx, infoQuery{
		Namespace: "usync",
		Type:      "get",
		To:        types.ServerJID,
		Content: []waBinary.Node{{
			Tag: "usync",
			Attrs: waBinary.Attrs{
				"sid":     cli.generateRequestID(),
				"mode":    mode,
				"last":    "true",
				"index":   "0",
				"context": context,
			},
			Content: []waBinary.Node{
				{Tag: "query", Content: query},
				{Tag: "list", Content: userList},
			},
		}},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to send usync query: %w", err)
	} else if list, ok := resp.GetOptionalChildByTag("usync", "list"); !ok {
		return nil, &ElementMissingError{Tag: "list", In: "response to usync query"}
	} else {
		return &list, err
	}
}

func (cli *Client) parseBlocklist(node *waBinary.Node) *types.Blocklist {
	output := &types.Blocklist{
		DHash: node.AttrGetter().String("dhash"),
	}
	for _, child := range node.GetChildren() {
		ag := child.AttrGetter()
		blockedJID := ag.JID("jid")
		if !ag.OK() {
			cli.Log.Debugf("Ignoring contact blocked data with unexpected attributes: %v", ag.Error())
			continue
		}

		output.JIDs = append(output.JIDs, blockedJID)
	}
	return output
}

// GetBlocklist gets the list of users that this user has blocked.
func (cli *Client) GetBlocklist(ctx context.Context) (*types.Blocklist, error) {
	resp, err := cli.sendIQ(ctx, infoQuery{
		Namespace: "blocklist",
		Type:      iqGet,
		To:        types.ServerJID,
	})
	if err != nil {
		return nil, err
	}
	list, ok := resp.GetOptionalChildByTag("list")
	if !ok {
		return nil, &ElementMissingError{Tag: "list", In: "response to blocklist query"}
	}
	return cli.parseBlocklist(&list), nil
}

// UpdateBlocklist updates the user's block list and returns the updated list.
func (cli *Client) UpdateBlocklist(ctx context.Context, jid types.JID, action events.BlocklistChangeAction) (*types.Blocklist, error) {
	resp, err := cli.sendIQ(ctx, infoQuery{
		Namespace: "blocklist",
		Type:      iqSet,
		To:        types.ServerJID,
		Content: []waBinary.Node{{
			Tag: "item",
			Attrs: waBinary.Attrs{
				"jid":    jid,
				"action": string(action),
			},
		}},
	})
	if err != nil {
		return nil, err
	}
	list, ok := resp.GetOptionalChildByTag("list")
	if !ok {
		return nil, &ElementMissingError{Tag: "list", In: "response to blocklist update"}
	}
	return cli.parseBlocklist(&list), err
}
