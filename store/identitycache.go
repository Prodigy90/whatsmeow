// Copyright (c) 2025 Tulir Asokan
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package store

import (
	"context"
	"fmt"

	"go.mau.fi/util/exsync"
)

const contextKeyIdentityCache contextKey = 1

type identityCacheEntry struct {
	Dirty bool
	Found bool
	Key   [32]byte
}

type identityCache = exsync.Map[string, identityCacheEntry]

func getIdentityCache(ctx context.Context) *identityCache {
	if ctx == nil {
		return nil
	}
	val := ctx.Value(contextKeyIdentityCache)
	if val == nil {
		return nil
	}
	if cache, ok := val.(*identityCache); ok {
		return cache
	}
	return nil
}

func getCachedIdentity(ctx context.Context, addr string) (key *[32]byte, inCache bool) {
	cache := getIdentityCache(ctx)
	if cache == nil {
		return nil, false
	}
	entry, ok := cache.Get(addr)
	if !ok {
		return nil, false
	}
	if !entry.Found {
		return nil, true // known not-found (TOFU)
	}
	return &entry.Key, true
}

func evictCachedIdentity(ctx context.Context, addr string) {
	cache := getIdentityCache(ctx)
	if cache == nil {
		return
	}
	cache.Delete(addr)
}

func putCachedIdentity(ctx context.Context, addr string, key [32]byte) bool {
	cache := getIdentityCache(ctx)
	if cache == nil {
		return false
	}
	if existing, ok := cache.Get(addr); ok && existing.Found && existing.Key == key {
		return true // key unchanged, skip marking dirty
	}
	cache.Set(addr, identityCacheEntry{
		Dirty: true,
		Found: true,
		Key:   key,
	})
	return true
}

func (device *Device) EvictCachedIdentity(ctx context.Context, addr string) {
	evictCachedIdentity(ctx, addr)
}

func (device *Device) WithCachedIdentities(ctx context.Context, addresses []string) (context.Context, error) {
	if len(addresses) == 0 {
		return ctx, nil
	}

	identities, err := device.Identities.GetManyIdentities(ctx, addresses)
	if err != nil {
		return ctx, fmt.Errorf("failed to prefetch identities: %w", err)
	}
	wrapped := make(map[string]identityCacheEntry, len(addresses))
	var found, notFound int
	for _, addr := range addresses {
		key, exists := identities[addr]
		if exists && key != nil {
			wrapped[addr] = identityCacheEntry{Found: true, Key: *key}
			found++
		} else {
			wrapped[addr] = identityCacheEntry{Found: false}
			notFound++
		}
	}

	device.Log.Infof("Identity cache prefetch: %d found, %d not-found (TOFU), %d total", found, notFound, len(addresses))

	ctx = context.WithValue(ctx, contextKeyIdentityCache, (*identityCache)(exsync.NewMapWithData(wrapped)))
	return ctx, nil
}

func (device *Device) PutCachedIdentities(ctx context.Context) error {
	cache := getIdentityCache(ctx)
	if cache == nil {
		return nil
	}
	dirtyIdentities := make(map[string][32]byte)
	for addr, item := range cache.Iter() {
		if item.Dirty {
			dirtyIdentities[addr] = item.Key
		}
	}
	if len(dirtyIdentities) > 0 {
		device.Log.Infof("Identity cache flush: %d dirty entries to write", len(dirtyIdentities))
		err := device.Identities.PutManyIdentities(ctx, dirtyIdentities)
		if err != nil {
			return fmt.Errorf("failed to store cached identities: %w", err)
		}
	}
	cache.Clear()
	return nil
}
