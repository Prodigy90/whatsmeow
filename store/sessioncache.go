// Copyright (c) 2025 Tulir Asokan
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package store

import (
	"context"
	"fmt"

	"github.com/rs/zerolog"
	"go.mau.fi/libsignal/state/record"

	"go.mau.fi/util/exsync"
)

type contextKey int

const (
	contextKeySessionCache contextKey = iota
)

type sessionCacheEntry struct {
	Dirty  bool
	Found  bool
	Record *record.Session
}

type sessionCache = exsync.Map[string, sessionCacheEntry]

func getSessionCache(ctx context.Context) *sessionCache {
	if ctx == nil {
		return nil
	}
	val := ctx.Value(contextKeySessionCache)
	if val == nil {
		return nil
	}
	if cache, ok := val.(*sessionCache); ok {
		return cache
	}
	return nil
}

func getCachedSession(ctx context.Context, addr string) *record.Session {
	cache := getSessionCache(ctx)
	if cache == nil {
		return nil
	}
	sess, ok := cache.Get(addr)
	if !ok {
		return nil
	}
	return sess.Record
}

func evictCachedSession(ctx context.Context, addr string) {
	cache := getSessionCache(ctx)
	if cache == nil {
		return
	}
	cache.Delete(addr)
}

func putCachedSession(ctx context.Context, addr string, record *record.Session) bool {
	cache := getSessionCache(ctx)
	if cache == nil {
		return false
	}
	cache.Set(addr, sessionCacheEntry{
		Dirty:  true,
		Found:  true,
		Record: record,
	})
	return true
}

func (device *Device) EvictCachedSession(ctx context.Context, addr string) {
	evictCachedSession(ctx, addr)
}

// sessionIterator is implemented by stores that can stream session blobs row by row
// (without materializing a map of blob copies). The blob passed to the callback is
// only valid for the duration of the callback.
type sessionIterator interface {
	IterateSessions(ctx context.Context, addresses []string, callback func(string, []byte) error) error
}

// sessionLoader is implemented by stores that can stream a single session blob into
// a callback without an intermediate copy. Returns whether a row existed.
type sessionLoader interface {
	IterateSession(ctx context.Context, address string, callback func([]byte) error) (bool, error)
}

func (device *Device) WithCachedSessions(ctx context.Context, addresses []string) (map[string]bool, context.Context, error) {
	if len(addresses) == 0 {
		return nil, ctx, nil
	}

	wrapped := make(map[string]sessionCacheEntry, len(addresses))
	existingSessions := make(map[string]bool, len(addresses))
	if iter, ok := device.Sessions.(sessionIterator); ok {
		// Streaming path: deserialize row by row instead of first materializing a
		// map holding every raw blob (matters on large prefetches / small pods).
		var broken map[string]struct{}
		err := iter.IterateSessions(ctx, addresses, func(addr string, rawSess []byte) error {
			if len(rawSess) == 0 {
				return nil // NULL session row — falls through to the cold fill below
			}
			sessionRecord, err := record.NewSessionFromBytes(rawSess, SignalProtobufSerializer.Session, SignalProtobufSerializer.State)
			if err != nil {
				zerolog.Ctx(ctx).Err(err).
					Str("address", addr).
					Msg("Failed to deserialize session")
				if broken == nil {
					broken = make(map[string]struct{})
				}
				broken[addr] = struct{}{}
				return nil
			}
			existingSessions[addr] = true
			wrapped[addr] = sessionCacheEntry{Record: sessionRecord, Found: true}
			return nil
		})
		if err != nil {
			return nil, ctx, fmt.Errorf("failed to prefetch sessions: %w", err)
		}
		for _, addr := range addresses {
			if _, isBroken := broken[addr]; isBroken {
				// Parity with the map path: a corrupt session stays out of the cache
				// entirely (so it is NOT silently replaced with a fresh session).
				continue
			}
			if _, ok := wrapped[addr]; !ok {
				existingSessions[addr] = false
				wrapped[addr] = sessionCacheEntry{Record: record.NewSession(SignalProtobufSerializer.Session, SignalProtobufSerializer.State)}
			}
		}
	} else {
		sessions, err := device.Sessions.GetManySessions(ctx, addresses)
		if err != nil {
			return nil, ctx, fmt.Errorf("failed to prefetch sessions: %w", err)
		}
		for addr, rawSess := range sessions {
			var sessionRecord *record.Session
			var found bool
			if rawSess == nil {
				sessionRecord = record.NewSession(SignalProtobufSerializer.Session, SignalProtobufSerializer.State)
			} else {
				found = true
				sessionRecord, err = record.NewSessionFromBytes(rawSess, SignalProtobufSerializer.Session, SignalProtobufSerializer.State)
				if err != nil {
					zerolog.Ctx(ctx).Err(err).
						Str("address", addr).
						Msg("Failed to deserialize session")
					continue
				}
			}
			existingSessions[addr] = found
			wrapped[addr] = sessionCacheEntry{Record: sessionRecord, Found: found}
		}
	}

	ctx = context.WithValue(ctx, contextKeySessionCache, (*sessionCache)(exsync.NewMapWithData(wrapped)))
	return existingSessions, ctx, nil
}

func (device *Device) PutCachedSessions(ctx context.Context) error {
	cache := getSessionCache(ctx)
	if cache == nil {
		return nil
	}
	dirtySessions := make(map[string][]byte)
	for addr, item := range cache.Iter() {
		if item.Dirty {
			dirtySessions[addr] = item.Record.Serialize()
		}
	}
	if len(dirtySessions) > 0 {
		err := device.Sessions.PutManySessions(ctx, dirtySessions)
		if err != nil {
			return fmt.Errorf("failed to store cached sessions: %w", err)
		}
	}
	cache.Clear()
	return nil
}
