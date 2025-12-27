// Copyright 2019 The Vearch Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package document

import (
	"context"
	"sync"
	"time"

	"github.com/vearch/vearch/v3/internal/entity"
	"github.com/vearch/vearch/v3/internal/proto/vearchpb"
)

// Space metadata cache configuration
const (
	// spaceCacheTTL is the time-to-live for cached space metadata (5 minutes)
	spaceCacheTTL = 5 * time.Minute
	// spaceCacheCleanup is the interval for cleanup goroutine to run (10 minutes)
	spaceCacheCleanup = 10 * time.Minute
)

// spaceCacheEntry represents a cached space with expiration time
type spaceCacheEntry struct {
	space      *entity.Space
	expiration int64 // Unix timestamp when this entry expires
}

// SpaceCache provides thread-safe caching for space metadata.
// It reduces load on the master server by caching space information
// with a configurable TTL. A background goroutine periodically cleans up expired entries.
type SpaceCache struct {
	spaces     sync.Map
	docService *docService
	metrics    *MetricsRecorder
}

// NewSpaceCache creates a new space metadata cache and starts the cleanup goroutine.
//
// Parameters:
//   - docService: The document service used to fetch uncached data from master
//
// Returns:
//   - *SpaceCache: A new space cache instance
func NewSpaceCache(docService *docService) *SpaceCache {
	cache := &SpaceCache{
		docService: docService,
		metrics:    NewMetricsRecorder(),
	}
	// Start cleanup goroutine
	go cache.cleanupExpired()
	return cache
}

// GetSpace retrieves space metadata from cache or master server.
// If the space is found in cache and not expired, returns immediately.
// Otherwise, fetches from master and caches for future requests.
//
// Parameters:
//   - ctx: Context for the request
//   - head: Request header containing db name and space name
//
// Returns:
//   - *entity.Space: The space object
//   - error: Any error encountered during lookup
func (sc *SpaceCache) GetSpace(ctx context.Context, head *vearchpb.RequestHead) (*entity.Space, error) {
	// Create cache key from db and space name
	key := head.DbName + ":" + head.SpaceName

	// Check cache first
	if cached, found := sc.spaces.Load(key); found {
		entry := cached.(*spaceCacheEntry)
		if time.Now().Unix() < entry.expiration {
			sc.metrics.RecordCacheLookup("space", true)
			return entry.space, nil
		}
		// Expired, remove it
		sc.spaces.Delete(key)
	}

	// Cache miss
	sc.metrics.RecordCacheLookup("space", false)

	// Cache miss or expired, fetch from master
	space, err := sc.docService.getSpaceFromMaster(ctx, head)
	if err != nil {
		return nil, err
	}

	// Store in cache
	sc.spaces.Store(key, &spaceCacheEntry{
		space:      space,
		expiration: time.Now().Add(spaceCacheTTL).Unix(),
	})

	return space, nil
}

// InvalidateSpace removes a space from the cache.
// This should be called when space metadata is updated to force a refresh.
//
// Parameters:
//   - dbName: The database name
//   - spaceName: The space name to invalidate
func (sc *SpaceCache) InvalidateSpace(dbName, spaceName string) {
	key := dbName + ":" + spaceName
	sc.spaces.Delete(key)
}

// InvalidateAll removes all spaces from the cache.
// This can be called when doing a full cache refresh.
func (sc *SpaceCache) InvalidateAll() {
	sc.spaces.Range(func(key, value interface{}) bool {
		sc.spaces.Delete(key)
		return true
	})
}

// cleanupExpired is a background goroutine that periodically removes expired cache entries.
// It runs every spaceCacheCleanup interval (10 minutes) to prevent memory leaks from
// accumulating expired entries.
func (sc *SpaceCache) cleanupExpired() {
	ticker := time.NewTicker(spaceCacheCleanup)
	defer ticker.Stop()

	for range ticker.C {
		now := time.Now().Unix()

		// Clean up expired spaces
		sc.spaces.Range(func(key, value interface{}) bool {
			entry := value.(*spaceCacheEntry)
			if now >= entry.expiration {
				sc.spaces.Delete(key)
			}
			return true
		})
	}
}
