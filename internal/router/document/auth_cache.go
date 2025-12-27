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
)

// Authentication cache configuration
const (
	// authCacheTTL is the time-to-live for cached user and role entries (10 minutes)
	authCacheTTL = 10 * time.Minute
	// authCacheCleanup is the interval for cleanup goroutine to run (20 minutes)
	authCacheCleanup = 20 * time.Minute
)

// cacheEntry represents a cached value with expiration time
type cacheEntry struct {
	value      interface{} // The cached user or role object
	expiration int64       // Unix timestamp when this entry expires
}

// AuthCache provides thread-safe caching for user and role authentication data.
// It reduces load on the master server by caching authentication information
// with a configurable TTL. A background goroutine periodically cleans up expired entries.
type AuthCache struct {
	users      sync.Map
	roles      sync.Map
	docService *docService
}

// NewAuthCache creates a new authentication cache and starts the cleanup goroutine.
//
// Parameters:
//   - docService: The document service used to fetch uncached data from master
//
// Returns:
//   - *AuthCache: A new authentication cache instance
func NewAuthCache(docService *docService) *AuthCache {
	cache := &AuthCache{
		docService: docService,
	}
	// Start cleanup goroutine
	go cache.cleanupExpired()
	return cache
}

// GetUser retrieves user information from cache or master server.
// If the user is found in cache and not expired, returns immediately.
// Otherwise, fetches from master and caches for future requests.
//
// Parameters:
//   - ctx: Context for the request
//   - username: The username to look up
//
// Returns:
//   - *entity.User: The user object
//   - error: Any error encountered during lookup
func (ac *AuthCache) GetUser(ctx context.Context, username string) (*entity.User, error) {
	// Check cache first
	if cached, found := ac.users.Load(username); found {
		entry := cached.(*cacheEntry)
		if time.Now().Unix() < entry.expiration {
			return entry.value.(*entity.User), nil
		}
		// Expired, remove it
		ac.users.Delete(username)
	}

	// Cache miss or expired, fetch from master
	user, err := ac.docService.getUser(ctx, username)
	if err != nil {
		return nil, err
	}

	// Store in cache
	ac.users.Store(username, &cacheEntry{
		value:      user,
		expiration: time.Now().Add(authCacheTTL).Unix(),
	})

	return user, nil
}

// GetRole retrieves role information from cache or master server.
// If the role is found in cache and not expired, returns immediately.
// Otherwise, fetches from master and caches for future requests.
//
// Parameters:
//   - ctx: Context for the request
//   - roleName: The role name to look up
//
// Returns:
//   - *entity.Role: The role object
//   - error: Any error encountered during lookup
func (ac *AuthCache) GetRole(ctx context.Context, roleName string) (*entity.Role, error) {
	// Check cache first
	if cached, found := ac.roles.Load(roleName); found {
		entry := cached.(*cacheEntry)
		if time.Now().Unix() < entry.expiration {
			return entry.value.(*entity.Role), nil
		}
		// Expired, remove it
		ac.roles.Delete(roleName)
	}

	// Cache miss or expired, fetch from master
	role, err := ac.docService.getRole(ctx, roleName)
	if err != nil {
		return nil, err
	}

	// Store in cache
	ac.roles.Store(roleName, &cacheEntry{
		value:      role,
		expiration: time.Now().Add(authCacheTTL).Unix(),
	})

	return role, nil
}

// InvalidateUser removes a user from the cache.
// This should be called when user information is updated to force a refresh.
//
// Parameters:
//   - username: The username to invalidate
func (ac *AuthCache) InvalidateUser(username string) {
	ac.users.Delete(username)
}

// InvalidateRole removes a role from the cache.
// This should be called when role information is updated to force a refresh.
//
// Parameters:
//   - roleName: The role name to invalidate
func (ac *AuthCache) InvalidateRole(roleName string) {
	ac.roles.Delete(roleName)
}

// cleanupExpired is a background goroutine that periodically removes expired cache entries.
// It runs every authCacheCleanup interval (20 minutes) to prevent memory leaks from
// accumulating expired entries.
func (ac *AuthCache) cleanupExpired() {
	ticker := time.NewTicker(authCacheCleanup)
	defer ticker.Stop()

	for range ticker.C {
		now := time.Now().Unix()

		// Clean up expired users
		ac.users.Range(func(key, value interface{}) bool {
			entry := value.(*cacheEntry)
			if now >= entry.expiration {
				ac.users.Delete(key)
			}
			return true
		})

		// Clean up expired roles
		ac.roles.Range(func(key, value interface{}) bool {
			entry := value.(*cacheEntry)
			if now >= entry.expiration {
				ac.roles.Delete(key)
			}
			return true
		})
	}
}
