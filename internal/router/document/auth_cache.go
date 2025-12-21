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

const (
	authCacheTTL     = 10 * time.Minute
	authCacheCleanup = 20 * time.Minute
)

type cacheEntry struct {
	value      interface{}
	expiration int64
}

type AuthCache struct {
	users      sync.Map
	roles      sync.Map
	docService *docService
}

func NewAuthCache(docService *docService) *AuthCache {
	cache := &AuthCache{
		docService: docService,
	}
	// Start cleanup goroutine
	go cache.cleanupExpired()
	return cache
}

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

func (ac *AuthCache) InvalidateUser(username string) {
	ac.users.Delete(username)
}

func (ac *AuthCache) InvalidateRole(roleName string) {
	ac.roles.Delete(roleName)
}

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
