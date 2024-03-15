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

package vmap

import (
	"strings"
)

// make map to level 1 example map[a][b]=1  it wil map[a.b]=1
func DrawMap(maps map[string]interface{}, split string) map[string]interface{} {
	newMap := make(map[string]interface{})
	drawMap(newMap, maps, "", split)
	return newMap
}

func drawMap(result, maps map[string]interface{}, prefix, split string) {
	newPrefix := prefix
	for k, v := range maps {

		if prefix == "" {
			newPrefix = k
		} else {
			newPrefix = prefix + split + k
		}

		switch v.(type) {
		case map[string]interface{}:
			drawMap(result, v.(map[string]interface{}), newPrefix, split)
		default:
			result[newPrefix] = v
		}
	}
}

// make map to level 1 example map[a.b]=1 it will map[a][b]=1
func AssembleMap(maps map[string]interface{}, split string) map[string]interface{} {
	newMap := make(map[string]interface{})

	for k, v := range maps {
		split := strings.Split(k, split)

		var temp interface{}
		pre := newMap
		for i := 0; i < len(split)-1; i++ {
			temp = pre[split[i]]
			if temp == nil {
				temp = make(map[string]interface{})
				pre[split[i]] = temp.(map[string]interface{})
			}
			pre = temp.(map[string]interface{})
		}
		pre[split[len(split)-1]] = v
	}
	return newMap
}

// deep merge src to dest
func MergeMap(dest, src map[string]interface{}) {
	for k, v := range src {
		switch v.(type) {
		case map[string]interface{}:
			if descV, ok := dest[k]; ok {
				if descVmap, cOk := descV.(map[string]interface{}); cOk {
					MergeMap(descVmap, v.(map[string]interface{}))
				} else {
					dest[k] = v
				}
			}
		default:
			dest[k] = v
		}
	}
}

// is key in map
func MapContains(m map[string]interface{}, key string) bool {
	_, ok := m[key]
	return ok
}

func CopyMap(src map[string]string) map[string]string {
	dst := make(map[string]string, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}
