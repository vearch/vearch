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

package uuid

// Generator is used to generate opaque unique strings.
type Generator interface {
	GetUUID() string
}

var (
	flakeUUIDGenerator = NewFlakeGenerator()
	timeUUIDGenerator  = NewTimeGenerator()
)

// FlakeUUID Generates an UUID (similar to Flake IDs)
func FlakeUUID() string {
	return flakeUUIDGenerator.GetUUID()
}

// TimeUUID Generates a time-based UUID for tests.
func TimeUUID() string {
	return timeUUIDGenerator.GetUUID()
}
