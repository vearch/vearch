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

//go:build darwin

package os

func ReadCgroupMemory() (available, limit uint64, err error) {
	return 0, 0, nil
}

func ReadCgroupTotalMemory() (limit int64, err error) {
	return -1, nil
}

func ReadProcMemory() (available, total uint64, err error) {
	return 0, 0, nil
}

func GetSystemMemory() (total uint64, err error) {
	return 0, nil
}

func GetMemoryUsage() (memoryUsage uint64, err error) {
	return 0, nil
}
