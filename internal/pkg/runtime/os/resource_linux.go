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

//go:build linux

package os

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"syscall"

	"github.com/shirou/gopsutil/process"
	"github.com/vearch/vearch/v3/internal/pkg/log"
	"github.com/vearch/vearch/v3/internal/proto/vearchpb"
)

func readCgroupMemory() (available, limit uint64, err error) {
	memoryLimitPath := "/sys/fs/cgroup/memory/memory.limit_in_bytes"

	data, err := os.ReadFile(memoryLimitPath)
	if err != nil {
		return available, limit, vearchpb.NewError(vearchpb.ErrorEnum_INTERNAL_ERROR, fmt.Errorf("failed to read file: %v", err))
	}

	limitInt64, err := strconv.ParseInt(strings.TrimSpace(string(data)), 10, 64)
	if err != nil {
		return available, limit, vearchpb.NewError(vearchpb.ErrorEnum_INTERNAL_ERROR, fmt.Errorf("failed to convert value to int64: %v", err))
	}

	pid := os.Getpid()
	p, err := process.NewProcess(int32(pid))
	if err != nil {
		fmt.Printf("Failed to get process: %v\n", err)
		return
	}

	memInfo, err := p.MemoryInfo()
	if err != nil {
		fmt.Printf("Failed to get memory info: %v\n", err)
		return
	}

	physicalMemory := memInfo.RSS
	usage := physicalMemory / 1024 / 1024
	limit = uint64(limitInt64) / 1024 / 1024
	available = limit - usage

	return available, limit, nil
}

func CheckResource(path string) (is bool, err error) {
	var stat syscall.Statfs_t
	err = syscall.Statfs(path, &stat)

	if err != nil {
		log.Error("syscall.Statfs %s err %v", path, err)
		return false, nil
	}

	totalDisk := stat.Blocks * uint64(stat.Bsize) / 1024 / 1024
	availDisk := stat.Bavail * uint64(stat.Bsize) / 1024 / 1024
	log.Debug("availDisk %dM, totalDisk %dM", availDisk, totalDisk)

	if !(availDisk > 1024 || float64(availDisk)/float64(totalDisk) > 0.05) {
		return true, vearchpb.NewError(vearchpb.ErrorEnum_PARTITION_RESOURCE_EXHAUSTED, fmt.Errorf("disk space not enough: total [%d]M, avail [%d]M", totalDisk, availDisk))
	}

	var info syscall.Sysinfo_t

	if err = syscall.Sysinfo(&info); err != nil {
		log.Error("syscall.Sysinfo err %v", err)
		return false, nil
	}

	totalMemory := (info.Totalram * uint64(info.Unit)) / 1024 / 1024
	availableMemory := (info.Freeram*uint64(info.Unit) + info.Bufferram*uint64(info.Unit)) / 1024 / 1024

	cgroupAvailableMemory, cgroupTotalMemory, err := readCgroupMemory()

	if err == nil {
		if cgroupTotalMemory < totalMemory {
			totalMemory = cgroupTotalMemory
			availableMemory = cgroupAvailableMemory
		}
	}

	log.Debug("total memory %dM, available memory %dM", totalMemory, availableMemory)

	if !(availableMemory > 512 || float64(availableMemory)/float64(totalMemory) > 0.05) {
		return true, vearchpb.NewError(vearchpb.ErrorEnum_PARTITION_RESOURCE_EXHAUSTED, fmt.Errorf("available memory not enough: total [%d]M, avail [%d]M", totalMemory, availableMemory))
	}
	return false, nil
}
