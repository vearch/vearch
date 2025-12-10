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
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/shirou/gopsutil/process"
	"github.com/vearch/vearch/v3/internal/proto/vearchpb"
)

func ReadCgroupMemory() (available, limit uint64, err error) {
	limitInt64, err := ReadCgroupTotalMemory()

	if err != nil {
		return available, limit, err
	}

	pid := os.Getpid()
	p, err := process.NewProcess(int32(pid))
	if err != nil {
		return available, limit, vearchpb.NewError(vearchpb.ErrorEnum_INTERNAL_ERROR, fmt.Errorf("failed to get process: %v", err))
	}

	memInfo, err := p.MemoryInfo()
	if err != nil {
		return available, limit, vearchpb.NewError(vearchpb.ErrorEnum_INTERNAL_ERROR, fmt.Errorf("failed to get memory info: %v", err))
	}

	physicalMemory := memInfo.RSS
	usage := physicalMemory / 1024 / 1024
	limit = uint64(limitInt64) / 1024 / 1024
	available = limit - usage

	return available, limit, nil
}

func ReadCgroupTotalMemory() (limit int64, err error) {
	memoryLimitPath := "/sys/fs/cgroup/memory/memory.limit_in_bytes"

	data, err := os.ReadFile(memoryLimitPath)
	if err != nil {
		return limit, vearchpb.NewError(vearchpb.ErrorEnum_INTERNAL_ERROR, fmt.Errorf("failed to read file: %v", err))
	}

	limit, err = strconv.ParseInt(strings.TrimSpace(string(data)), 10, 64)
	if err != nil {
		return limit, vearchpb.NewError(vearchpb.ErrorEnum_INTERNAL_ERROR, fmt.Errorf("failed to convert value to int64: %v", err))
	}

	return limit, nil
}

func ReadProcMemory() (available, total uint64, err error) {
	file, err := os.Open("/proc/meminfo")
	if err != nil {
		return 0, 0, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "MemAvailable:") {
			fields := strings.Fields(line)
			if len(fields) < 2 {
				return 0, 0, fmt.Errorf("unexpected format in /proc/meminfo")
			}
			availableKBytes, err := strconv.ParseUint(fields[1], 10, 64)
			if err != nil {
				return 0, 0, err
			}
			available = availableKBytes * 1024
		}
		if strings.HasPrefix(line, "MemTotal:") {
			fields := strings.Fields(line)
			if len(fields) < 2 {
				return 0, 0, fmt.Errorf("unexpected format in /proc/meminfo")
			}
			totalKBytes, err := strconv.ParseUint(fields[1], 10, 64)
			if err != nil {
				return 0, 0, err
			}
			total = totalKBytes * 1024
		}
	}

	if err := scanner.Err(); err != nil {
		return 0, 0, err
	}
	return available, total, nil
}

func GetSystemMemory() (total uint64, err error) {
	_, total, err = ReadProcMemory()
	if err != nil {
		return 0, err
	}

	CgroupMemoryLimit, err := ReadCgroupTotalMemory()

	if err != nil {
		return 0, err
	} else if uint64(CgroupMemoryLimit) < total {
		total = uint64(CgroupMemoryLimit)
	}
	return total, nil
}

func GetMemoryUsage() (memoryUsage uint64, err error) {
	pid := os.Getpid()
	p, err := process.NewProcess(int32(pid))
	if err != nil {
		return 0, fmt.Errorf("failed to get process: %v", err)
	}

	memInfo, err := p.MemoryInfo()
	if err != nil {
		return 0, fmt.Errorf("failed to get memory info: %v", err)
	}

	memoryUsage = memInfo.RSS

	return memoryUsage, nil
}
