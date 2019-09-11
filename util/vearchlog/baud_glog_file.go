// Go support for leveled logs, analogous to https://code.google.com/p/google-glog/
//
// Copyright 2013 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// File I/O for logs.

package vearchlog

import (
	"errors"
	"fmt"
	"os"
	"os/user"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const defaultRotateSize = 1024 * 1024 * 100

func createLogDirs(dir string) {
	if dir == "" {
		dir = os.TempDir()
	}

	fInfo, err := os.Stat(dir)
	if (err != nil && !os.IsNotExist(err)) || (err == nil && !fInfo.IsDir()) {
		fmt.Printf("log dir[%s] had already exists or not is a dir", dir)
		return
	}
	if err != nil && os.IsNotExist(err) {
		if err = os.MkdirAll(dir, os.ModePerm); err != nil {
			fmt.Printf("Create log dir[%s] err: [%s]\r\n", dir, err)
		}
	}
}

var (
	pid      = os.Getpid()
	program  = filepath.Base(os.Args[0])
	host     = "unknownhost"
	userName = "unknownuser"
)

func init() {
	h, err := os.Hostname()
	if err == nil {
		host = shortHostname(h)
	}

	current, err := user.Current()
	if err == nil {
		userName = current.Username
	}

	// Sanitize userName since it may contain filepath separators on Windows.
	userName = strings.Replace(userName, `\`, "_", -1)
}

// shortHostname returns its argument, truncating at the first period.
// For instance, given "www.google.com" it returns "www".
func shortHostname(hostname string) string {
	if i := strings.Index(hostname, "."); i >= 0 {
		return hostname[:i]
	}
	return hostname
}

// logName returns a new log file name containing tag, with start time t, and
// the name for the symlink for tag.
func lastLogName(prefix, tag string, t time.Time) (name string) {
	name = fmt.Sprintf("%s.%s.log.%04d%02d%02d-%02d%02d%02d",
		prefix,
		tag,
		t.Year(),
		t.Month(),
		t.Day(),
		t.Hour(),
		t.Minute(),
		t.Second())
	return name
}

func appendLogName(prefix, tag string) string {
	return fmt.Sprintf("%s.%s.log", prefix, tag)
}

var onceLogDirs sync.Once

// create creates a new log file and returns the file and its filename, which
// contains tag ("INFO", "FATAL", etc.) and t.  If the file is created
// successfully, create also attempts to update the symlink for that tag, ignoring
// errors.
func create(dir, prefix, tag string) (*os.File, int64, error) {
	onceLogDirs.Do(func() {
		createLogDirs(dir)
	})

	fName := appendLogName(prefix, tag)
	fPath := filepath.Join(dir, fName)

	fOpt := os.O_RDWR | os.O_CREATE | os.O_APPEND
	f, err := os.OpenFile(fPath, fOpt, os.ModePerm)
	if err != nil {
		return nil, 0, errors.New(fmt.Sprintf("open log file[%s] err[%v]", fPath, err))
	}

	fInfo, err := f.Stat()
	if err != nil {
		return nil, 0, errors.New(fmt.Sprintf("stat log file[%s] err[%v]", fPath, err))
	}

	return f, fInfo.Size(), nil
}

func renameAndCreate(dir, prefix, tag string, t time.Time) (*os.File, error) {
	fOldName := appendLogName(prefix, tag)
	// TODO: it's better that naming last log name using file create time
	fNewName := lastLogName(prefix, tag, t)

	fOldPath := filepath.Join(dir, fOldName)
	fNewPath := filepath.Join(dir, fNewName)
	if err := os.Rename(fOldPath, fNewPath); err != nil {
		return nil, errors.New(fmt.Sprintf("rename log file err[%v]", err))
	}

	f, _, err := create(dir, prefix, tag)
	return f, err
}
