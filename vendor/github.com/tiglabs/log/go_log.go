// Copyright 2018 The Chubao Authors.
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

package log

import (
	"fmt"
	golog "log"
	"os"
)

type Level int8

const (
	DEBUG Level = iota
	INFO
	WARN
	ERROR

	debug = "[DEBUG] "
	info  = "[INFO] "
	warn  = "[WARN] "
	err   = "[ERROR] "
)

var std Log = &GoLog{Logger: golog.New(os.Stderr, "", golog.Lshortfile|golog.LstdFlags), L: DEBUG, LevelCode: debug}

type GoLog struct {
	*golog.Logger
	L         Level
	LevelCode string
}

func (this *GoLog) Flush() {
}

func NewGoLog(lg *golog.Logger, l Level) Log {
	return &GoLog{Logger: lg, L: l}
}

func (this *GoLog) IsDebugEnabled() bool {
	return this.L == 0
}

func (this *GoLog) IsInfoEnabled() bool {
	return this.L <= 1
}

func (this *GoLog) IsWarnEnabled() bool {
	return this.L <= 2
}

func (this *GoLog) Debug(format string, args ...interface{}) {
	if this.IsDebugEnabled() {
		this.LevelCode = debug
		this.write(format, args...)
	}
}

func (this *GoLog) Info(format string, args ...interface{}) {
	if this.IsInfoEnabled() {
		this.LevelCode = info
		this.write(format, args...)
	}
}

func (this *GoLog) Warn(format string, args ...interface{}) {
	if this.IsWarnEnabled() {
		this.LevelCode = warn
		this.write(format, args...)
	}
}

func (this *GoLog) Error(format string, args ...interface{}) {
	this.LevelCode = err
	this.write(format, args...)
}

func (this *GoLog) write(format string, args ...interface{}) {
	if len(args) == 0 {
		_ = this.Output(4, this.LevelCode+format)
	} else {
		_ = this.Output(4, fmt.Sprintf(this.LevelCode+format, args...))
	}

}
