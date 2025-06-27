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

package log

import (
	"fmt"
	golog "log"
	"os"
)

type Level int8

const (
	DEBUG Level = iota
	TRACE
	INFO
	WARN
	ERROR

	debug = "[DEBUG] "
	trace = "[TRACE] "
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

func (gl *GoLog) Flush() {
}

func NewGoLog(lg *golog.Logger, l Level) Log {
	return &GoLog{Logger: lg, L: l}
}

func (gl *GoLog) IsDebugEnabled() bool {
	return gl.L == 0
}

func (gl *GoLog) IsTraceEnabled() bool {
	return gl.L <= 1
}

func (gl *GoLog) IsInfoEnabled() bool {
	return gl.L <= 2
}

func (gl *GoLog) IsWarnEnabled() bool {
	return gl.L <= 3
}

func (gl *GoLog) Debugf(format string, args ...any) {
	if gl.IsDebugEnabled() {
		gl.LevelCode = debug
		gl.write(format, args...)
	}
}

func (gl *GoLog) Tracef(format string, args ...any) {
	if gl.IsTraceEnabled() {
		gl.LevelCode = trace
		gl.write(format, args...)
	}
}

func (gl *GoLog) Infof(format string, args ...any) {
	if gl.IsInfoEnabled() {
		gl.LevelCode = info
		gl.write(format, args...)
	}
}

func (gl *GoLog) Warnf(format string, args ...any) {
	if gl.IsWarnEnabled() {
		gl.LevelCode = warn
		gl.write(format, args...)
	}
}

func (gl *GoLog) Errorf(format string, args ...any) {
	gl.LevelCode = err
	gl.write(format, args...)
}

func (gl *GoLog) write(format string, args ...any) {
	if len(args) == 0 {
		_ = gl.Output(4, gl.LevelCode+format)
	} else {
		_ = gl.Output(4, fmt.Sprintf(gl.LevelCode+format, args...))
	}

}

func (gl *GoLog) Fatalf(format string, args ...any) {
	gl.LevelCode = err
	gl.write(format, args...)
}
func (gl *GoLog) Panicf(format string, args ...any) {
	gl.LevelCode = err
	gl.write(format, args...)
}
func (gl *GoLog) Error(args ...any) {
	gl.LevelCode = err
	if len(args) <= 1 {
		gl.write(fmt.Sprint(args...))
	} else {
		format := args[0].(string)
		gl.write(format, args[1:]...)
	}
}
func (gl *GoLog) Warn(args ...any) {
	gl.LevelCode = warn
	if len(args) <= 1 {
		gl.write(fmt.Sprint(args...))
	} else {
		format := args[0].(string)
		gl.write(format, args[1:]...)
	}
}
func (gl *GoLog) Info(args ...any) {
	gl.LevelCode = info
	if len(args) <= 1 {
		gl.write(fmt.Sprint(args...))
	} else {
		format := args[0].(string)
		gl.write(format, args[1:]...)
	}
}

func (gl *GoLog) Trace(args ...any) {
	gl.LevelCode = trace
	if len(args) <= 1 {
		gl.write(fmt.Sprint(args...))
	} else {
		format := args[0].(string)
		gl.write(format, args[1:]...)
	}
}

func (gl *GoLog) Debug(args ...any) {
	gl.LevelCode = debug
	if len(args) <= 1 {
		gl.write(fmt.Sprint(args...))
	} else {
		format := args[0].(string)
		gl.write(format, args[1:]...)
	}
}

func (gl *GoLog) Fatal(args ...any) {
	gl.LevelCode = err
	if len(args) <= 1 {
		gl.write(fmt.Sprint(args...))
	} else {
		format := args[0].(string)
		gl.write(format, args[1:]...)
	}
}

func (gl *GoLog) Panic(args ...any) {
	gl.LevelCode = err
	if len(args) <= 1 {
		gl.write(fmt.Sprint(args...))
	} else {
		format := args[0].(string)
		gl.write(format, args[1:]...)
	}
}
