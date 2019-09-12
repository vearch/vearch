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

package vearchlog

import (
	"fmt"
	"os"
)

func NewVearchLog(dir, module, level string, toConsole bool) *vearchLog {
	var l loggingT
	l.dir = dir
	l.module = module
	l.alsoToStderr = toConsole
	var ok bool
	if l.outputLevel, ok = severityByName(level); !ok {
		panic("Unknown output log level")
	}
	ToInit(&l)
	return &vearchLog{l: &l}
}

type vearchLog struct {
	l *loggingT
}

func (l *vearchLog) IsDebugEnabled() bool {
	return l.l.outputLevel == debugLog
}

func (l *vearchLog) IsInfoEnabled() bool {
	return int(l.l.outputLevel) <= INFO
}

func (l *vearchLog) IsWarnEnabled() bool {
	return int(l.l.outputLevel) <= WARN
}

func (l *vearchLog) Error(format string, arg ...interface{}) {
	if errorLog >= l.l.outputLevel {
		l.l.printDepth(errorLog, 1, format, arg...)
	}
}

func (l *vearchLog) Info(format string, arg ...interface{}) {
	if infoLog >= l.l.outputLevel {
		l.l.printDepth(infoLog, 1, format, arg...)
	}
}

func (l *vearchLog) Debug(format string, arg ...interface{}) {
	if debugLog >= l.l.outputLevel {
		l.l.printDepth(debugLog, 1, format, arg...)
	}
}

func (l *vearchLog) Warn(format string, arg ...interface{}) {
	if warningLog >= l.l.outputLevel {
		l.l.printDepth(warningLog, 1, format, arg...)
	}
}

func (l *vearchLog) Panic(format string, v ...interface{}) {
	if errorLog >= l.l.outputLevel {
		l.l.printDepth(errorLog, 1, format, v...)
	}
	panic(fmt.Sprintf(format, v...))
}

func (l *vearchLog) Fault(format string, v ...interface{}) {
	if errorLog >= l.l.outputLevel {
		l.l.printDepth(errorLog, 1, format, v...)
	}
	os.Exit(-1)
}

func (l *vearchLog) Flush() {
	l.l.lockAndFlushAll()
}
