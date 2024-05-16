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

func (l *vearchLog) IsTraceEnabled() bool {
	return int(l.l.outputLevel) <= TRACE
}

func (l *vearchLog) IsInfoEnabled() bool {
	return int(l.l.outputLevel) <= INFO
}

func (l *vearchLog) IsWarnEnabled() bool {
	return int(l.l.outputLevel) <= WARN
}

func (l *vearchLog) Errorf(format string, v ...interface{}) {
	if errorLog >= l.l.outputLevel {
		l.l.printDepth(errorLog, 1, format, v...)
	}
}

func (l *vearchLog) Infof(format string, v ...interface{}) {
	if infoLog >= l.l.outputLevel {
		l.l.printDepth(infoLog, 1, format, v...)
	}
}

func (l *vearchLog) Debugf(format string, v ...interface{}) {
	if debugLog >= l.l.outputLevel {
		l.l.printDepth(debugLog, 1, format, v...)
	}
}

func (l *vearchLog) Tracef(format string, v ...interface{}) {
	if traceLog >= l.l.outputLevel {
		l.l.printDepth(traceLog, 1, format, v...)
	}
}

func (l *vearchLog) Warnf(format string, v ...interface{}) {
	if warningLog >= l.l.outputLevel {
		l.l.printDepth(warningLog, 1, format, v...)
	}
}

func (l *vearchLog) Panicf(format string, v ...interface{}) {
	if panicLog >= l.l.outputLevel {
		l.l.printDepth(panicLog, 1, format, v...)
	}
	l.l.lockAndFlushAll()
	panic(fmt.Sprintf(format, v...))
}

func (l *vearchLog) Fatalf(format string, v ...interface{}) {
	if fatalLog >= l.l.outputLevel {
		l.l.printDepth(fatalLog, 1, format, v...)
	}
	os.Exit(-1)
}

func (l *vearchLog) Flush() {
	l.l.lockAndFlushAll()
}

func (l *vearchLog) Error(v ...interface{}) {
	format := ""
	if errorLog >= l.l.outputLevel {
		if len(v) <= 1 {
			format = fmt.Sprint(v...)
		} else {
			format = v[0].(string)
			format = fmt.Sprintf(format, v[1:]...)
		}
		l.l.printDepth(errorLog, 1, format)
	}
}

func (l *vearchLog) Info(v ...interface{}) {
	format := ""
	if infoLog >= l.l.outputLevel {
		if len(v) <= 1 {
			format = fmt.Sprint(v...)
		} else {
			format = v[0].(string)
			format = fmt.Sprintf(format, v[1:]...)
		}
		l.l.printDepth(infoLog, 1, format)
	}
}

func (l *vearchLog) Trace(v ...interface{}) {
	format := ""
	if traceLog >= l.l.outputLevel {
		if len(v) <= 1 {
			format = fmt.Sprint(v...)
		} else {
			format = v[0].(string)
			format = fmt.Sprintf(format, v[1:]...)
		}
		l.l.printDepth(traceLog, 1, format)
	}
}

func (l *vearchLog) Debug(v ...interface{}) {
	format := ""
	if debugLog >= l.l.outputLevel {
		if len(v) <= 1 {
			format = fmt.Sprint(v...)
		} else {
			format = v[0].(string)
			format = fmt.Sprintf(format, v[1:]...)
		}
		l.l.printDepth(debugLog, 1, format)
	}
}

func (l *vearchLog) Warn(v ...interface{}) {
	format := ""
	if warningLog >= l.l.outputLevel {
		if len(v) <= 1 {
			format = fmt.Sprint(v...)
		} else {
			format = v[0].(string)
			format = fmt.Sprintf(format, v[1:]...)
		}
		l.l.printDepth(warningLog, 1, format)
	}
}

func (l *vearchLog) Panic(v ...interface{}) {
	format := ""
	if panicLog >= l.l.outputLevel {
		if len(v) <= 1 {
			format = fmt.Sprint(v...)
		} else {
			format = v[0].(string)
			format = fmt.Sprintf(format, v[1:]...)
		}
		l.l.printDepth(panicLog, 1, format)
	}
	l.l.lockAndFlushAll()
	panic(fmt.Sprintf(format))
}

func (l *vearchLog) Fatal(v ...interface{}) {
	format := ""
	if errorLog >= l.l.outputLevel {
		if len(v) <= 1 {
			format = fmt.Sprint(v...)
		} else {
			format = v[0].(string)
			format = fmt.Sprintf(format, v[1:]...)
		}
		l.l.printDepth(errorLog, 1, format)
	}
	os.Exit(-1)
}
