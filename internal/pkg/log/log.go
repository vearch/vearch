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
	"math"
)

type Log interface {
	IsDebugEnabled() bool

	IsTraceEnabled() bool

	IsInfoEnabled() bool

	IsWarnEnabled() bool

	Debug(v ...any)
	Debugf(format string, v ...any)

	Trace(v ...any)
	Tracef(format string, v ...any)

	Info(v ...any)
	Infof(format string, v ...any)

	Warn(v ...any)
	Warnf(format string, v ...any)

	Error(v ...any)
	Errorf(format string, v ...any)

	Fatal(v ...any)
	Fatalf(format string, v ...any)

	Panic(v ...any)
	Panicf(format string, v ...any)

	//when system exit you should use it
	Flush()
}

var logs [math.MaxUint8]Log

func RemoveLogI(i uint8) {
	logs[i] = nil
}

func RegistLog(i uint8, l Log) error {
	if logs[i] != nil {
		return fmt.Errorf("log has already exists index %d", i)
	}
	logs[i] = l
	return nil
}

func GetrDef(i int) Log {
	if logs[i] != nil {
		return logs[i]
	}
	return Get()
}

func GetLog(i int) Log {
	return logs[i]
}

func Get() Log {
	if logs[0] == nil {
		return std
	}
	return logs[0]
}

func Regist(log Log) {
	RegistLog(0, log)
}

func IsDebugEnabled() bool {
	return Get().IsDebugEnabled()
}

func IsTraceEnabled() bool {
	return Get().IsTraceEnabled()
}

func IsInfoEnabled() bool {
	return Get().IsInfoEnabled()
}

func IsWarnEnabled() bool {
	return Get().IsWarnEnabled()
}

func Errorf(format string, args ...any) {
	Get().Errorf(format, args...)
}

func Infof(format string, args ...any) {
	Get().Infof(format, args...)
}

func Debugf(format string, args ...any) {
	Get().Debugf(format, args...)
}

func Tracef(format string, args ...any) {
	Get().Tracef(format, args...)
}

func Warnf(format string, args ...any) {
	Get().Warnf(format, args...)
}

func Fatalf(format string, args ...any) {
	Get().Fatalf(format, args...)
}
func Panicf(format string, args ...any) {
	Get().Panicf(format, args...)
}

func Error(args ...any) {
	Get().Error(args...)
}
func Warn(args ...any) {
	Get().Warn(args...)
}
func Info(args ...any) {
	Get().Info(args...)
}
func Debug(args ...any) {
	Get().Debug(args...)
}

func Trace(args ...any) {
	Get().Trace(args...)
}

func Fatal(args ...any) {
	Get().Fatal(args...)
}

func Panic(args ...any) {
	Get().Panic(args...)
}

func Flush() {
	Get().Flush()
}
