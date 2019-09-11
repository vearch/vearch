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
	"math"
)

type Log interface {

	IsDebugEnabled() bool

	IsInfoEnabled() bool

	IsWarnEnabled() bool

	Error(format string, args ...interface{})

	Info(format string, args ...interface{})

	Debug(format string, args ...interface{})

	Warn(format string, args ...interface{})

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

func IsInfoEnabled() bool {
	return Get().IsInfoEnabled()
}

func IsWarnEnabled() bool {
	return Get().IsWarnEnabled()
}

func Error(format string, args ...interface{}) {
	Get().Error(format, args...)
}

func Info(format string, args ...interface{}) {
	Get().Info(format, args...)
}

func Debug(format string, args ...interface{}) {
	Get().Debug(format, args...)
}

func Warn(format string, args ...interface{}) {
	Get().Warn(format, args...)
}

func Flush() {
	Get().Flush()
}
