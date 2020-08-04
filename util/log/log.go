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
