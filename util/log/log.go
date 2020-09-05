package log

import (
	"fmt"
	"math"
)

type Log interface {
	IsDebugEnabled() bool

	IsInfoEnabled() bool

	IsWarnEnabled() bool

	Debug(v ...interface{})
	Debugf(format string, v ...interface{})

	Info(v ...interface{})
	Infof(format string, v ...interface{})

	Warn(v ...interface{})
	Warnf(format string, v ...interface{})

	Error(v ...interface{})
	Errorf(format string, v ...interface{})

	Fatal(v ...interface{})
	Fatalf(format string, v ...interface{})

	Panic(v ...interface{})
	Panicf(format string, v ...interface{})

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

func Errorf(format string, args ...interface{}) {
	Get().Errorf(format, args...)
}

func Infof(format string, args ...interface{}) {
	Get().Infof(format, args...)
}

func Debugf(format string, args ...interface{}) {
	Get().Debugf(format, args...)
}

func Warnf(format string, args ...interface{}) {
	Get().Warnf(format, args...)
}

func Fatalf(format string, args ...interface{}) {
	Get().Fatalf(format, args...)
}
func Panicf(format string, args ...interface{}) {
	Get().Panicf(format, args...)
}

func Error(args ...interface{}) {
	Get().Error(args...)
}
func Warn(args ...interface{}) {
	Get().Warn(args...)
}
func Info(args ...interface{}) {
	Get().Info(args...)
}
func Debug(args ...interface{}) {
	Get().Debug(args...)
}

func Fatal(args ...interface{}) {
	Get().Fatal(args...)
}

func Panic(args ...interface{}) {
	Get().Panic(args...)
}

func Flush() {
	Get().Flush()
}
