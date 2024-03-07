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

func (this *GoLog) Debugf(format string, args ...interface{}) {
	if this.IsDebugEnabled() {
		this.LevelCode = debug
		this.write(format, args...)
	}
}

func (this *GoLog) Infof(format string, args ...interface{}) {
	if this.IsInfoEnabled() {
		this.LevelCode = info
		this.write(format, args...)
	}
}

func (this *GoLog) Warnf(format string, args ...interface{}) {
	if this.IsWarnEnabled() {
		this.LevelCode = warn
		this.write(format, args...)
	}
}

func (this *GoLog) Errorf(format string, args ...interface{}) {
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

func (this *GoLog) Fatalf(format string, args ...interface{}) {
	this.LevelCode = err
	this.write(format, args...)
}
func (this *GoLog) Panicf(format string, args ...interface{}) {
	this.LevelCode = err
	this.write(format, args...)
}
func (this *GoLog) Error(args ...interface{}) {
	this.LevelCode = err
	if len(args) <= 1 {
		this.write(fmt.Sprint(args...))
	} else {
		format := args[0].(string)
		this.write(format, args[1:]...)
	}
}
func (this *GoLog) Warn(args ...interface{}) {
	this.LevelCode = err
	if len(args) <= 1 {
		this.write(fmt.Sprint(args...))
	} else {
		format := args[0].(string)
		this.write(format, args[1:]...)
	}
}
func (this *GoLog) Info(args ...interface{}) {
	this.LevelCode = err
	if len(args) <= 1 {
		this.write(fmt.Sprint(args...))
	} else {
		format := args[0].(string)
		this.write(format, args[1:]...)
	}
}
func (this *GoLog) Debug(args ...interface{}) {
	this.LevelCode = err
	if len(args) <= 1 {
		this.write(fmt.Sprint(args...))
	} else {
		format := args[0].(string)
		this.write(format, args[1:]...)
	}
}

func (this *GoLog) Fatal(args ...interface{}) {
	this.LevelCode = err
	if len(args) <= 1 {
		this.write(fmt.Sprint(args...))
	} else {
		format := args[0].(string)
		this.write(format, args[1:]...)
	}
}

func (this *GoLog) Panic(args ...interface{}) {
	this.LevelCode = err
	if len(args) <= 1 {
		this.write(fmt.Sprint(args...))
	} else {
		format := args[0].(string)
		this.write(format, args[1:]...)
	}
}
