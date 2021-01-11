package errutil

import (
	"fmt"
	"github.com/pkg/errors"
	"github.com/tiglabs/raft/logger"
	"github.com/vearch/vearch/util/log"
	"runtime/debug"
)

// throw error panic
func ThrowError(err error) {
	if err != nil {
		panic(errors.WithStack(err))
	}
}

// catch error
func CatchError(err *error) {
	if info := recover(); info != nil {
		if log.IsDebugEnabled() {
			debug.PrintStack()
		}
		*err = fmt.Errorf("CatchError is %v",info)
		logger.Debug("CatchError is %v",*err)
	}
}
