package errutil

import (
	"fmt"
	"runtime/debug"

	"github.com/pkg/errors"
	"github.com/vearch/vearch/internal/pkg/log"
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
		tempErr := fmt.Errorf("CatchError is %v", info)
		err = &tempErr
	}
}
