package pkg

import (
	"fmt"
	"runtime/debug"
)

type VearchErr struct {
	error
	Msg   string
	Code  int64
	Stack string
}

func (v *VearchErr) Error() string {
	if v.error == nil {
		return ""
	}
	return v.error.Error()
}

func VErr(code int64) *VearchErr {
	return &VearchErr{
		error: CodeErr(code),
		Code:  code,
		Stack: string(debug.Stack()),
	}
}

func VErrStr(code int64, msg string, a ...interface{}) *VearchErr {
	return &VearchErr{
		error: CodeErr(code),
		Code:  code,
		Msg:   fmt.Sprintf(msg, a...),
		Stack: string(debug.Stack()),
	}
}

func vErrErr(code int64, err error) *VearchErr {
	return &VearchErr{
		error: err,
		Code:  code,
		Stack: string(debug.Stack()),
	}
}
