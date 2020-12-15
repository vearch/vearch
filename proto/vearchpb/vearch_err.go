package vearchpb

import (
	"fmt"
	strings "strings"

	"github.com/pkg/errors"
)

type VearchErr struct {
	error *Error
}

func (v *VearchErr) Error() string {
	if v.error == nil {
		return ""
	}
	if v.error.Msg != "" {
		return v.error.Msg
	}
	return ErrorEnum_name[int32(v.error.Code)]
}

func (v *VearchErr) GetError() *Error {
	return v.error
}

func NewError(code ErrorEnum, err error) (vErr *VearchErr) {
	if err == nil {
		vErr = &VearchErr{error: &Error{Code: code, Msg: ErrMsg(code)}}
		return
	}
	if vErr, ok := err.(*VearchErr); ok {
		return vErr
	}
	vErr = &VearchErr{error: &Error{Code: code, Msg: err.Error()}}
	return
}

func ErrCode(code ErrorEnum) (c int) {
	if code == ErrorEnum_SUCCESS {
		c = 200
	} else {
		c = int(code) + 549
	}
	return
}

func ErrMsg(code ErrorEnum) (s string) {
	return strings.ToLower(ErrorEnum_name[int32(code)])
}

func Wrap(err error, s string) error {
	if err == nil {
		return nil
	}
	if vErr, ok := err.(*VearchErr); ok {
		vErr.error.Msg = fmt.Sprintf("%s:%s", s, vErr.error.Msg)
		return vErr
	}
	return NewError(0, errors.Wrap(err, s))
}

func NewErrorInfo(code ErrorEnum, msg string) (vErr *VearchErr) {
	vErr = &VearchErr{error: &Error{Code: code, Msg: msg}}
	return
}
