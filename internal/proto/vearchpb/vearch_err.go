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

package vearchpb

import (
	"fmt"
	"strings"

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
	if strings.HasPrefix(err.Error(), ErrMsg(code)) {
		vErr = &VearchErr{error: &Error{Code: code, Msg: err.Error()}}
		return
	} else {
		vErr = &VearchErr{error: &Error{Code: code, Msg: ErrMsg(code) + ":" + err.Error()}}
		return
	}
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
