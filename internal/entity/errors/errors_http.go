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

package errors

import (
	"fmt"
	"net/http"

	"github.com/vearch/vearch/internal/proto/vearchpb"
)

type ErrRequest struct {
	err      error
	msg      string
	code     int
	httpCode int
}

func (e ErrRequest) Msg() string {
	return e.msg
}

func (e ErrRequest) Code() int {
	return e.code
}

func (e ErrRequest) HttpCode() int {
	return e.httpCode
}

func NewErrBadRequest(err error) *ErrRequest {
	if vErr, ok := err.(*vearchpb.VearchErr); ok {
		return &ErrRequest{
			err:      fmt.Errorf(vErr.Error()),
			msg:      vErr.Error(),
			code:     int(vErr.GetError().Code),
			httpCode: http.StatusBadRequest,
		}
	}
	return &ErrRequest{
		err:      err,
		msg:      err.Error(),
		code:     int(vearchpb.ErrorEnum_INTERNAL_ERROR),
		httpCode: http.StatusBadRequest,
	}
}

func NewErrUnprocessable(err error) *ErrRequest {
	if vErr, ok := err.(*vearchpb.VearchErr); ok {
		return &ErrRequest{
			err:      fmt.Errorf(vErr.Error()),
			msg:      vErr.Error(),
			code:     int(vErr.GetError().Code),
			httpCode: http.StatusUnprocessableEntity,
		}
	}
	return &ErrRequest{
		err:      err,
		msg:      err.Error(),
		code:     int(vearchpb.ErrorEnum_INTERNAL_ERROR),
		httpCode: http.StatusUnprocessableEntity,
	}
}

func NewErrNotFound(err error) *ErrRequest {
	if vErr, ok := err.(*vearchpb.VearchErr); ok {
		return &ErrRequest{
			err:      fmt.Errorf(vErr.Error()),
			msg:      vErr.Error(),
			code:     int(vErr.GetError().Code),
			httpCode: http.StatusNotFound,
		}
	}
	return &ErrRequest{
		err:      err,
		msg:      err.Error(),
		code:     int(vearchpb.ErrorEnum_INTERNAL_ERROR),
		httpCode: http.StatusNotFound,
	}
}

func NewErrInternal(err error) *ErrRequest {
	if vErr, ok := err.(*vearchpb.VearchErr); ok {
		return &ErrRequest{
			err:      fmt.Errorf(vErr.Error()),
			msg:      vErr.Error(),
			code:     int(vErr.GetError().Code),
			httpCode: http.StatusInternalServerError,
		}
	}
	return &ErrRequest{
		err:      err,
		msg:      err.Error(),
		code:     int(vearchpb.ErrorEnum_INTERNAL_ERROR),
		httpCode: http.StatusInternalServerError,
	}
}
