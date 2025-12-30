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

package response

import (
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/vearch/vearch/v3/internal/entity/errors"
	"github.com/vearch/vearch/v3/internal/pkg/log"
	"github.com/vearch/vearch/v3/internal/proto/vearchpb"
)

type Response struct {
	ginContext *gin.Context
	httpStatus int64
	httpReply  *HttpReply
}

// http protocol
type HttpReply struct {
	Code      int    `json:"code"`
	RequestId string `json:"request_id,omitempty"`
	Msg       string `json:"msg,omitempty"`
	Data      any    `json:"data,omitempty"`
}

func New(ginContext *gin.Context) *Response {
	return &Response{
		ginContext: ginContext,
		httpStatus: http.StatusOK,
	}
}

// default status is 200
func (r *Response) SetHttpStatus(httpStatus int64) *Response {
	r.httpStatus = httpStatus
	return r
}

func (r *Response) GetHttpStatus() int64 {
	return r.httpStatus
}

func (r *Response) SetHttpReply(HttpReply *HttpReply) *Response {
	r.httpReply = HttpReply
	return r
}

func (r *Response) GetHttpReply() *HttpReply {
	return r.httpReply
}

func (r *Response) SendJson() {
	if r.ginContext.Request.Context().Err() != nil {
		log.Warn("attempted to write response to canceled context")
		return
	}

	if r.ginContext.Writer.Written() {
		log.Warn("response already committed, cannot write JSON")
		return
	}

	r.ginContext.Set("httpResponse", r)
}

func (r *Response) SendJsonBytes(bytes []byte) {
	r.ginContext.Header("Content-Type", "application/json; charset=UTF-8")
	r.ginContext.Header("Content-Length", strconv.Itoa(len(bytes)))
	r.ginContext.Writer.WriteHeader(int(r.httpStatus))

	if _, err := r.ginContext.Writer.Write(bytes); err != nil {
		log.Errorf("fail to write http reply, err:[%v], body:[%s] len[%d]", err, string(bytes), len(bytes))
	}
}

// response only called for handler which called after TimeoutMiddleware
func (r *Response) JsonSuccess(data any) int {
	httpReply := &HttpReply{
		Code:      int(vearchpb.ErrorEnum_SUCCESS),
		RequestId: r.ginContext.GetHeader("X-Request-Id"),
		Msg:       "",
		Data:      data,
	}
	r.SetHttpStatus(int64(http.StatusOK))
	r.SetHttpReply(httpReply)
	r.SendJson()
	return http.StatusOK
}

// response only called for handler which called after TimeoutMiddleware
func (r *Response) SuccessDelete() int {
	httpReply := &HttpReply{
		Code:      int(vearchpb.ErrorEnum_SUCCESS),
		RequestId: r.ginContext.GetHeader("X-Request-Id"),
		Msg:       "",
	}
	r.SetHttpStatus(int64(http.StatusOK))
	r.SetHttpReply(httpReply)
	r.SendJson()
	return http.StatusOK
}

// response only called for handler which called after TimeoutMiddleware
func (r *Response) JsonError(err *errors.ErrRequest) int {
	httpReply := &HttpReply{
		Code:      err.Code(),
		RequestId: r.ginContext.GetHeader("X-Request-Id"),
		Msg:       err.Msg(),
	}
	r.SetHttpStatus(int64(err.HttpCode()))
	r.SetHttpReply(httpReply)
	r.SendJson()
	return err.HttpCode()
}
