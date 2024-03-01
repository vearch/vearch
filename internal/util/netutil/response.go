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

package netutil

import (
	"net/http"
	"strconv"

	"github.com/bytedance/sonic"
	"github.com/vearch/vearch/internal/proto/vearchpb"
	"github.com/vearch/vearch/internal/util/log"
)

type Response struct {
	respWriter  http.ResponseWriter
	httpStatus  int
	allowMethod string
}

func NewResponse(respWriter http.ResponseWriter) *Response {
	return &Response{
		respWriter: respWriter,
		httpStatus: http.StatusOK,
	}
}

func (this *Response) SetHttpStatus(httpStatus int) *Response {
	this.httpStatus = httpStatus
	return this
}

func (this *Response) SetAllowMethod(method string) *Response {
	this.allowMethod = method
	return this
}

func (this *Response) SendText(text string) {
	this.respWriter.Header().Set("Content-Type", "text/plain; charset=UTF-8")
	this.respWriter.Header().Set("Content-Length", strconv.Itoa(len(text)))
	if this.allowMethod != "" {
		/*
			< HTTP/1.1 405 Method Not Allowed
			< Allow: POST
		*/
		this.respWriter.Header().Set("Allow", this.allowMethod)
	}
	this.respWriter.WriteHeader(this.httpStatus)

	if _, err := this.respWriter.Write([]byte(text)); err != nil {
		log.Error("fail to write http reply[%s] len[%d]. err:[%v]", string(text), len(text), err)
	}
}

func (this *Response) SendJsonBytes(bytes []byte) {
	this.respWriter.Header().Set("Content-Type", "application/json; charset=UTF-8")
	this.respWriter.Header().Set("Content-Length", strconv.Itoa(len(bytes)))
	if this.allowMethod != "" {
		/*
			< HTTP/1.1 405 Method Not Allowed
			< Allow: POST
		*/
		this.respWriter.Header().Set("Allow", this.allowMethod)
	}
	this.respWriter.WriteHeader(this.httpStatus)

	if _, err := this.respWriter.Write(bytes); err != nil {
		log.Error("fail to write http reply, err:[%v], body:[%s] len[%d]", err, string(bytes), len(bytes))
	}
}

func (this *Response) SendJson(data interface{}) {
	reply, err := sonic.Marshal(data)
	if err != nil {
		this.SetHttpStatus(http.StatusInternalServerError).SendText(err.Error())
		return
	}

	this.SendJsonBytes(reply)
}

func (this *Response) SendJsonHttpReplySuccess(data interface{}) {
	httpReply := &HttpReply{
		Code: int64(vearchpb.ErrCode(vearchpb.ErrorEnum_SUCCESS)),
		Msg:  vearchpb.ErrMsg(vearchpb.ErrorEnum_SUCCESS),
		Data: data,
	}
	this.SendJson(httpReply)
}

func (this *Response) SendJsonHttpReplyError(err error) {
	if err == nil {
		this.SetHttpStatus(http.StatusInternalServerError).SendText("SendJsonHttpReplyError param err is nil")
		return
	}

	httpReply := &HttpReply{
		Code: int64(vearchpb.ErrCode(vearchpb.NewError(vearchpb.ErrorEnum_INTERNAL_ERROR, err).GetError().Code)),
		Msg:  err.Error(),
	}

	this.SendJson(httpReply)
}
