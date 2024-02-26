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

package ginutil

import (
	"fmt"
	"net/http"
	"strconv"

	"github.com/bytedance/sonic"
	"github.com/gin-gonic/gin"
	"github.com/vearch/vearch/proto/vearchpb"
	"github.com/vearch/vearch/util/log"
	"github.com/vearch/vearch/util/netutil"
)

type Response struct {
	ginContext *gin.Context
	httpStatus int64
}

func New(ginContext *gin.Context) *Response {
	return &Response{
		ginContext: ginContext,
		httpStatus: http.StatusOK,
	}
}

func NewAutoMehtodName(ginContext *gin.Context) *Response {
	response := &Response{
		ginContext: ginContext,
		httpStatus: http.StatusOK,
	}
	return response
}

/*
default status is 200
*/
func (r *Response) SetHttpStatus(httpStatus int64) *Response {
	r.httpStatus = httpStatus
	return r
}

func (r *Response) SendJson(data interface{}) {
	reply, err := sonic.Marshal(data)
	if err != nil {
		r.SendJsonHttpReplyError(err)
		return
	}
	r.ginContext.Data(int(r.httpStatus), "application/json", reply)
}

func (r *Response) SendJsonBytes(bytes []byte) {
	r.ginContext.Header("Content-Type", "application/json; charset=UTF-8")
	r.ginContext.Header("Content-Length", strconv.Itoa(len(bytes)))
	r.ginContext.Writer.WriteHeader(int(r.httpStatus))

	if _, err := r.ginContext.Writer.Write(bytes); err != nil {
		log.Errorf("fail to write http reply, err:[%v], body:[%s] len[%d]", err, string(bytes), len(bytes))
	}
}

func (r *Response) SendJsonHttpReplySuccess(data interface{}) {
	httpReply := &netutil.HttpReply{
		Code: int64(vearchpb.ErrCode(vearchpb.ErrorEnum_SUCCESS)),
		Msg:  vearchpb.ErrMsg(vearchpb.ErrorEnum_SUCCESS),
		Data: data,
	}
	r.SetHttpStatus(httpReply.Code)
	r.SendJson(httpReply)
}

func (r *Response) SendJsonHttpReplyError(err error) {
	if err == nil {
		err = fmt.Errorf("")
	}

	httpReply := &netutil.HttpReply{
		Code: int64(vearchpb.ErrCode(vearchpb.NewError(vearchpb.ErrorEnum_INTERNAL_ERROR, err).GetError().Code)),
		Msg:  err.Error(),
	}
	r.SetHttpStatus(httpReply.Code)
	r.SendJson(httpReply)
}
