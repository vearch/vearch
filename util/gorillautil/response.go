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

package gorillautil

import (
	"context"
	"fmt"
	"github.com/vearch/vearch/router/document/resp"
	"net/http"

	"github.com/vearch/vearch/proto/vearchpb"
	"github.com/vearch/vearch/util/cbjson"
	"github.com/vearch/vearch/util/netutil"
)

type Response struct {
	w http.ResponseWriter
	httpStatus int64
	ctx context.Context
}
// create response
func NewAutoMehtodName(ctx context.Context, w http.ResponseWriter) *Response {
	response := &Response{
		w: w,
		httpStatus: http.StatusOK,
		ctx: ctx,
	}
	return response
}


// default status is 200
func (gr *Response) SetHttpStatus(httpStatus int64) *Response {
	gr.httpStatus = httpStatus
	return gr
}

// send json data to server
func (gr *Response) SendJson(data interface{}) {
	reply, err := cbjson.Marshal(data)
	if err != nil {
		gr.SendJsonHttpReplyError(err)
		return
	}
	resp.SendJsonBytes(gr.ctx, gr.w, reply)
}

// send json data with success code
func (gr *Response) SendJsonHttpReplySuccess(data interface{}) {
	httpReply := &netutil.HttpReply{
		Code: int64(vearchpb.ErrCode(vearchpb.ErrorEnum_SUCCESS)),
		Msg:  vearchpb.ErrMsg(vearchpb.ErrorEnum_SUCCESS),
		Data: data,
	}
	gr.SetHttpStatus(httpReply.Code)
	gr.SendJson(httpReply)
}

// send err data
func (gr *Response) SendJsonHttpReplyError(err error) {
	if err == nil {
		err = fmt.Errorf("")
	}

	httpReply := &netutil.HttpReply{
		Code: int64(vearchpb.ErrCode(vearchpb.NewError(vearchpb.ErrorEnum_INTERNAL_ERROR, err).GetError().Code)),
		Msg:  err.Error(),
	}
	gr.SetHttpStatus(httpReply.Code)
	gr.SendJson(httpReply)
}
