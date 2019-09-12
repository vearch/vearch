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

package resp

import (
	"context"
	"fmt"
	"github.com/tiglabs/log"
	"github.com/vearch/vearch/util/monitoring"
	"github.com/vearch/vearch/util/netutil"
	"github.com/vearch/vearch/util/reflect"
	"net/http"
	"runtime/debug"
	"time"
)

func SendError(ctx context.Context, w http.ResponseWriter, httpStatus int, errorMsg string, monitor monitoring.Monitor) {
	if log.IsDebugEnabled() {
		fmt.Println(string(debug.Stack()))
	}

	netutil.NewResponse(w).SetHttpStatus(httpStatus).SendJson(NewBody(errorMsg, httpStatus))

	if monitor != nil {
		if value := ctx.Value(netutil.StartTime); value != nil {
			monitor.New(reflect.RuntimeMethodName(2)).FunctionTP(value.(time.Time), true)
		}
	}
}

func SendErrorMethodNotAllowed(ctx context.Context, w http.ResponseWriter, url string, method string, allowMethod string, monitor monitoring.Monitor) {
	err := fmt.Errorf(ErrReasonIncorrectHttpMethod, url, method, allowMethod)
	netutil.NewResponse(w).SetHttpStatus(http.StatusMethodNotAllowed).SetAllowMethod(http.MethodPost).SendJson(NewBody(err.Error(), http.StatusMethodNotAllowed))

	if monitor != nil {
		if value := ctx.Value(netutil.StartTime); value != nil {
			monitor.New(reflect.RuntimeMethodName(2)).FunctionTP(value.(time.Time), true)
		}
	}
}

func SendErrorRootCause(ctx context.Context, w http.ResponseWriter, httpStatus int, errorType string, errorReason string, monitor monitoring.Monitor) {
	if log.IsDebugEnabled() {
		fmt.Println(string(debug.Stack()))
	}
	netutil.NewResponse(w).SetHttpStatus(httpStatus).SendJson(NewBodyRootCause(errorType, errorReason, httpStatus))

	if monitor != nil {
		if value := ctx.Value(netutil.StartTime); value != nil {
			monitor.New(reflect.RuntimeMethodName(2)).FunctionTP(value.(time.Time), true)
		}
	}
}

func SendJsonBytes(ctx context.Context, w http.ResponseWriter, bytes []byte, monitor monitoring.Monitor) {
	netutil.NewResponse(w).SetHttpStatus(http.StatusOK).SendJsonBytes(bytes)
	if monitor != nil {
		if value := ctx.Value(netutil.StartTime); value != nil {
			monitor.New(reflect.RuntimeMethodName(2)).FunctionTP(value.(time.Time), false)
		}
	}
}

func SendJson(ctx context.Context, w http.ResponseWriter, obj interface{}, monitor monitoring.Monitor) {
	netutil.NewResponse(w).SetHttpStatus(http.StatusOK).SendJson(obj)
	if monitor != nil {
		if value := ctx.Value(netutil.StartTime); value != nil {
			monitor.New(reflect.RuntimeMethodName(2)).FunctionTP(value.(time.Time), false)
		}
	}
}

func SendJsonHttpReplySuccess(ctx context.Context, w http.ResponseWriter, obj interface{}, monitor monitoring.Monitor) {
	netutil.NewResponse(w).SendJsonHttpReplySuccess(obj)
	if monitor != nil {
		if value := ctx.Value(netutil.StartTime); value != nil {
			monitor.New(reflect.RuntimeMethodName(2)).FunctionTP(value.(time.Time), false)
		}
	}
}
