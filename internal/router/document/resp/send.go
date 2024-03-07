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
	"github.com/gin-gonic/gin"
	"github.com/vearch/vearch/internal/pkg/ginutil"
)

func SendError(c *gin.Context, httpStatus int, errorMsg string) {
	ginutil.NewAutoMehtodName(c).SetHttpStatus(int64(httpStatus)).SendJson(NewBody(errorMsg, httpStatus))
}

func SendErrorRootCause(c *gin.Context, httpStatus int, errorType string, errorReason string) {
	ginutil.NewAutoMehtodName(c).SetHttpStatus(int64(httpStatus)).SendJson(NewBodyRootCause(errorType, errorReason, httpStatus))
}

func SendJsonBytes(c *gin.Context, bytes []byte) {
	ginutil.NewAutoMehtodName(c).SendJsonBytes(bytes)
}

func SendJson(c *gin.Context, obj interface{}) {
	ginutil.NewAutoMehtodName(c).SendJson(obj)
}
