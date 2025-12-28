// Copyright 2025 The Vearch Authors.
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

package examples

import (
	"fmt"

	"github.com/gin-gonic/gin"
	verrors "github.com/vearch/vearch/v3/internal/pkg/errors"
	"github.com/vearch/vearch/v3/internal/router/document"
)

// 示例 1: 参数验证错误
func ExampleParameterValidation(c *gin.Context) {
	spaceName := c.Param("space")
	
	// 旧方式（不推荐）
	// if spaceName == "" {
	//     c.JSON(400, gin.H{"error": "space name is required"})
	//     return
	// }
	
	// 新方式（推荐）
	if err := document.ValidateSpaceName(spaceName); err != nil {
		document.HandleError(c, err)
		return
	}
	
	// 继续处理...
	document.HandleSuccess(c, gin.H{"space": spaceName})
}

// 示例 2: 资源未找到
func ExampleResourceNotFound(c *gin.Context) {
	spaceName := c.Param("space")
	
	// 模拟获取 space
	space := getSpace(spaceName)
	
	// 旧方式（不推荐）
	// if space == nil {
	//     c.JSON(404, gin.H{"error": "space not found"})
	//     return
	// }
	
	// 新方式（推荐）
	if space == nil {
		document.HandleError(c, verrors.SpaceNotFound(spaceName))
		return
	}
	
	document.HandleSuccess(c, space)
}

// 示例 3: 包装 RPC 错误
func ExampleRPCError(c *gin.Context) {
	// 模拟 RPC 调用
	result, err := performRPCCall()
	
	// 旧方式（不推荐）
	// if err != nil {
	//     c.JSON(500, gin.H{"error": err.Error()})
	//     return
	// }
	
	// 新方式（推荐）
	if err != nil {
		document.HandleError(c, verrors.RPCError("search", err))
		return
	}
	
	document.HandleSuccess(c, result)
}

// 示例 4: 带详情的错误
func ExampleErrorWithDetails(c *gin.Context) {
	vectorDim := 64
	expectedDim := 128
	
	// 旧方式（不推荐）
	// if vectorDim != expectedDim {
	//     c.JSON(400, gin.H{
	//         "error": "invalid vector dimension",
	//         "expected": expectedDim,
	//         "actual": vectorDim,
	//     })
	//     return
	// }
	
	// 新方式（推荐）
	if vectorDim != expectedDim {
		err := verrors.InvalidParam("vector_dimension")
		err.WithDetail("expected", expectedDim)
		err.WithDetail("actual", vectorDim)
		err.WithDetail("field", "embeddings")
		document.HandleError(c, err)
		return
	}
	
	document.HandleSuccess(c, nil)
}

// 示例 5: 检查错误类型
func ExampleErrorTypeCheck() {
	err := performOperation()
	
	// 检查是否可重试
	if verrors.IsRetryable(err) {
		fmt.Println("Error is retryable, implementing retry logic...")
		// 实现重试逻辑
	}
	
	// 获取错误码
	code := verrors.GetCode(err)
	fmt.Printf("Error code: %d (%s)\n", code, code.String())
	
	// 获取 HTTP 状态码
	status := verrors.HTTPStatus(err)
	fmt.Printf("HTTP status: %d\n", status)
	
	// 提取 VearchError
	if vErr := verrors.GetVearchError(err); vErr != nil {
		fmt.Printf("Error details: %v\n", vErr.Details)
	}
}

// 示例 6: 在 Service 层创建错误
func ExampleServiceLayerError() error {
	// 验证输入
	if len(documents) == 0 {
		return verrors.MissingParam("documents")
	}
	
	// 检查资源
	space, err := getSpaceFromMaster(spaceName)
	if err != nil {
		// 包装错误
		return verrors.Wrap(verrors.ErrMasterUnavailable, "failed to get space", err)
	}
	
	if space == nil {
		return verrors.SpaceNotFound(spaceName)
	}
	
	// 执行操作
	if err := performOperation(space, documents); err != nil {
		return verrors.StorageError("bulk insert", err)
	}
	
	return nil
}

// 示例 7: 错误响应格式
func ExampleErrorResponse() {
	// 错误响应会自动格式化为:
	// {
	//     "code": 3002,                           // HTTP 状态码
	//     "error": "SPACE_NOT_FOUND",            // 错误码字符串
	//     "message": "space not found: my_space", // 错误消息
	//     "details": {                            // 可选的详情
	//         "space_name": "my_space",
	//         "db_name": "my_db"
	//     }
	// }
}

// 模拟函数
func getSpace(name string) interface{} {
	return nil
}

func getSpaceFromMaster(name string) (interface{}, error) {
	return nil, nil
}

func performRPCCall() (interface{}, error) {
	return nil, nil
}

func performOperation() error {
	return verrors.Timeout("search")
}

var documents []interface{}
var spaceName string
