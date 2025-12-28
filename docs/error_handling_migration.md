# 统一错误处理迁移指南

## 概述

为了解决 Vearch 项目中错误处理不统一的问题，我们引入了统一的错误处理框架。该框架提供：

- **统一的错误码**: 跨所有组件的标准化错误代码
- **错误包装链**: 支持 Go 1.13+ 的 error wrapping
- **错误分类**: 区分临时错误、可重试错误、永久错误
- **HTTP 映射**: 自动映射错误码到 HTTP 状态码
- **结构化错误**: 支持添加上下文详情

## 包位置

```go
import verrors "github.com/vearch/vearch/v3/internal/pkg/errors"
```

## 核心组件

### 1. ErrorCode (错误码)

定义在 `internal/pkg/errors/code.go`:

```go
type ErrorCode int

const (
    // Client errors (1000-1999)
    ErrInvalidParam     ErrorCode = 1000
    ErrMissingParam     ErrorCode = 1001
    
    // Auth errors (2000-2999)
    ErrUnauthorized     ErrorCode = 2000
    
    // Not found errors (3000-3999)
    ErrNotFound         ErrorCode = 3000
    ErrSpaceNotFound    ErrorCode = 3002
    
    // Server errors (5000-5999)
    ErrInternal         ErrorCode = 5000
    ErrTimeout          ErrorCode = 5001
    // ...
)
```

错误码范围：
- **1000-1999**: 客户端错误（400）
- **2000-2999**: 认证授权错误（401/403）
- **3000-3999**: 资源未找到（404）
- **4000-4999**: 资源冲突（409）
- **5000-5999**: 服务器错误（500）
- **6000-6999**: 资源限制（429/503）
- **7000-7999**: 数据错误（500）

### 2. VearchError (错误类型)

定义在 `internal/pkg/errors/error.go`:

```go
type VearchError struct {
    Code    ErrorCode              // 错误码
    Message string                 // 可读的错误消息
    Cause   error                  // 底层错误（支持 error chain）
    Details map[string]interface{} // 额外的上下文信息
}
```

### 3. 辅助函数

定义在 `internal/pkg/errors/helper.go`，提供常用错误的快捷构造：

```go
verrors.InvalidParam("user_id")
verrors.SpaceNotFound("my_space")
verrors.Timeout("search operation")
verrors.Unauthorized("invalid token")
```

## 使用指南

### 创建新错误

#### 方法 1: 使用辅助函数（推荐）

```go
// 参数错误
err := verrors.InvalidParam("vector_dimension")

// 资源未找到
err := verrors.SpaceNotFound(spaceName)

// 超时
err := verrors.Timeout("search operation")

// 内部错误
err := verrors.Internal("unexpected database state")
```

#### 方法 2: 直接构造

```go
// 简单错误
err := verrors.New(verrors.ErrNotFound, "resource not found")

// 格式化消息
err := verrors.Newf(verrors.ErrInvalidParam, "invalid param: %s", paramName)
```

#### 方法 3: 包装现有错误

```go
// 包装标准错误
dbErr := someDatabase.Query()
err := verrors.Wrap(verrors.ErrStorageError, "database query failed", dbErr)

// 包装并格式化
err := verrors.Wrapf(verrors.ErrRPCError, rpcErr, "RPC call to %s failed", service)
```

### 添加错误详情

```go
err := verrors.InvalidParam("vector_dimension")
err.WithDetail("expected", 128)
err.WithDetail("actual", vectorDim)
err.WithDetail("field", "embeddings")
```

### 在 HTTP Handler 中使用

#### Before (不统一):

```go
func handleSearch(c *gin.Context) {
    space := getSpace()
    if space == nil {
        c.JSON(404, gin.H{"error": "space not found"})  // ❌ 不统一
        return
    }
    
    result, err := search(space)
    if err != nil {
        c.JSON(500, gin.H{"msg": err.Error()})  // ❌ 错误格式不一致
        return
    }
    
    c.JSON(200, result)
}
```

#### After (统一):

```go
import verrors "github.com/vearch/vearch/v3/internal/pkg/errors"
import "github.com/vearch/vearch/v3/internal/router/document"

func handleSearch(c *gin.Context) {
    // 验证参数
    spaceName := c.Param("space")
    if err := document.ValidateSpaceName(spaceName); err != nil {
        document.HandleError(c, err)  // ✅ 统一错误处理
        return
    }
    
    // 获取 Space
    space, err := getSpace(spaceName)
    if err != nil {
        document.HandleError(c, verrors.SpaceNotFound(spaceName))  // ✅ 语义化错误
        return
    }
    
    // 执行搜索
    result, err := search(space)
    if err != nil {
        // 包装 RPC 错误
        document.HandleError(c, verrors.RPCError("search", err))  // ✅ 包装底层错误
        return
    }
    
    document.HandleSuccess(c, result)  // ✅ 统一成功响应
}
```

### 错误响应格式

统一的错误响应格式：

```json
{
    "code": 404,
    "error": "SPACE_NOT_FOUND",
    "message": "space not found: my_space",
    "details": {
        "space_name": "my_space",
        "db_name": "my_db"
    }
}
```

### 检查错误类型

```go
// 检查是否为 VearchError
if vErr := verrors.GetVearchError(err); vErr != nil {
    fmt.Printf("Error code: %d\n", vErr.Code)
}

// 获取错误码
code := verrors.GetCode(err)

// 检查是否可重试
if verrors.IsRetryable(err) {
    // 实现重试逻辑
}

// 检查是否为临时错误
if verrors.IsTemporary(err) {
    // 处理临时错误
}

// 获取 HTTP 状态码
status := verrors.HTTPStatus(err)
```

## 迁移步骤

### Phase 1: Router 层（当前阶段）

1. **已完成**:
   - ✅ 创建 `internal/pkg/errors/` 包
   - ✅ 定义统一错误码和类型
   - ✅ 实现 `error_handler.go` 辅助函数
   - ✅ 添加单元测试

2. **进行中**:
   - 🔄 在新代码中使用统一错误处理
   - 🔄 逐步迁移 `internal/router/document/` 的错误处理

3. **待办**:
   - ⏳ 迁移所有 Router 层 HTTP handlers
   - ⏳ 更新相关单元测试

### Phase 2: Master 层

```go
// Before
func (s *MasterService) CreateSpace(space *entity.Space) error {
    if space == nil {
        return fmt.Errorf("space is nil")  // ❌
    }
    // ...
}

// After
import verrors "github.com/vearch/vearch/v3/internal/pkg/errors"

func (s *MasterService) CreateSpace(space *entity.Space) error {
    if space == nil {
        return verrors.InvalidParam("space")  // ✅
    }
    // ...
}
```

### Phase 3: PS 层

```go
// Before
func (s *PartitionService) Write(doc *Document) error {
    if err := s.engine.Index(doc); err != nil {
        return vearchpb.NewError(500, err)  // ❌ 使用 protobuf 错误
    }
}

// After
func (s *PartitionService) Write(doc *Document) error {
    if err := s.engine.Index(doc); err != nil {
        return verrors.StorageError("index", err)  // ✅ 统一错误
    }
}
```

### Phase 4: Client 层

保持向后兼容，提供错误转换：

```go
// 在 client 包中提供转换函数
func (c *Client) Search(req *Request) (*Response, error) {
    resp, err := c.doRPC(req)
    if err != nil {
        // 转换为统一错误
        return nil, verrors.RPCError("search", err)
    }
    return resp, nil
}
```

## 最佳实践

### 1. 选择合适的错误码

```go
// ✅ Good: 使用语义化的错误码
if user == nil {
    return verrors.UserNotFound(userID)
}

// ❌ Bad: 使用通用错误码
if user == nil {
    return verrors.NotFound("user")  // 不够具体
}
```

### 2. 保留错误链

```go
// ✅ Good: 包装错误保留上下文
result, err := db.Query(sql)
if err != nil {
    return verrors.Wrap(verrors.ErrStorageError, "query failed", err)
}

// ❌ Bad: 丢失原始错误
if err != nil {
    return verrors.Internal("query failed")  // 丢失了 err 的信息
}
```

### 3. 添加有用的上下文

```go
// ✅ Good: 添加调试信息
err := verrors.InvalidParam("vector_dimension")
err.WithDetail("expected", expectedDim)
err.WithDetail("actual", actualDim)
err.WithDetail("space", spaceName)

// ❌ Bad: 没有上下文
return verrors.InvalidParam("vector_dimension")
```

### 4. 日志记录

```go
// ✅ Good: 在统一的地方记录日志
func HandleError(c *gin.Context, err error) {
    vErr := verrors.GetVearchError(err)
    if vErr.IsRetryable() {
        log.Warn("retryable error: %v", err)  // 可重试错误用 WARN
    } else {
        log.Error("error: %v", err)  // 其他错误用 ERROR
    }
    // ...
}

// ❌ Bad: 到处记录日志
log.Error("space not found")
return verrors.SpaceNotFound(spaceName)
```

### 5. 测试错误处理

```go
func TestSearchHandler_SpaceNotFound(t *testing.T) {
    // Setup mock
    mockService := &MockService{
        getSpaceFunc: func() (*Space, error) {
            return nil, verrors.SpaceNotFound("test_space")
        },
    }
    
    // Execute
    w := httptest.NewRecorder()
    c, _ := gin.CreateTestContext(w)
    handler(c)
    
    // Verify
    assert.Equal(t, 404, w.Code)
    
    var resp document.ErrorResponse
    json.Unmarshal(w.Body.Bytes(), &resp)
    assert.Equal(t, "SPACE_NOT_FOUND", resp.Error)
    assert.Contains(t, resp.Message, "test_space")
}
```

## 兼容性

### 与现有代码兼容

在迁移期间，可以使用 `ConvertLegacyError` 转换旧错误：

```go
import "github.com/vearch/vearch/v3/internal/router/document"

// 调用旧代码
legacyErr := legacyFunction()
if legacyErr != nil {
    // 转换为新错误
    return document.ConvertLegacyError(legacyErr)
}
```

### 错误码扩展

如需添加新错误码：

1. 在 `code.go` 中添加常量
2. 更新 `String()` 方法
3. 如果需要，添加辅助函数到 `helper.go`
4. 更新测试

```go
// 1. 添加错误码
const (
    ErrCustomError ErrorCode = 8000
)

// 2. 更新 String()
func (c ErrorCode) String() string {
    switch c {
    // ...
    case ErrCustomError:
        return "CUSTOM_ERROR"
    // ...
    }
}

// 3. 添加辅助函数
func CustomError(msg string) *VearchError {
    return New(ErrCustomError, msg)
}

// 4. 添加测试
func TestCustomError(t *testing.T) {
    err := CustomError("test")
    assert.Equal(t, ErrCustomError, err.Code)
}
```

## 常见问题

### Q1: 如何处理 panic？

```go
// 在 middleware 中统一捕获
func RecoveryMiddleware() gin.HandlerFunc {
    return func(c *gin.Context) {
        defer func() {
            if r := recover(); r != nil {
                err := verrors.Internalf("panic: %v", r)
                document.HandleError(c, err)
            }
        }()
        c.Next()
    }
}
```

### Q2: 如何区分客户端错误和服务器错误？

```go
code := verrors.GetCode(err)
if code >= 1000 && code < 5000 {
    // 客户端错误 (4xx)
} else {
    // 服务器错误 (5xx)
}

// 或者直接使用 HTTP 状态码
status := verrors.HTTPStatus(err)
if status >= 400 && status < 500 {
    // 客户端错误
}
```

### Q3: 性能影响如何？

性能测试结果（见 `error_test.go`）：

```
BenchmarkNew-8              10000000    120 ns/op
BenchmarkWrap-8              5000000    250 ns/op
BenchmarkGetVearchError-8   50000000     35 ns/op
```

错误处理的性能开销可忽略不计。

### Q4: 如何与 gRPC 集成？

```go
import (
    "google.golang.org/grpc/codes"
    "google.golang.org/grpc/status"
)

// VearchError 转 gRPC Status
func ToGRPCStatus(err error) error {
    vErr := verrors.GetVearchError(err)
    if vErr == nil {
        return status.Error(codes.Internal, err.Error())
    }
    
    var code codes.Code
    switch vErr.Code {
    case verrors.ErrInvalidParam:
        code = codes.InvalidArgument
    case verrors.ErrNotFound:
        code = codes.NotFound
    case verrors.ErrUnauthorized:
        code = codes.Unauthenticated
    case verrors.ErrTimeout:
        code = codes.DeadlineExceeded
    default:
        code = codes.Internal
    }
    
    return status.Error(code, vErr.Message)
}
```

## 总结

统一错误处理框架的优势：

- ✅ **一致性**: 所有组件使用相同的错误格式
- ✅ **可追踪**: 支持错误链，方便调试
- ✅ **可分类**: 明确区分错误类型和处理策略
- ✅ **易维护**: 集中管理错误码和消息
- ✅ **易测试**: 标准化的错误便于编写测试
- ✅ **向后兼容**: 提供转换工具平滑迁移

## 相关文件

- `internal/pkg/errors/code.go` - 错误码定义
- `internal/pkg/errors/error.go` - 错误类型实现
- `internal/pkg/errors/helper.go` - 辅助函数
- `internal/pkg/errors/error_test.go` - 单元测试
- `internal/router/document/error_handler.go` - HTTP 错误处理
- `docs/error_handling_migration.md` - 本文档

## 反馈

如有问题或建议，请：
1. 提交 GitHub Issue
2. 在代码审查中讨论
3. 联系架构团队
