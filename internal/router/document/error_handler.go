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

package document

import (
	"fmt"

	"github.com/gin-gonic/gin"
	verrors "github.com/vearch/vearch/v3/internal/pkg/errors"
	"github.com/vearch/vearch/v3/internal/pkg/log"
)

// ErrorResponse represents the standard error response format for REST API.
type ErrorResponse struct {
	Code    int                    `json:"code"`    // HTTP status code
	Error   string                 `json:"error"`   // Error code string
	Message string                 `json:"message"` // Human-readable error message
	Details map[string]interface{} `json:"details,omitempty"` // Additional error details
}

// HandleError processes VearchError and returns appropriate HTTP response.
// It extracts error code, maps to HTTP status, and constructs error response.
//
// Usage:
//   if err := someOperation(); err != nil {
//       HandleError(c, err)
//       return
//   }
func HandleError(c *gin.Context, err error) {
	if err == nil {
		return
	}

	// Extract VearchError from error chain
	vErr := verrors.GetVearchError(err)
	if vErr == nil {
		// Not a VearchError, treat as internal server error
		log.Error("unhandled error: %v", err)
		c.JSON(500, ErrorResponse{
			Code:    500,
			Error:   "INTERNAL",
			Message: err.Error(),
		})
		return
	}

	// Log error details
	if vErr.IsRetryable() {
		log.Warn("retryable error [%s]: %s", vErr.Code.String(), vErr.Message)
	} else {
		log.Error("error [%s]: %s", vErr.Code.String(), vErr.Message)
	}

	// Construct error response
	response := ErrorResponse{
		Code:    vErr.HTTPStatus(),
		Error:   vErr.Code.String(),
		Message: vErr.Message,
		Details: vErr.Details,
	}

	c.JSON(vErr.HTTPStatus(), response)
}

// HandleSuccess returns a successful response with data.
func HandleSuccess(c *gin.Context, data interface{}) {
	if data == nil {
		c.JSON(200, gin.H{
			"code":    200,
			"message": "success",
		})
		return
	}

	c.JSON(200, data)
}

// ValidateDBName validates database name parameter.
func ValidateDBName(dbName string) error {
	if dbName == "" {
		return verrors.MissingParam("db_name")
	}
	return nil
}

// ValidateSpaceName validates space name parameter.
func ValidateSpaceName(spaceName string) error {
	if spaceName == "" {
		return verrors.MissingParam("space_name")
	}
	return nil
}

// ValidateDocID validates document ID parameter.
func ValidateDocID(docID string) error {
	if docID == "" {
		return verrors.MissingParam("doc_id")
	}
	return nil
}

// ConvertLegacyError converts legacy error types to VearchError.
// This is a compatibility layer during migration period.
//
// Usage:
//   err := legacyFunction()
//   return ConvertLegacyError(err)
func ConvertLegacyError(err error) error {
	if err == nil {
		return nil
	}

	// If already a VearchError, return as is
	if verrors.GetVearchError(err) != nil {
		return err
	}

	// Try to detect error type from message
	msg := err.Error()

	// Pattern matching for common error types
	switch {
	case contains(msg, "not found"):
		return verrors.Wrap(verrors.ErrNotFound, msg, err)
	case contains(msg, "timeout"):
		return verrors.Wrap(verrors.ErrTimeout, msg, err)
	case contains(msg, "unauthorized"):
		return verrors.Wrap(verrors.ErrUnauthorized, msg, err)
	case contains(msg, "invalid"):
		return verrors.Wrap(verrors.ErrInvalidParam, msg, err)
	case contains(msg, "already exists"):
		return verrors.Wrap(verrors.ErrAlreadyExists, msg, err)
	default:
		// Default to internal error
		return verrors.Wrap(verrors.ErrInternal, msg, err)
	}
}

// contains is a helper function to check if a string contains a substring.
func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || 
		(len(s) > len(substr) && containsHelper(s, substr)))
}

func containsHelper(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

// Example usage of the error handling system:
//
// Before (inconsistent):
//   if space == nil {
//       c.JSON(404, gin.H{"error": "space not found"})
//       return
//   }
//
// After (unified):
//   if space == nil {
//       HandleError(c, verrors.SpaceNotFound(spaceName))
//       return
//   }
//
// For wrapped errors:
//   result, err := client.Search(ctx, req)
//   if err != nil {
//       HandleError(c, verrors.RPCError("search", err))
//       return
//   }
//
// For custom details:
//   err := verrors.InvalidParam("vector_dimension")
//   err.WithDetail("expected", 128).WithDetail("actual", 64)
//   HandleError(c, err)
