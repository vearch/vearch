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

package document

import (
	"fmt"
	"regexp"
	"unicode/utf8"
)

// Validation constants
const (
	// maxFieldNameLength is the maximum allowed length for field names
	maxFieldNameLength = 256
	// maxNestedDepth is the maximum nesting depth for JSON objects
	maxNestedDepth = 32
	// maxDocumentSize is the maximum size of a single document in bytes (10MB)
	maxDocumentSize = 10 * 1024 * 1024
	// maxBulkDocuments is the maximum number of documents in a bulk operation
	maxBulkDocuments = 10000
)

var (
	// fieldNamePattern defines valid field name pattern (alphanumeric, underscore, hyphen)
	fieldNamePattern = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_-]*$`)
	
	// reservedFieldNames contains field names that cannot be used
	reservedFieldNames = map[string]bool{
		"_id":      true,
		"_score":   true,
		"_source":  true,
		"_version": true,
	}
)

// InputValidator provides validation methods for user input
type InputValidator struct{}

// NewInputValidator creates a new input validator
func NewInputValidator() *InputValidator {
	return &InputValidator{}
}

// ValidateFieldName validates a field name against security and naming rules.
//
// Parameters:
//   - fieldName: The field name to validate
//
// Returns:
//   - error: Validation error if the field name is invalid, nil otherwise
func (v *InputValidator) ValidateFieldName(fieldName string) error {
	// Check length
	if len(fieldName) == 0 {
		return fmt.Errorf("field name cannot be empty")
	}
	if len(fieldName) > maxFieldNameLength {
		return fmt.Errorf("field name exceeds maximum length of %d characters", maxFieldNameLength)
	}

	// Check for valid UTF-8
	if !utf8.ValidString(fieldName) {
		return fmt.Errorf("field name contains invalid UTF-8 characters")
	}

	// Check for reserved names
	if reservedFieldNames[fieldName] {
		return fmt.Errorf("field name '%s' is reserved and cannot be used", fieldName)
	}

	// Check pattern
	if !fieldNamePattern.MatchString(fieldName) {
		return fmt.Errorf("field name '%s' contains invalid characters (only alphanumeric, underscore, and hyphen allowed)", fieldName)
	}

	return nil
}

// ValidateNestingDepth validates the nesting depth of a JSON structure.
//
// Parameters:
//   - depth: Current nesting depth
//
// Returns:
//   - error: Validation error if depth exceeds maximum, nil otherwise
func (v *InputValidator) ValidateNestingDepth(depth int) error {
	if depth > maxNestedDepth {
		return fmt.Errorf("nesting depth %d exceeds maximum allowed depth of %d", depth, maxNestedDepth)
	}
	return nil
}

// ValidateDocumentSize validates the size of a document.
//
// Parameters:
//   - size: Size of the document in bytes
//
// Returns:
//   - error: Validation error if size exceeds maximum, nil otherwise
func (v *InputValidator) ValidateDocumentSize(size int) error {
	if size > maxDocumentSize {
		return fmt.Errorf("document size %d bytes exceeds maximum allowed size of %d bytes", size, maxDocumentSize)
	}
	return nil
}

// ValidateBulkSize validates the number of documents in a bulk operation.
//
// Parameters:
//   - count: Number of documents
//
// Returns:
//   - error: Validation error if count exceeds maximum, nil otherwise
func (v *InputValidator) ValidateBulkSize(count int) error {
	if count > maxBulkDocuments {
		return fmt.Errorf("bulk operation contains %d documents, exceeds maximum of %d", count, maxBulkDocuments)
	}
	if count <= 0 {
		return fmt.Errorf("bulk operation must contain at least one document")
	}
	return nil
}

// ValidateStringLength validates string length to prevent DoS attacks.
//
// Parameters:
//   - s: The string to validate
//   - maxLength: Maximum allowed length
//   - fieldName: Name of the field for error message
//
// Returns:
//   - error: Validation error if string is too long, nil otherwise
func (v *InputValidator) ValidateStringLength(s string, maxLength int, fieldName string) error {
	if len(s) > maxLength {
		return fmt.Errorf("field '%s' length %d exceeds maximum of %d", fieldName, len(s), maxLength)
	}
	return nil
}

// SanitizeInput removes potentially dangerous characters from user input.
// This is a basic sanitization and should be used in conjunction with proper validation.
//
// Parameters:
//   - input: The input string to sanitize
//
// Returns:
//   - string: Sanitized input
func (v *InputValidator) SanitizeInput(input string) string {
	// Remove null bytes which can cause issues
	result := ""
	for _, r := range input {
		if r != 0 {
			result += string(r)
		}
	}
	return result
}
