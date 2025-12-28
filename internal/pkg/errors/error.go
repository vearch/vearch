package errors

import (
"errors"
"fmt"
)

type VearchError struct {
	Code    ErrorCode
	Message string
	Cause   error
	Details map[string]interface{}
}

func (e *VearchError) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("[%s] %s: %v", e.Code.String(), e.Message, e.Cause)
	}
	return fmt.Sprintf("[%s] %s", e.Code.String(), e.Message)
}

func (e *VearchError) Unwrap() error {
	return e.Cause
}

func (e *VearchError) WithDetail(key string, value interface{}) *VearchError {
	if e.Details == nil {
		e.Details = make(map[string]interface{})
	}
	e.Details[key] = value
	return e
}

func (e *VearchError) GetCode() ErrorCode {
	return e.Code
}

func (e *VearchError) IsRetryable() bool {
	return e.Code.IsRetryable()
}

func (e *VearchError) IsTemporary() bool {
	return e.Code.IsRetryable()
}

func (e *VearchError) HTTPStatus() int {
	return e.Code.HTTPStatus()
}

func New(code ErrorCode, message string) *VearchError {
	return &VearchError{Code: code, Message: message}
}

func Newf(code ErrorCode, format string, args ...interface{}) *VearchError {
	return &VearchError{Code: code, Message: fmt.Sprintf(format, args...)}
}

func Wrap(code ErrorCode, message string, cause error) *VearchError {
	return &VearchError{Code: code, Message: message, Cause: cause}
}

func Wrapf(code ErrorCode, cause error, format string, args ...interface{}) *VearchError {
	return &VearchError{Code: code, Message: fmt.Sprintf(format, args...), Cause: cause}
}

func GetVearchError(err error) *VearchError {
	var vErr *VearchError
	if errors.As(err, &vErr) {
		return vErr
	}
	return nil
}

func GetCode(err error) ErrorCode {
	if vErr := GetVearchError(err); vErr != nil {
		return vErr.Code
	}
	return ErrInternal
}

func IsRetryable(err error) bool {
	if vErr := GetVearchError(err); vErr != nil {
		return vErr.IsRetryable()
	}
	return false
}

func HTTPStatus(err error) int {
	if vErr := GetVearchError(err); vErr != nil {
		return vErr.HTTPStatus()
	}
	return 500
}
