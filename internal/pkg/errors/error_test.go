package errors

import (
"net/http"
"testing"
)

func TestNew(t *testing.T) {
	err := New(ErrInvalidParam, "test message")
	if err.Code != ErrInvalidParam {
		t.Errorf("expected code %d, got %d", ErrInvalidParam, err.Code)
	}
	if err.Message != "test message" {
		t.Errorf("expected message 'test message', got '%s'", err.Message)
	}
}

func TestWrap(t *testing.T) {
	cause := http.ErrServerClosed
	err := Wrap(ErrInternal, "wrapped", cause)
	if err.Cause != cause {
		t.Error("expected cause to be preserved")
	}
}

func TestHelpers(t *testing.T) {
	tests := []struct{
		name string
		fn func() *VearchError
		code ErrorCode
	}{
		{"InvalidParam", func() *VearchError { return InvalidParam("test") }, ErrInvalidParam},
		{"SpaceNotFound", func() *VearchError { return SpaceNotFound("test") }, ErrSpaceNotFound},
		{"Timeout", func() *VearchError { return Timeout("test") }, ErrTimeout},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
err := tt.fn()
			if err.Code != tt.code {
				t.Errorf("expected code %d, got %d", tt.code, err.Code)
			}
		})
	}
}

func TestIsRetryable(t *testing.T) {
	if !IsRetryable(Timeout("test")) {
		t.Error("timeout should be retryable")
	}
	if IsRetryable(InvalidParam("test")) {
		t.Error("invalid param should not be retryable")
	}
}
