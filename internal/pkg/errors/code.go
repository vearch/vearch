package errors

type ErrorCode int

const (
ErrOK                ErrorCode = 0
ErrInvalidParam      ErrorCode = 1000
ErrMissingParam      ErrorCode = 1001
ErrNotFound          ErrorCode = 3000
ErrDBNotFound        ErrorCode = 3001
ErrSpaceNotFound     ErrorCode = 3002
ErrPartitionNotFound ErrorCode = 3003
ErrDocumentNotFound  ErrorCode = 3004
ErrUnauthorized      ErrorCode = 2000
ErrPermissionDenied  ErrorCode = 2002
ErrConflict          ErrorCode = 4000
ErrAlreadyExists     ErrorCode = 4001
ErrInternal          ErrorCode = 5000
ErrTimeout           ErrorCode = 5001
ErrServiceUnavailable ErrorCode = 5002
ErrMasterUnavailable ErrorCode = 5003
ErrPSUnavailable     ErrorCode = 5004
ErrStorageError      ErrorCode = 5006
ErrNetworkError      ErrorCode = 5007
ErrRPCError          ErrorCode = 5008
ErrTooManyRequests   ErrorCode = 6001
)

func (c ErrorCode) String() string {
	names := map[ErrorCode]string{
		ErrOK: "OK",
		ErrInvalidParam: "INVALID_PARAM",
		ErrMissingParam: "MISSING_PARAM",
		ErrNotFound: "NOT_FOUND",
		ErrDBNotFound: "DB_NOT_FOUND",
		ErrSpaceNotFound: "SPACE_NOT_FOUND",
		ErrPartitionNotFound: "PARTITION_NOT_FOUND",
		ErrDocumentNotFound: "DOCUMENT_NOT_FOUND",
		ErrUnauthorized: "UNAUTHORIZED",
		ErrPermissionDenied: "PERMISSION_DENIED",
		ErrConflict: "CONFLICT",
		ErrAlreadyExists: "ALREADY_EXISTS",
		ErrInternal: "INTERNAL",
		ErrTimeout: "TIMEOUT",
		ErrServiceUnavailable: "SERVICE_UNAVAILABLE",
		ErrMasterUnavailable: "MASTER_UNAVAILABLE",
		ErrPSUnavailable: "PS_UNAVAILABLE",
		ErrStorageError: "STORAGE_ERROR",
		ErrNetworkError: "NETWORK_ERROR",
		ErrRPCError: "RPC_ERROR",
		ErrTooManyRequests: "TOO_MANY_REQUESTS",
	}
	if name, ok := names[c]; ok {
		return name
	}
	return "UNKNOWN"
}

func (c ErrorCode) HTTPStatus() int {
	switch {
	case c == ErrOK:
		return 200
	case c >= 1000 && c < 2000:
		return 400
	case c >= 2000 && c < 3000:
		return 401
	case c >= 3000 && c < 4000:
		return 404
	case c >= 4000 && c < 5000:
		return 409
	case c >= 5000 && c < 6000:
		return 500
	case c >= 6000 && c < 7000:
		return 429
	default:
		return 500
	}
}

func (c ErrorCode) IsRetryable() bool {
	return c == ErrTimeout || c == ErrServiceUnavailable || c == ErrMasterUnavailable ||
		c == ErrPSUnavailable || c == ErrNetworkError || c == ErrRPCError || c == ErrTooManyRequests
}
