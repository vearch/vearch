package errors

func InvalidParam(paramName string) *VearchError {
	return Newf(ErrInvalidParam, "invalid parameter: %s", paramName)
}

func MissingParam(paramName string) *VearchError {
	return Newf(ErrMissingParam, "required parameter missing: %s", paramName)
}

func NotFound(resource string) *VearchError {
	return Newf(ErrNotFound, "%s not found", resource)
}

func DBNotFound(dbName string) *VearchError {
	return Newf(ErrDBNotFound, "database not found: %s", dbName)
}

func SpaceNotFound(spaceName string) *VearchError {
	return Newf(ErrSpaceNotFound, "space not found: %s", spaceName)
}

func DocumentNotFound(docID string) *VearchError {
	return Newf(ErrDocumentNotFound, "document not found: %s", docID)
}

func Unauthorized(reason string) *VearchError {
	return Newf(ErrUnauthorized, "unauthorized: %s", reason)
}

func PermissionDenied(operation string) *VearchError {
	return Newf(ErrPermissionDenied, "permission denied: %s", operation)
}

func Timeout(operation string) *VearchError {
	return Newf(ErrTimeout, "operation timeout: %s", operation)
}

func Internal(message string) *VearchError {
	return New(ErrInternal, message)
}

func AlreadyExists(resource string) *VearchError {
	return Newf(ErrAlreadyExists, "%s already exists", resource)
}

func RPCError(operation string, cause error) *VearchError {
	return Wrapf(ErrRPCError, cause, "RPC error in %s", operation)
}

func StorageError(operation string, cause error) *VearchError {
	return Wrapf(ErrStorageError, cause, "storage error in %s", operation)
}
