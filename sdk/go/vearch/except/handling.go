package except

import (
	"fmt"

	"github.com/vearch/vearch/sdk/go/v3/vearch/connection"
	"github.com/vearch/vearch/sdk/go/v3/vearch/fault"
)

func NewClientError(statusCode int, format string, args ...interface{}) *fault.ClientError {
	return &fault.ClientError{
		IsUnexpectedStatusCode: true,
		StatusCode:             statusCode,
		Msg:                    fmt.Sprintf(format, args...),
	}
}

func NewDerivedClientError(err error) *fault.ClientError {
	return &fault.ClientError{
		IsUnexpectedStatusCode: false,
		StatusCode:             -1,
		Msg:                    "check the DerivedFromError field for more information",
		DerivedFromError:       err,
	}
}

func NewUnexpectedStatusCodeErrorFromRESTResponse(responseData *connection.ResponseData) *fault.ClientError {
	return NewClientError(responseData.StatusCode, string(responseData.Body))
}

func CheckResponseDataErrorAndStatusCode(responseData *connection.ResponseData, responseErr error, expectedStatusCodes ...int) error {
	if responseErr != nil {
		return NewDerivedClientError(responseErr)
	}
	for i := range expectedStatusCodes {
		if responseData.StatusCode == expectedStatusCodes[i] {
			return nil
		}
	}
	return NewUnexpectedStatusCodeErrorFromRESTResponse(responseData)
}
