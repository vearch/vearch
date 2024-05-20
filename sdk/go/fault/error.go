package fault

import (
	"fmt"
)

type ClientError struct {
	IsUnexpectedStatusCode bool
	StatusCode             int
	Msg                    string
	DerivedFromError       error
}

func (ce *ClientError) Error() string {
	msg := ce.Msg
	if ce.DerivedFromError != nil {
		msg = fmt.Sprintf("%s: %s", ce.Msg, ce.DerivedFromError.Error())
	}
	return fmt.Sprintf("status code: %v, error: %v", ce.StatusCode, msg)
}

func (ce *ClientError) GoString() string {
	return ce.Error()
}
