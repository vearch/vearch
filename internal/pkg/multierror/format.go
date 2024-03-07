package multierror

import (
	"io"
)

// ErrorFormat error print format definition
type ErrorFormat func([]error, io.Writer)

var (
	multilinePrefix    = []byte("the following errors occurred:")
	multilineSeparator = []byte("\n -- ")
)

// MultilineFormat error print format
func MultilineFormat(errs []error, w io.Writer) {
	w.Write(multilinePrefix)
	for _, err := range errs {
		w.Write(multilineSeparator)
		io.WriteString(w, err.Error())
	}
}
