package multierror

import "github.com/vearch/vearch/internal/pkg/bufalloc"

// Combine merge multiple error and create MultiError
func Combine(errs ...error) *MultiError {
	merr := &MultiError{}

	for _, e := range errs {
		if e == nil {
			continue
		}
		switch e := e.(type) {
		case *MultiError:
			merr.errors = append(merr.errors, e.Errors()...)
		default:
			merr.errors = append(merr.errors, e)
		}
	}
	return merr
}

// MultiError multiple error
type MultiError struct {
	errors []error
	Format ErrorFormat
}

// Errors return contained errors
func (me *MultiError) Errors() []error {
	if me == nil {
		return nil
	}
	return me.errors
}

// Append append error to MultiError
func (me *MultiError) Append(errs ...error) {
	for _, e := range errs {
		if e == nil {
			continue
		}
		switch e := e.(type) {
		case *MultiError:
			me.errors = append(me.errors, e.Errors()...)
		default:
			me.errors = append(me.errors, e)
		}
	}
}

// ErrorOrNil if contained errors then return self,else return nil
func (me *MultiError) ErrorOrNil() error {
	if me == nil || len(me.errors) == 0 {
		return nil
	}

	return me
}

// Error return error format output
func (me *MultiError) Error() string {
	if me == nil || len(me.errors) == 0 {
		return ""
	}

	fn := me.Format
	if fn == nil {
		fn = MultilineFormat
	}
	buf := bufalloc.AllocBuffer(1024)
	fn(me.errors, buf)
	result := buf.String()
	bufalloc.FreeBuffer(buf)
	return result
}

type causer interface {
	Cause() error
}

// Cause returns the underlying cause of the error.
func Cause(err error) error {
	for err != nil {
		cause, ok := err.(causer)
		if !ok {
			break
		}
		err = cause.Cause()
	}
	return err
}
