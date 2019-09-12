package multierror

import (
	"errors"
	"reflect"
	"testing"
)

func TestErrors(t *testing.T) {
	errors := []error{errors.New("foo"), errors.New("bar")}
	multi := &MultiError{errors: errors}

	if !reflect.DeepEqual(errors, multi.Errors()) {
		t.Fatalf("bad: %s", multi.Errors())
	}
}

func TestErrorErrorOrNil(t *testing.T) {
	errs := new(MultiError)
	if errs.ErrorOrNil() != nil {
		t.Fatalf("bad: %#v", errs.ErrorOrNil())
	}

	errs.errors = []error{errors.New("foo")}
	if v := errs.ErrorOrNil(); v == nil {
		t.Fatal("should not be nil")
	} else if !reflect.DeepEqual(v, errs) {
		t.Fatalf("bad: %#v", v)
	}
}

func TestAppendError(t *testing.T) {
	expected := `the following errors occurred:
 -- foo
 -- bar
 -- test`

	multi := Combine(errors.New("foo"), errors.New("bar"))
	multi.Append(errors.New("test"))
	if multi.Error() != expected {
		t.Fatalf("bad: %s", multi.Error())
	}
}
