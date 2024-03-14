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

package errutil

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
