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

package assert

import (
	"fmt"
	"path/filepath"
	"reflect"
	"runtime"
	"runtime/debug"

	"gotest.tools/assert"
	"gotest.tools/assert/cmp"
)

type TestingT interface {
	FailNow()
	Fail()
	Log(args ...interface{})
	Fatalf(format string, args ...interface{})
}

func Equal(t TestingT, actual, expected interface{}, msg string) {
	assert.Check(t, func() (success bool, message string) {
		actualType := reflect.TypeOf(actual)
		expectedType := reflect.TypeOf(expected)
		if actualType.Kind() != expectedType.Kind() {
			return false, fmt.Sprintf(fatalSource()+"Needs same type, but %s != %s",
				actualType.Kind(), expectedType.Kind())
		}
		if !cmp.Equal(actual, expected)().Success() {
			return false, fmt.Sprintf(fatalSource()+"Expected '%v' (%T) got '%v' (%T). msg:%s",
				expected, expected, actual, actual, msg)
		}
		return true, ""
	}, msg)
}

func NotEqual(t assert.TestingT, actual, expected interface{}, msg string) {
	assert.Check(t, func() (success bool, message string) {
		actualType := reflect.TypeOf(actual)
		expectedType := reflect.TypeOf(expected)
		if actualType.Kind() != expectedType.Kind() {
			return false, fmt.Sprintf(fatalSource()+"Needs same type, but %s != %s",
				actualType.Kind(), expectedType.Kind())
		}
		if cmp.Equal(actual, expected)().Success() {
			return false, fmt.Sprintf(fatalSource()+"Not Expected '%v' (%T) got '%v' (%T). msg:%s",
				expected, expected, actual, actual, msg)
		}
		return true, ""
	}, msg)
}

func DeepEqual(t assert.TestingT, actual, expected interface{}) {
	assert.Check(t, func() (success bool, message string) {
		actualType := reflect.TypeOf(actual)
		expectedType := reflect.TypeOf(expected)
		if actualType.Kind() != expectedType.Kind() {
			return false, fmt.Sprintf(fatalSource()+"Needs same type, but %s != %s",
				actualType.Kind(), expectedType.Kind())
		}
		if !cmp.DeepEqual(actual, expected)().Success() {
			return false, fmt.Sprintf(fatalSource()+"Expected '%v' (%T) got '%v' (%T).",
				expected, expected, actual, actual)
		}
		return true, ""
	}, "")
}

func NotNil(t assert.TestingT, obj interface{}) {
	assert.Check(t, func() (success bool, message string) {
		if cmp.Nil(obj)().Success() {
			return false, fmt.Sprintf(fatalSource()+"Expected non-nil value. %t", obj)
		}
		return true, ""
	}, "")
}

func Nil(t assert.TestingT, obj interface{}) {
	assert.Check(t, func() (success bool, message string) {
		if !cmp.Nil(obj)().Success() {
			return false, fmt.Sprintf(fatalSource() + "Expected nil value.")
		}
		return true, ""
	}, "")
}

func True(t TestingT, obj bool) {
	if obj == false {
		fatal(t, "Expected true value.")
	}
}

func False(t TestingT, obj bool) {
	if obj == true {
		fatal(t, "Expected false value.")
	}
}

const stackIndex = 3

func fatal(t TestingT, format string, args ...interface{}) {
	fmt.Println(string(debug.Stack()))
	t.Fatalf(fatalSource()+format, args...)
}

func fatalSource() string {
	_, filename, line, ok := runtime.Caller(stackIndex)
	if !ok {
		return ""
	}
	return fmt.Sprintf("%s:%d: ", filepath.Base(filename), line)
}
