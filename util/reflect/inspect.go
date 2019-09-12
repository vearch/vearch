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

package reflect

import (
	"fmt"
	"reflect"
	. "runtime"
)

func InspectStruct(i interface{}) {
	t := reflect.TypeOf(i)
	fmt.Println("[INFO] The interface kind is:", t.Kind())
	fmt.Println("[INFO] The interface Name is:", t)
	fmt.Println("[INFO] The interface's number of field(s) is:", t.NumField())
	for idx := 0; idx < t.NumField(); idx++ {
		fmt.Println("[INFO]     field[", idx, "]:", t.Field(idx))
	}
	fmt.Println("[INFO] The interface's number of method(s) is:", t.NumMethod())
	for idx := 0; idx < t.NumMethod(); idx++ {
		fmt.Println("[INFO]     field[", idx, "]:", t.Method(idx))
	}
}

func InspectPtr(i interface{}) {
	t := reflect.TypeOf(i)
	v := reflect.ValueOf(i)
	fmt.Println("[INFO] The interface kind is:", t.Kind())
	fmt.Println("[INFO] The interface Name is:", t)
	fmt.Println("[INFO] The interface's number of field(s) is:", v.Elem().NumField())
	for idx := 0; idx < v.Elem().NumField(); idx++ {
		fmt.Println("[INFO]     field[", idx, "]:", v.Elem().Field(idx))
	}
	fmt.Println("[INFO] The interface's number of method(s) is:", reflect.TypeOf(i).NumMethod())
	for idx := 0; idx < reflect.TypeOf(i).NumMethod(); idx++ {
		fmt.Println("[INFO]     field[", idx, "]:", reflect.TypeOf(i).Method(idx))
	}
}

func RuntimeMethodName(skip int) string {
	pc, _, _, _ := Caller(skip)
	return FuncForPC(pc).Name()
}
