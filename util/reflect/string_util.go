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

	"github.com/spf13/cast"
)

func StructToString(prefix string, u interface{}) (str string) {

	t := reflect.TypeOf(u)
	v := reflect.ValueOf(u)
	returnStr := ""
	for i := 0; i < v.NumField(); i++ {
		if v.Field(i).CanInterface() {
			switch v.Field(i).Type().Kind() {
			case reflect.Struct:
				returnStr = returnStr + StructToString(prefix+"."+t.Field(i).Name+"", v.Field(i).Interface())
			case reflect.Ptr:
				returnStr = returnStr + PtrToString(prefix+"."+t.Field(i).Name+"", v.Field(i).Interface())
			case reflect.Slice, reflect.Array:
				returnStr = returnStr + SliceToString(prefix+"."+t.Field(i).Name+"", v.Field(i).Interface())
			default:
				returnStr = returnStr + fmt.Sprintf("%s.%s = %v \n", prefix, t.Field(i).Name, v.Field(i).Interface())
			}
		}
	}
	return returnStr
}

func SliceToString(prefix string, u interface{}) (str string) {
	v := reflect.ValueOf(u)
	returnStr := ""
	for i := 0; i < v.Len(); i++ {
		returnStr = returnStr + ToString(prefix+"["+cast.ToString(i)+"]", v.Index(i).Interface())
	}
	return returnStr
}

func PtrToString(prefix string, i interface{}) (str string) {
	t := reflect.TypeOf(i)
	v := reflect.ValueOf(i)
	returnStr := ""
	for i := 0; i < v.Elem().NumField(); i++ {
		if v.Elem().Field(i).CanInterface() {
			switch v.Elem().Field(i).Type().Kind() {
			case reflect.Struct:
				returnStr = returnStr + StructToString(prefix+"."+t.Elem().Field(i).Name+"", v.Elem().Field(i).Interface())
			case reflect.Ptr:
				returnStr = returnStr + PtrToString(prefix+"."+t.Elem().Field(i).Name+"", v.Elem().Field(i).Interface())
			case reflect.Slice, reflect.Array:
				returnStr = returnStr + SliceToString(prefix+"."+t.Elem().Field(i).Name+"", v.Elem().Field(i).Interface())
			default:
				returnStr = returnStr + fmt.Sprintf("%s.%s = %v \n", prefix, t.Elem().Field(i).Name, v.Elem().Field(i).Interface())
			}
		}
	}
	return returnStr
}

func ToString(prefix string, i interface{}) (returnStr string) {
	t := reflect.TypeOf(i)
	switch t.Kind() {
	case reflect.Ptr:
		returnStr = PtrToString(prefix, i)
	case reflect.Struct:
		returnStr = StructToString(prefix, i)
	case reflect.Slice, reflect.Array:
		returnStr = SliceToString(prefix, i)
	default:
		returnStr = fmt.Sprintf("%s = %s\n", prefix, cast.ToString(i))
	}
	return
}
