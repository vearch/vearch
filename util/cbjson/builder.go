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

package cbjson

import (
	"reflect"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/json-iterator/go/extra"
)

type ContentBuilder interface {
	Field(name string) ContentBuilder
	ValueString(value string) ContentBuilder
	ValueNumeric(value int64) ContentBuilder
	ValueUNumeric(value uint64) ContentBuilder
	ValueFloat(value float64) ContentBuilder
	ValueBool(value bool) ContentBuilder
	ValueNull() ContentBuilder
	ValueInterface(value interface{}) ContentBuilder
	ValueRaw(raw string) ContentBuilder

	BeginObject() ContentBuilder
	BeginObjectWithField(name string) ContentBuilder
	EndObject() ContentBuilder

	BeginArray() ContentBuilder
	BeginArrayWithField(name string) ContentBuilder
	EndArray() ContentBuilder

	More() ContentBuilder
	Output() ([]byte, error)
}

type JsonContentBuilder struct {
	*jsoniter.Stream
}

func (b *JsonContentBuilder) Field(name string) ContentBuilder {
	b.WriteObjectField(name)
	return b
}

func (b *JsonContentBuilder) ValueString(value string) ContentBuilder {
	b.WriteString(value)
	return b
}

func (b *JsonContentBuilder) ValueNumeric(value int64) ContentBuilder {
	b.WriteInt64(value)
	return b
}

func (b *JsonContentBuilder) ValueUNumeric(value uint64) ContentBuilder {
	b.WriteUint64(value)
	return b
}

func (b *JsonContentBuilder) ValueFloat(value float64) ContentBuilder {
	b.WriteFloat64(value)
	return b
}

func (b *JsonContentBuilder) ValueBool(value bool) ContentBuilder {
	b.WriteBool(value)
	return b
}

func (b *JsonContentBuilder) ValueRaw(raw string) ContentBuilder {
	b.WriteRaw(raw)
	return b
}

func (b *JsonContentBuilder) ValueNull() ContentBuilder {
	b.WriteNil()
	return b
}

func (b *JsonContentBuilder) ValueInterface(value interface{}) ContentBuilder {
	if value == nil {
		b.WriteNil()

	} else {
		switch reflect.ValueOf(value).Type().Kind() {
		case reflect.String:
			b.WriteString(value.(string))
		case reflect.Float64, reflect.Float32:
			b.WriteFloat64(value.(float64))
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			b.WriteUint64(value.(uint64))
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			b.WriteInt64(value.(int64))
		case reflect.Bool:
			if value.(bool) {
				b.WriteTrue()
			} else {
				b.WriteFalse()
			}
		default:
			b.WriteVal(value)
		}
	}

	return b
}

func (b *JsonContentBuilder) BeginObject() ContentBuilder {
	b.WriteObjectStart()
	return b
}

func (b *JsonContentBuilder) BeginObjectWithField(name string) ContentBuilder {
	b.Field(name).BeginObject()
	return b
}

func (b *JsonContentBuilder) EndObject() ContentBuilder {
	b.WriteObjectEnd()
	return b
}

func (b *JsonContentBuilder) BeginArray() ContentBuilder {
	b.WriteArrayStart()
	return b
}

func (b *JsonContentBuilder) BeginArrayWithField(name string) ContentBuilder {
	b.Field(name).BeginArray()
	return b
}

func (b *JsonContentBuilder) EndArray() ContentBuilder {
	b.WriteArrayEnd()
	return b
}

func (b *JsonContentBuilder) More() ContentBuilder {
	b.WriteMore()
	return b
}

func (b *JsonContentBuilder) Output() ([]byte, error) {
	return b.Buffer(), b.Error
}

func ContentBuilderFactory() ContentBuilder {
	builder := &JsonContentBuilder{
		Stream: jsoniter.NewStream(aggrJsoniter, nil, 8192),
	}

	return builder
}

var aggrJsoniter jsoniter.API

func init() {
	extra.RegisterTimeAsInt64Codec(time.Nanosecond)

	aggrJsoniter = jsoniter.Config{
		SortMapKeys:             false,
		EscapeHTML:              true,
		ValidateJsonRawMessage:  true,
		MarshalFloatWith6Digits: true,
		UseNumber:               true,
	}.Froze()
}
