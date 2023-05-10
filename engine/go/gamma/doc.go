/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

package gamma

import (
	"../../idl/fbs-gen/go/gamma_api"
	flatbuffers "github.com/google/flatbuffers/go"
)

type Field struct {
	Name     string
	Value    []byte
	Source   string
	Datatype DataType
}

type Doc struct {
	Fields []Field
	doc    *gamma_api.Doc
}

func (doc *Doc) Serialize(buffer *[]byte) int {
	builder := flatbuffers.NewBuilder(0)
	var names, values, sources []flatbuffers.UOffsetT
	names = make([]flatbuffers.UOffsetT, len(doc.Fields))
	values = make([]flatbuffers.UOffsetT, len(doc.Fields))
	sources = make([]flatbuffers.UOffsetT, len(doc.Fields))
	i := 0
	for _, field := range doc.Fields {
		names[i] = builder.CreateString(field.Name)
		values[i] = builder.CreateString(string(field.Value))
		sources[i] = builder.CreateString(field.Source)
		i++
	}

	var fields []flatbuffers.UOffsetT
	fields = make([]flatbuffers.UOffsetT, len(doc.Fields))
	for i := 0; i < len(doc.Fields); i++ {
		gamma_api.FieldStart(builder)
		gamma_api.FieldAddName(builder, names[i])
		gamma_api.FieldAddValue(builder, values[i])
		gamma_api.FieldAddSource(builder, sources[i])
		gamma_api.FieldAddDataType(builder, int8(doc.Fields[i].Datatype))
		fields[i] = gamma_api.FieldEnd(builder)
	}

	gamma_api.DocStartFieldsVector(builder, len(doc.Fields))
	for i := 0; i < len(doc.Fields); i++ {
		builder.PrependUOffsetT(fields[i])
	}
	f := builder.EndVector(len(doc.Fields))

	gamma_api.TableStart(builder)
	gamma_api.DocAddFields(builder, f)
	builder.Finish(builder.EndObject())

	bufferLen := len(builder.FinishedBytes())
	*buffer = make([]byte, bufferLen)
	copy(*buffer, builder.FinishedBytes())
	return bufferLen
}

func (doc *Doc) DeSerialize(buffer []byte) {
	doc.doc = gamma_api.GetRootAsDoc(buffer, 0)
	doc.Fields = make([]Field, doc.doc.FieldsLength())
	for i:= 0; i < len(doc.Fields); i++ {
		var field gamma_api.Field
		doc.doc.Fields(&field, i)
		doc.Fields[i].Name = string(field.Name())
		doc.Fields[i].Value = field.ValueBytes()
		doc.Fields[i].Source = string(field.Source())
		doc.Fields[i].Datatype = DataType(field.DataType())
	}
}
