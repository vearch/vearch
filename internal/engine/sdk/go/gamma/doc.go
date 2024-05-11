/**
 * Copyright 2019 The Vearch Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

package gamma

import (
	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/vearch/vearch/v3/internal/engine/idl/fbs-gen/go/gamma_api"
	"github.com/vearch/vearch/v3/internal/proto/vearchpb"
)

type Field struct {
	Name     string
	Value    []byte
	Datatype DataType
}

type Doc struct {
	Fields []*vearchpb.Field
	doc    *gamma_api.Doc
}

func (doc *Doc) Serialize() []byte {
	builder := flatbuffers.NewBuilder(0)
	var names, values []flatbuffers.UOffsetT
	names = make([]flatbuffers.UOffsetT, len(doc.Fields))
	values = make([]flatbuffers.UOffsetT, len(doc.Fields))
	i := 0
	for _, field := range doc.Fields {
		names[i] = builder.CreateString(field.Name)
		values[i] = builder.CreateString(string(field.Value))
		i++
	}

	fields := make([]flatbuffers.UOffsetT, len(doc.Fields))
	for i := 0; i < len(doc.Fields); i++ {
		gamma_api.FieldStart(builder)
		gamma_api.FieldAddName(builder, names[i])
		gamma_api.FieldAddValue(builder, values[i])
		gamma_api.FieldAddDataType(builder, int8(doc.Fields[i].Type))
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

	return builder.FinishedBytes()
}

func (doc *Doc) DeSerialize(buffer []byte) {
	doc.doc = gamma_api.GetRootAsDoc(buffer, 0)
	doc.Fields = make([]*vearchpb.Field, doc.doc.FieldsLength())
	for i := 0; i < len(doc.Fields); i++ {
		var field gamma_api.Field
		doc.doc.Fields(&field, i)
		doc.Fields[i] = &vearchpb.Field{}
		doc.Fields[i].Name = string(field.Name())
		doc.Fields[i].Value = field.ValueBytes()
		doc.Fields[i].Type = vearchpb.FieldType(field.DataType())
	}
}
