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

package gammacb

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/spf13/cast"
	"github.com/vearch/vearch/engine/sdk/go/gamma"
	"github.com/vearch/vearch/proto/entity"
	"github.com/vearch/vearch/proto/vearchpb"
	"github.com/vearch/vearch/ps/engine/mapping"
	"github.com/vearch/vearch/ps/engine/register"
	"github.com/vearch/vearch/util/cbjson"
)

var empty = []byte{0}

func int64ToBytes(i int64) []byte {
	var buf = make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(i))
	return buf
}

func bytesToInt64(buf []byte) int64 {
	return int64(binary.BigEndian.Uint64(buf))
}

func buildField(name string, value []byte, dataType gamma.DataType) gamma.Field {
	field := gamma.Field{Name: name, Datatype: dataType, Value: value}
	return field
}

func buildFieldBySource(name string, value []byte, source string, dataType gamma.DataType) gamma.Field {
	field := gamma.Field{Name: name, Datatype: dataType, Value: value}
	if source != "" {
		field.Source = source
	}
	return field
}

func mapping2Table(cfg register.EngineConfig, m *mapping.IndexMapping) (*gamma.Table, error) {
	dim := make(map[string]int)

	engine := cfg.Space.Engine
	retrievalParam := ""
	if engine.RetrievalParam != nil {
		retrievalParam = string(engine.RetrievalParam)
	}

	var retrievalParamsArr []string
	retrievalParams := &entity.RetrievalParams{}
	if engine.RetrievalParams != nil {
		err := cbjson.Unmarshal(engine.RetrievalParams, &retrievalParams.RetrievalParamArr)
		if err != nil {
			return nil, fmt.Errorf("retrieval_params Unmarshal error")
		}
		retrievalParamsArr = make([]string, len(retrievalParams.RetrievalParamArr))
		for i := 0; i < len(retrievalParams.RetrievalParamArr); i++ {
			v := retrievalParams.RetrievalParamArr[i]
			retrievalParamsByte, err := json.Marshal(v)
			if err != nil {
				return nil, fmt.Errorf("retrieval_params Unmarshal error")
			}
			retrievalParamsArr[i] = string(retrievalParamsByte)
		}
	}

	table := &gamma.Table{
		Name:            cfg.Space.Name + "-" + cast.ToString(cfg.PartitionID),
		IndexingSize:    int32(engine.IndexSize),
		RetrievalType:   engine.RetrievalType,
		RetrievalTypes:  engine.RetrievalTypes,
		RetrievalParam:  retrievalParam,
		RetrievalParams: retrievalParamsArr,
	}

	idTypeStr := engine.IdType
	if strings.EqualFold("long", idTypeStr) {
		fieldInfo := gamma.FieldInfo{Name: mapping.IdField, DataType: gamma.LONG, IsIndex: false}
		table.Fields = append(table.Fields, fieldInfo)
	} else {
		fieldInfo := gamma.FieldInfo{Name: mapping.IdField, DataType: gamma.STRING, IsIndex: false}
		table.Fields = append(table.Fields, fieldInfo)
	}

	err := m.SortRangeField(func(key string, value *mapping.DocumentMapping) error {
		switch value.Field.FieldType() {
		case vearchpb.FieldType_STRING:
			value.Field.Options()
			index := (value.Field.Options() & vearchpb.FieldOption_Index) / vearchpb.FieldOption_Index
			fieldInfo := gamma.FieldInfo{Name: key, DataType: gamma.STRING}
			if index == 1 {
				fieldInfo.IsIndex = true
			} else {
				fieldInfo.IsIndex = false
			}
			table.Fields = append(table.Fields, fieldInfo)
		case vearchpb.FieldType_FLOAT:
			index := (value.Field.Options() & vearchpb.FieldOption_Index) / vearchpb.FieldOption_Index
			fieldInfo := gamma.FieldInfo{Name: key, DataType: gamma.FLOAT}
			if index == 1 {
				fieldInfo.IsIndex = true
			} else {
				fieldInfo.IsIndex = false
			}
			table.Fields = append(table.Fields, fieldInfo)
		case vearchpb.FieldType_DOUBLE:
			index := (value.Field.Options() & vearchpb.FieldOption_Index) / vearchpb.FieldOption_Index
			fieldInfo := gamma.FieldInfo{Name: key, DataType: gamma.DOUBLE}
			if index == 1 {
				fieldInfo.IsIndex = true
			} else {
				fieldInfo.IsIndex = false
			}
			table.Fields = append(table.Fields, fieldInfo)
		case vearchpb.FieldType_DATE, vearchpb.FieldType_LONG:
			index := (value.Field.Options() & vearchpb.FieldOption_Index) / vearchpb.FieldOption_Index
			fieldInfo := gamma.FieldInfo{Name: key, DataType: gamma.LONG}
			if index == 1 {
				fieldInfo.IsIndex = true
			} else {
				fieldInfo.IsIndex = false
			}
			table.Fields = append(table.Fields, fieldInfo)
		case vearchpb.FieldType_INT:
			index := (value.Field.Options() & vearchpb.FieldOption_Index) / vearchpb.FieldOption_Index
			fieldInfo := gamma.FieldInfo{Name: key, DataType: gamma.INT}
			if index == 1 {
				fieldInfo.IsIndex = true
			} else {
				fieldInfo.IsIndex = false
			}
			table.Fields = append(table.Fields, fieldInfo)
		case vearchpb.FieldType_VECTOR:
			fieldMapping := value.Field.FieldMappingI.(*mapping.VectortFieldMapping)
			dim[key] = fieldMapping.Dimension
			//index := (value.Field.Options() & vearchpb.FieldOption_Index) / vearchpb.FieldOption_Index
			vectorInfo := gamma.VectorInfo{
				Name:       key,
				DataType:   gamma.FLOAT,
				Dimension:  int32(fieldMapping.Dimension),
				ModelId:    fieldMapping.ModelId,
				StoreType:  fieldMapping.StoreType,
				StoreParam: string(fieldMapping.StoreParam),
				HasSource:  fieldMapping.HasSource,
			}
			vectorInfo.IsIndex = true
			/*if index == 1 {
				vectorInfo.IsIndex = true
			} else {
				vectorInfo.IsIndex = false
			}*/
			table.VectorsInfos = append(table.VectorsInfos, vectorInfo)
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	m.DimensionMap = dim

	if !(engine.DataType != "" && (strings.EqualFold("scalar", engine.DataType))) {
		if len(table.VectorsInfos) == 0 {
			return nil, fmt.Errorf("create table has no vector field")
		}
	}

	return table, nil

}

/*
//create new_doc
func NewDocCmd2Document(docCmd *vearchpb.DocCmd, idType string) (*gamma.Doc, error) {

	//fields := make([]gamma.Field, len(docCmd.Fields)+2)
	var doc gamma.Doc
	if strings.EqualFold("long", idType) {
		int64Id, err := strconv.ParseInt(docCmd.Doc.PKey, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("table id is long but docId is string , docId Convert long error")
		}

		if toByte, e := cbbytes.ValueToByte(int64Id); e != nil {
			return nil, e
		} else {
			doc.Fields = append(doc.Fields, buildField(mapping.IdField, toByte, gamma.LONG))
		}
	} else {
		doc.Fields = append(doc.Fields, buildField(mapping.IdField, []byte(docCmd.Doc.PKey), gamma.STRING))
	}

	checkVectorHave := false
	for _, f := range docCmd.Doc.Fields {

		if f.Value == nil {
			return nil, fmt.Errorf("miss field value by name:%s", f.Name)
		}

		if mapping.FieldsIndex[f.Name] > 0 {
			continue
		}

		switch f.Type {
		case vearchpb.FieldType_STRING:
			doc.Fields = append(doc.Fields, buildField(f.Name, f.Value, gamma.STRING))
		case vearchpb.FieldType_FLOAT:
			doc.Fields = append(doc.Fields, buildField(f.Name, f.Value, gamma.FLOAT))
		case vearchpb.FieldType_DATE:
			doc.Fields = append(doc.Fields, buildField(f.Name, f.Value, gamma.LONG))
		case vearchpb.FieldType_INT:
			doc.Fields = append(doc.Fields, buildField(f.Name, f.Value, gamma.INT))
		case vearchpb.FieldType_LONG:
			doc.Fields = append(doc.Fields, buildField(f.Name, f.Value, gamma.LONG))
		case vearchpb.FieldType_BOOL:
			doc.Fields = append(doc.Fields, buildField(f.Name, f.Value, gamma.INT))
		case vearchpb.FieldType_VECTOR:
			length := int(cbbytes.ByteToUInt32(f.Value))
			if length > 0 {
				checkVectorHave = true
			}
			doc.Fields = append(doc.Fields, buildFieldBySource(f.Name, f.Value[4:length+4], string(f.Value[length+4:]), gamma.VECTOR))
		default:
			log.Debug("gamma invalid field type:[%v]", f.Type)
		}
	}

	if !checkVectorHave {
		return nil, fmt.Errorf("insert field data no vector please check")
	}

	return &doc, nil
}

func (ge *gammaEngine) convertFieldType(gammaField gamma.Field) vearchpb.Field {
	vearchpbF := vearchpb.Field{Name: gammaField.Name, Source: gammaField.Source, Value: gammaField.Value}
	switch gammaField.Datatype {
	case gamma.INT:
		vearchpbF.Type = vearchpb.FieldType_INT
	case gamma.LONG:
		vearchpbF.Type = vearchpb.FieldType_LONG
	case gamma.FLOAT:
		vearchpbF.Type = vearchpb.FieldType_FLOAT
	case gamma.DOUBLE:
		vearchpbF.Type = vearchpb.FieldType_FLOAT
	case gamma.STRING:
		vearchpbF.Type = vearchpb.FieldType_STRING
	case gamma.VECTOR:
		vearchpbF.Type = vearchpb.FieldType_VECTOR
	}
	return vearchpbF
}

func (ge *gammaEngine) GammaDocConvertGODoc(docGamma *gamma.Doc, doc *vearchpb.Document) {
	fields := docGamma.Fields
	for _, fv := range fields {
		vearchpbF := ge.convertFieldType(fv)
		doc.Fields = append(doc.Fields, &vearchpbF)
	}
}
*/
