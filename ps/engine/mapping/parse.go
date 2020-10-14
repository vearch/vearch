// Copyright 2018 The Couchbase Authors.
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

package mapping

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"strings"

	"github.com/valyala/fastjson"
	"github.com/vearch/vearch/proto/vearchpb"
	"github.com/vearch/vearch/util"
	"github.com/vearch/vearch/util/cbbytes"
	"github.com/vearch/vearch/util/log"
)

func ParseSchema(schema []byte) (*DocumentMapping, error) {
	tmp := make(map[string]json.RawMessage)
	err := json.Unmarshal(schema, &tmp)
	if err != nil {
		return nil, err
	}

	dms := NewDocumentMapping()

	for name, data := range tmp {
		dm := NewDocumentMapping()
		err = json.Unmarshal(data, dm)
		if err != nil {
			return nil, err
		}
		dms.addSubDocumentMapping(name, dm)
	}
	return dms, nil
}

func (im *IndexMapping) MapDocument(source []byte, retrievalType string) ([]*vearchpb.Field, map[string]vearchpb.FieldType, error) {
	walkContext := im.newWalkContext()
	im.walkDocument(walkContext, source, retrievalType)
	if walkContext.Err != nil {
		return nil, nil, walkContext.Err
	}

	if len(walkContext.Fields) != len(im.DocumentMapping.Properties) {
		return nil, nil, fmt.Errorf("field num:[%d] not same by schema:[%d]", len(walkContext.Fields), len(im.DocumentMapping.Properties))
	}

	return walkContext.Fields, walkContext.DynamicFields, nil
}

func (im *IndexMapping) walkDocument(context *walkContext, data []byte, retrievalType string) {
	var fast fastjson.Parser
	v, err := fast.ParseBytes(data)
	if err != nil {
		log.Error("parse document err:[%s] ,data:[%s]", err.Error(), string(data))
		context.Err = err
		return
	}
	if v.Type() != fastjson.TypeObject {
		context.Err = fmt.Errorf("content type err:[type:object] type is [%s] ", v.Type())
		return
	}
	var path []string
	im.DocumentMapping.parseJson(context, path, v, retrievalType)
}

func (dm *DocumentMapping) parseJson(context *walkContext, path []string, v *fastjson.Value, retrievalType string) {
	switch v.Type() {
	case fastjson.TypeObject:
		obj, err := v.Object()
		if err != nil {
			context.Err = err
			return
		}
		obj.Visit(func(key []byte, val *fastjson.Value) {
			fieldName := string(key)
			subDocM := dm.subDocumentMapping(fieldName)
			if subDocM != nil {
				subDocM.processProperty(context, fieldName, path, val, retrievalType)
			} else {
				context.Err = fmt.Errorf("unrecognizable field:[%s] value %s %v", fieldName, v.String(), dm)
			}
		})
	case fastjson.TypeArray:
		items, err := v.Array()
		if err != nil {
			context.Err = err
			return
		}
		if dm.Field != nil {
			if dm.Field.FieldType() == vearchpb.FieldType_GEOPOINT {
				dm.processProperty(context, dm.Field.FieldName(), path, v, retrievalType)
			} else {
				for _, item := range items {
					dm.processProperty(context, dm.Field.FieldName(), path, item, retrievalType)
				}
			}
		} else {
			for _, item := range items {
				dm.parseJson(context, path, item, retrievalType)
			}
		}
	default:
		if dm.Field != nil {
			dm.processProperty(context, dm.Field.FieldName(), path, v, retrievalType)
		} else {
			dm.processProperty(context, path[len(path)-1], path[:len(path)-1], v, retrievalType)
		}
	}
}

func (dm *DocumentMapping) processProperty(context *walkContext, fieldName string, path []string, v *fastjson.Value, retrievalType string) {

	if context.Err != nil {
		return
	}

	if len(path) == 0 && FieldsIndex[fieldName] > 0 {
		context.Err = fmt.Errorf("filed name [%s]  is an internal field that cannot be used", fieldName)
		return
	}

	pathString := fieldName
	if len(path) > 0 {
		pathString = encodePath(append(path, fieldName))
	}

	if v.Type() == fastjson.TypeNull {
		return
	}

	switch v.Type() {
	case fastjson.TypeString:
		propertyValueByte, err := v.StringBytes()
		if err != nil {
			context.Err = err
			return
		}

		propertyValueString := string(propertyValueByte)
		if dm.Field != nil {
			field, err := processString(context, dm.Field, pathString, propertyValueString)
			if err != nil {
				context.Err = err
				return
			}
			context.AddField(field)
		} else {
			context.Err = fmt.Errorf("unrecognizable field %s %v", pathString, dm)
			return
		}
	case fastjson.TypeNumber:
		propertyValFloat, err := v.Float64()
		if err != nil {
			context.Err = err
			return
		}
		if dm.Field != nil {
			fm := dm.Field
			field, err := processNumber(context, fm, pathString, propertyValFloat)
			if err != nil {
				context.Err = err
				return
			}
			context.AddField(field)
		} else {
			context.Err = fmt.Errorf("unrecognizable field %s %v", pathString, dm)
			return
		}
	case fastjson.TypeTrue, fastjson.TypeFalse:
		propertyValBool, err := v.Bool()
		if err != nil {
			context.Err = err
			return
		}
		if dm.Field != nil {
			fm := dm.Field
			field, err := processBool(context, fm, pathString, propertyValBool)
			if err != nil {
				context.Err = err
				return
			}
			context.AddField(field)
		} else {
			context.Err = fmt.Errorf("unrecognizable field %s %v", pathString, dm)
			return
		}
	case fastjson.TypeObject:
		if dm.Field != nil {
			fm := dm.Field
			if fm.FieldType() == vearchpb.FieldType_GEOPOINT {
				// Geo-point expressed as an object, with lat and lon keys.
				latV := v.Get("lat")
				lonV := v.Get("lon")
				if latV != nil && latV.Type() == fastjson.TypeNumber && lonV != nil && lonV.Type() == fastjson.TypeNumber {
					lon, err := lonV.Float64()
					if err != nil {
						context.Err = fmt.Errorf("field value %s mismatch geo point, err %v", v.String(), err)
						return
					}
					lat, err := latV.Float64()
					if err != nil {
						context.Err = fmt.Errorf("field value %s mismatch geo point, err %v", v.String(), err)
						return
					}
					field, err := processGeoPoint(context, fm, pathString, lon, lat)
					context.AddField(field)
				} else {
					context.Err = fmt.Errorf("field value %s mismatch geo point", v.String())
				}
				return
			} else if fm.FieldType() == vearchpb.FieldType_VECTOR {

				source := v.GetStringBytes("source")
				feature := v.GetArray("feature")
				if strings.Compare("BINARYIVF", retrievalType) == 0 {
					vector := make([]uint8, len(feature))
					for i := 0; i < len(feature); i++ {
						if int8, err := feature[i].Int(); err != nil {
							context.Err = fmt.Errorf("vector can not to uint8 %v", feature[i])
							return
						} else {
							vector[i] = uint8(int8)
						}
					}
					field, err := processVectorBinary(context, fm, pathString, vector, string(source))
					if err != nil {
						context.Err = fmt.Errorf("process vectory binary err:[%s] m value:[%v]", err.Error(), vector)
						return
					}
					context.AddField(field)
					return
				} else {
					vector := make([]float32, len(feature))
					for i := 0; i < len(feature); i++ {
						if f64, err := feature[i].Float64(); err != nil {
							context.Err = fmt.Errorf("vector can not to float 64 %v", feature[i])
							return
						} else if math.IsNaN(f64) || math.IsInf(f64, 0) {
							context.Err = fmt.Errorf("vector value is index:[%d], err:[ %v]", i, feature[i])
							return
						} else {
							vector[i] = float32(f64)
						}
					}

					field, err := processVector(context, fm, pathString, vector, string(source))
					if err != nil {
						context.Err = fmt.Errorf("process vectory err:[%s] m value:[%v]", err.Error(), vector)
						return
					}
					context.AddField(field)
					return
				}
			}
		}
		dm.parseJson(context, append(path, fieldName), v, retrievalType)
	case fastjson.TypeArray:
		vs, err := v.Array()
		if err != nil {
			context.Err = err
			return
		}
		if dm.Field != nil {
			fm := dm.Field
			if fm.FieldType() == vearchpb.FieldType_GEOPOINT {
				if len(vs) != 2 {
					context.Err = fmt.Errorf("field value %s mismatch geo point, %v", v.String(), vs)
					return
				}
				// Geo-point expressed as an array with the format: [ lon, lat]
				if vs[0].Type() == fastjson.TypeNumber && vs[1].Type() == fastjson.TypeNumber {
					lon, err := vs[0].Float64()
					if err != nil {
						context.Err = fmt.Errorf("field value %s mismatch geo point, err %v", v.String(), err)
						return
					}
					lat, err := vs[1].Float64()
					if err != nil {
						context.Err = fmt.Errorf("field value %s mismatch geo point, err %v", v.String(), err)
						return
					}
					field, err := processGeoPoint(context, fm, pathString, lon, lat)
					if err != nil {
						context.Err = err
						return
					}
					context.AddField(field)
				}
				return
			}

			if fm.FieldType() == vearchpb.FieldType_STRING && fm.FieldMappingI.(*StringFieldMapping).Array {
				buffer := bytes.Buffer{}
				for i, vv := range vs {
					if stringBytes, err := vv.StringBytes(); err != nil {
						context.Err = err
						return
					} else {
						buffer.Write(stringBytes)
						if i < len(vs)-1 {
							buffer.WriteRune('\001')
						}
					}
				}
				field, err := processString(context, fm, pathString, buffer.String())
				if err != nil {
					context.Err = err
					return
				}
				context.AddField(field)

				return
			}

			if fm.FieldType() == vearchpb.FieldType_INT && fm.FieldMappingI.(*NumericFieldMapping).Array {
				buffer := bytes.Buffer{}
				for _, vv := range vs {
					buffer.Write(cbbytes.Int32ToByte(int32(vv.GetInt64())))
				}
				field := &vearchpb.Field{
					Name:   fieldName,
					Type:   vearchpb.FieldType_INT,
					Value:  buffer.Bytes(),
					Option: fm.Options(),
				}
				context.AddField(field)

				return
			}

			if fm.FieldType() == vearchpb.FieldType_LONG && fm.FieldMappingI.(*NumericFieldMapping).Array {
				buffer := bytes.Buffer{}
				for _, vv := range vs {
					buffer.Write(cbbytes.Int64ToByte(vv.GetInt64()))
				}
				field := &vearchpb.Field{
					Name:   fieldName,
					Type:   vearchpb.FieldType_LONG,
					Value:  buffer.Bytes(),
					Option: fm.Options(),
				}
				context.AddField(field)

				return
			}

			if fm.FieldType() == vearchpb.FieldType_FLOAT && fm.FieldMappingI.(*NumericFieldMapping).Array {
				buffer := bytes.Buffer{}
				for _, vv := range vs {
					buffer.Write(cbbytes.Float64ToByte(vv.GetFloat64()))
				}

				field := &vearchpb.Field{
					Name:   fieldName,
					Type:   vearchpb.FieldType_FLOAT,
					Value:  buffer.Bytes(),
					Option: fm.Options(),
				}
				context.AddField(field)

				return
			}

			context.Err = fmt.Errorf("field:[%s]  this type:[%d] can use by array", fieldName, fm.FieldType())
			return
		}

		for _, vv := range vs {
			dm.processProperty(context, fieldName, path, vv, retrievalType)
		}
	}
}

func (dm *DocumentMapping) subDocumentMapping(field string) *DocumentMapping {
	if dm.Properties != nil {
		return dm.Properties[field]
	}
	return nil
}

//make schema to map[path]FieldMapping level is 1
func SchemaMap(schema []byte) (map[string]FieldMappingI, error) {
	dm, err := ParseSchema(schema)
	if err != nil {
		return nil, err
	}

	result := make(map[string]FieldMappingI)

	if err := parseMappingProperties("", result, dm.Properties); err != nil {
		return nil, err
	} else {
		return result, nil
	}
}

func Equals(f1, f2 FieldMappingI) bool {
	if f1.FieldName() != f2.FieldName() {
		return false
	}
	if f1.FieldType() != f2.FieldType() {
		return false
	}
	if f1.Options() != f2.Options() {
		return false
	}
	return true
}

func parseMapping(prefix string, result map[string]FieldMappingI, dm *DocumentMapping) error {

	if dm.Field != nil {
		result[prefix] = dm.Field
	} else if dm.Properties != nil {
		return parseMappingProperties(prefix, result, dm.Properties)
	}

	return nil
}

func parseMappingProperties(prefix string, result map[string]FieldMappingI, dms map[string]*DocumentMapping) error {
	var key string
	for name, dm := range dms {
		if len(prefix) == 0 {
			key = name
		} else {
			key = prefix + "." + name
		}
		if err := parseMapping(key, result, dm); err != nil {
			return err
		}
	}
	return nil
}

//merge two schema to a new one
func MergeSchema(old, new []byte) ([]byte, error) {
	newSchemaMap := make(map[string]interface{})
	err := json.Unmarshal([]byte(new), &newSchemaMap)
	if err != nil {
		return nil, err
	}
	newSchemaMap = util.DrawMap(newSchemaMap, ".")

	oldSchemaMap := make(map[string]interface{})
	err = json.Unmarshal([]byte(old), &oldSchemaMap)
	if err != nil {
		return nil, err
	}
	oldSchemaMap = util.DrawMap(oldSchemaMap, ".")

	for ok, ov := range oldSchemaMap {
		if nv := newSchemaMap[ok]; nv != nil && nv != ov {
			return nil, fmt.Errorf("not same save by key:%s values[%v, %v]", ok, ov, nv)
		} else if nv == nil {
			newSchemaMap[ok] = ov
		}
	}

	newSchemaMap = util.AssembleMap(newSchemaMap, ".")
	bytes, err := json.Marshal(newSchemaMap)
	if err != nil {
		return nil, err
	}

	return bytes, nil

}

//it use for parse docmapping
func (im *IndexMapping) documentMappingForPath(path string) *DocumentMapping {
	paths := decodePath(path)
	properties := im.DocumentMapping.Properties
	len := len(paths) - 1
	for i := 0; i < len; i++ {
		properties = properties[paths[i]].Properties
	}
	return properties[paths[len]]
}

const pathSeparator = "."

func decodePathWithName(path string) (string, []string) {
	split := decodePath(path)

	return split[len(split)-1], split[0 : len(split)-1]
}

func decodePath(path string) []string {
	return strings.Split(path, pathSeparator)
}

func encodePath(pathElements []string) string {
	return strings.Join(pathElements, pathSeparator)
}
