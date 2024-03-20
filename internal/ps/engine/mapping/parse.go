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

package mapping

import (
	"encoding/json"
	"fmt"

	vmap "github.com/vearch/vearch/internal/pkg/map"
)

const pathSeparator = "."

func ParseSchema(schema []byte) (*DocumentMapping, error) {
	tmp := make([]json.RawMessage, 0)
	err := json.Unmarshal(schema, &tmp)
	if err != nil {
		return nil, err
	}

	dms := NewDocumentMapping()

	for _, data := range tmp {
		dm := NewDocumentMapping()
		err = json.Unmarshal(data, dm)
		if err != nil {
			return nil, err
		}
		dms.addSubDocumentMapping(dm.Field.Name, dm)
	}
	return dms, nil
}

// make schema to map[path]FieldMapping level is 1
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

// merge two schema to a new one
func MergeSchema(old, new []byte) ([]byte, error) {
	newSchemaMap := make(map[string]interface{})
	err := json.Unmarshal([]byte(new), &newSchemaMap)
	if err != nil {
		return nil, err
	}
	newSchemaMap = vmap.DrawMap(newSchemaMap, ".")

	oldSchemaMap := make(map[string]interface{})
	err = json.Unmarshal([]byte(old), &oldSchemaMap)
	if err != nil {
		return nil, err
	}
	oldSchemaMap = vmap.DrawMap(oldSchemaMap, ".")

	for ok, ov := range oldSchemaMap {
		if nv := newSchemaMap[ok]; nv != nil && nv != ov {
			return nil, fmt.Errorf("not same save by key:%s values[%v, %v]", ok, ov, nv)
		} else if nv == nil {
			newSchemaMap[ok] = ov
		}
	}

	newSchemaMap = vmap.AssembleMap(newSchemaMap, ".")
	bytes, err := json.Marshal(newSchemaMap)
	if err != nil {
		return nil, err
	}

	return bytes, nil
}
