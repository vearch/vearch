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
	"strings"

	"github.com/vearch/vearch/v3/internal/proto/vearchpb"
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

	err = parseMappingProperties("", result, dm.Properties)
	return result, err
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

// make map to level 1 example map[a][b]=1  it wil map[a.b]=1
func DrawMap(maps map[string]any, split string) map[string]any {
	newMap := make(map[string]any)
	drawMap(newMap, maps, "", split)
	return newMap
}

func drawMap(result, maps map[string]any, prefix, split string) {
	newPrefix := prefix
	for k, v := range maps {

		if prefix == "" {
			newPrefix = k
		} else {
			newPrefix = prefix + split + k
		}

		switch v := v.(type) {
		case map[string]any:
			drawMap(result, v, newPrefix, split)
		default:
			result[newPrefix] = v
		}
	}
}

// make map to level 1 example map[a.b]=1 it will map[a][b]=1
func assembleMap(maps map[string]any, split string) map[string]any {
	newMap := make(map[string]any)

	for k, v := range maps {
		split := strings.Split(k, split)

		var temp any
		pre := newMap
		for i := range len(split) - 1 {
			temp = pre[split[i]]
			if temp == nil {
				temp = make(map[string]any)
				pre[split[i]] = temp.(map[string]any)
			}
			pre = temp.(map[string]any)
		}
		pre[split[len(split)-1]] = v
	}
	return newMap
}

func MergeSchemaArray(old, new []byte) ([]byte, error) {
	// Parse new schema array
	var newSchemaArray []map[string]any
	if err := json.Unmarshal(new, &newSchemaArray); err != nil {
		return nil, fmt.Errorf("failed to parse new schema as array: %v", err)
	}

	// Parse old schema array
	var oldSchemaArray []map[string]any
	if err := json.Unmarshal(old, &oldSchemaArray); err != nil {
		return nil, fmt.Errorf("failed to parse old schema as array: %v", err)
	}

	// Convert arrays to maps with field names as keys for easier lookup and comparison
	newFieldMap := make(map[string]map[string]any)
	for _, field := range newSchemaArray {
		if name, ok := field["name"].(string); ok {
			newFieldMap[name] = field
		}
	}

	oldFieldMap := make(map[string]map[string]any)
	for _, field := range oldSchemaArray {
		if name, ok := field["name"].(string); ok {
			oldFieldMap[name] = field
		}
	}

	// Check for conflicts and merge
	for fieldName, oldField := range oldFieldMap {
		if newField, exists := newFieldMap[fieldName]; exists {
			// Field exists in both schemas, check if they are the same
			if !areFieldsEqual(oldField, newField) {
				return nil, vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR,
					fmt.Errorf("field conflict for %s: old=%v, new=%v", fieldName, oldField, newField))
			}
		} else {
			// Old field not in new schema, add it
			newFieldMap[fieldName] = oldField
		}
	}

	// Convert merged map back to array
	var resultArray []map[string]any
	for _, field := range newFieldMap {
		resultArray = append(resultArray, field)
	}

	return json.Marshal(resultArray)
}

// areFieldsEqual compares two fields for equality (ignoring index option differences)
func areFieldsEqual(field1, field2 map[string]any) bool {
	// Compare basic properties
	basicFields := []string{"name", "type", "dimension", "store_type", "format"}
	for _, key := range basicFields {
		if field1[key] != field2[key] {
			return false
		}
	}
	return true
}
