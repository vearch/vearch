//  Copyright (c) 2014 Couchbase, Inc.
// Modified work copyright (C) 2019 The Vearch Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// 		http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mapping

import (
	"sort"
	"strings"

	"github.com/vearch/vearch/proto/entity"
	"github.com/vearch/vearch/proto/vearchpb"
	"github.com/vearch/vearch/util/log"
)

// An IndexMapping controls how objects are placed
// into an index.
// First the type of the object is determined.
// Once the type is know, the appropriate
// DocumentMapping is selected by the type.
// If no mapping was determined for that type,
// a DefaultMapping will be used.
type IndexMapping struct {
	DocumentMapping *DocumentMapping `json:"doc_mapping"`
	//it is not config in index
	DefaultDateTimeParserName string                      `json:"-"`
	fieldCacher               map[string]*DocumentMapping `json:"-"`
	DimensionMap              map[string]int              `json:"-"`
}

// NewIndexMapping creates a new IndexMapping that will use all the default indexing rules
func NewIndexMapping() *IndexMapping {
	mapping := &IndexMapping{
		DocumentMapping: NewDocumentMapping(),
	}
	return mapping
}

//you can use it like dm.DocumentMappingForField("person.name")
func (im *IndexMapping) GetField(path string) *FieldMapping {
	mapping := im.fieldCacher[path]
	if mapping != nil {
		return mapping.Field
	}
	return nil
}

//you can use it like dm.DocumentMappingForField("person.name")
func (im *IndexMapping) GetDocument(path string) *DocumentMapping {
	return im.fieldCacher[path]
}

func (im *IndexMapping) InitFieldCache() {
	if im.fieldCacher != nil {
		log.Error("can use this initFieldCache more than once")
		return
	}
	temp := make(map[string]*DocumentMapping)
	_initFieldCache(temp, im.DocumentMapping.Properties, "")
	im.fieldCacher = temp
}

func _initFieldCache(temp map[string]*DocumentMapping, mappings map[string]*DocumentMapping, path string) {
	if mappings == nil {
		return
	}
	if path != "" {
		path += pathSeparator
	}

	for name, dm := range mappings {
		temp[path+name] = dm
		_initFieldCache(temp, dm.Properties, path+name)
	}
}

type walkContext struct {
	im            *IndexMapping
	Fields        []*vearchpb.Field
	DynamicFields map[string]vearchpb.FieldType
	Err           error
}

func (ctx *walkContext) AddField(f *vearchpb.Field) {
	ctx.Fields = append(ctx.Fields, f)
}

func (im *IndexMapping) newWalkContext() *walkContext {
	return &walkContext{
		im: im,
	}
}

func (im *IndexMapping) GetFieldsType() map[string]vearchpb.FieldType {
	result := make(map[string]vearchpb.FieldType)
	for f, fm := range im.fieldCacher {
		result[f] = vearchpb.FieldType(fm.Field.FieldType())
	}
	return result
}

func (im *IndexMapping) RangeField(f func(key string, value *DocumentMapping) error) error {
	for k, v := range im.fieldCacher {
		if err := f(k, v); err != nil {
			return err
		}
	}
	return nil
}

func (im *IndexMapping) SortRangeField(f func(key string, value *DocumentMapping) error) error {

	var keys []string
	for k := range im.fieldCacher {
		keys = append(keys, k)
	}

	sort.Slice(keys, func(i, j int) bool {
		return strings.Compare(keys[i], keys[j]) > 0
	})

	for _, k := range keys {
		if err := f(k, im.fieldCacher[k]); err != nil {
			return err
		}
	}

	return nil
}

func Space2Mapping(space *entity.Space) (indexMapping *IndexMapping, err error) {
	var docMapping *DocumentMapping

	docMapping, err = ParseSchema(space.Properties)
	if err != nil {
		return nil, err
	}

	indexMapping = NewIndexMapping()
	indexMapping.DocumentMapping = docMapping

	indexMapping.InitFieldCache()

	return indexMapping, nil
}
