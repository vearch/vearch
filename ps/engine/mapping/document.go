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
	"encoding/json"
	"fmt"
	"github.com/tiglabs/log"
)

type DocumentMapping struct {
	Properties map[string]*DocumentMapping `json:"properties,omitempty"`
	Field      *FieldMapping               `json:"field,omitempty"`
}

func NewDocumentMapping() *DocumentMapping {
	return &DocumentMapping{}
}

func (dm *DocumentMapping) addSubDocumentMapping(property string, sdm *DocumentMapping) {
	if dm.Properties == nil {
		dm.Properties = make(map[string]*DocumentMapping)
	}
	dm.Properties[property] = sdm
}

func (dm *DocumentMapping) UnmarshalJSON(data []byte) error {

	var tmp map[string]json.RawMessage
	err := json.Unmarshal(data, &tmp)
	if err != nil {
		return err
	}

	if tmp["properties"] != nil {
		for k, v := range tmp {
			switch k {
			case "properties":
				err := json.Unmarshal(v, &dm.Properties)
				if err != nil {
					return err
				}
			default:
				log.Warn("unsupport properties type [%s]:[%s]", k, string(v))
				return fmt.Errorf("unsupport properties type [%s]:[%s]", k, string(v))
			}
		}
	} else {
		err := json.Unmarshal(data, &dm.Field)
		if err != nil {
			return err
		}
	}

	return nil
}
