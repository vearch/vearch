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
	"encoding/json"
	"fmt"
	"github.com/vearch/vearch/proto/vearchpb"
	"github.com/vearch/vearch/util/assert"
	"testing"
	"time"
)

func TestParseSchema(t *testing.T) {

	schema := `{
        "city": {
            "type": "string"
        },
        "age": {
            "type": "integer"
        },
        "insert_time": {
            "type": "date"
        },
        "geo_location": {
            "type": "geo_point"
        },
        "content": {
            "type": "string",
            "analyzer": "standard"
        }
    }`

	mapping, err := ParseSchema([]byte(schema))

	if err != nil {
		t.Errorf(err.Error())
	}

	assert.True(t, mapping.Properties != nil)

	assert.True(t, mapping.Properties["content"] != nil)

	assert.Equal(t, mapping.Properties["content"].Field.FieldMappingI.(*TextFieldMapping).Analyzer, "standard", "parse err ")

	im := NewIndexMapping()
	im.DocumentMapping = mapping

	source := []byte(`{
    "city": "Brogan",
    "age": 32,
    "insert_time":"1985-08-28",
    "new_field":"1985-08-28",
    "aaaa":{
      "bbb":"love",
      "efg":"love",
      "ccc":123,
      "inner":{"_id":"abc","name":123}
    }
}`)

	fields, newSchema, err := im.MapDocument(source, "")

	if err != nil {
		t.Fatal(err)
	}

	for _, f := range fields {
		fmt.Println(f.Name, f.Type)
	}

	assert.Equal(t, len(newSchema), 6, "new schema not right")

}

func TestParseErr(t *testing.T) {
	mapping, err := ParseSchema([]byte(`{"body":{"type":"string"},"name":{"type":"text"},"title":{"type":"string"},"val":{"type":"integer"}}`))
	if err != nil {
		t.Fatal(err)
	}

	im := NewIndexMapping()
	im.DocumentMapping = mapping

	fields, types, err := im.MapDocument([]byte(`{"body2":"test no body","name":"new_name","title":"test title","val":100}`), "")

	if err != nil {
		t.Fatal(err)
	}

	assert.True(t, len(types) == 1)
	assert.True(t, len(fields) == 4)
}

func TestParseSchemaErr(t *testing.T) {
	schema := `{
				"title": {
					"type": "string",
					"analyzer": "keyword"
				},
				"name": {
					"type": "keyword"
				},
				"ipaddr": {
					"type": "keyword"
				},
				"age": {
					"type": "integer"
				},
				"birthday": {
					"type": "date"
				},
				"content": {
					"type": "string",
					"analyzer": "standard"
				},
				"point": {
					"type": "string",
					"analyzer": "standard"
				},
				"location": {
					"type": "geo_point"
				},
				"time_stamp": {
					"type": "date"
				},
				"time_stamp2": {
					"type": "date"
				},
				"time_stamp3": {
					"type": "date"
				},
				"youyou":{
					"properties":{
						"title": {
							"type": "string"
						},
						"name": {
							"type": "string"
						},
						"age": {
							"type": "integer"
						},
						"birthday": {
							"type": "date"
						}
					}

				}
			}`

	mapping, e := ParseSchema([]byte(schema))

	if e != nil {
		t.Fatal(e)
	}

	im := NewIndexMapping()
	im.DocumentMapping = mapping

	doc := struct {
		Title    string    `json:"title"`
		Name     []string  `json:"name"`
		Age      int       `json:"age"`
		Birthday time.Time `json:"birthday"`
		Content  string    `json:"content"`
		Location struct {
			Lat float64 `json:"lat"`
			Lon float64 `json:"lon"`
		} `json:"location"`
		TimeStamp  int64  `json:"time_stamp"`
		TimeStamp3 string `json:"time_stamp3"`
		YouYou     struct {
			Name string `json:"name"`
		} `json:"youyou"`
	}{
		Title: "ansj ",
		Name:  []string{"hello"},
		Age:   33,
		//Birthday:   time.Unix(int64(time.Now().Unix()-int64(i*3600*24)), 0),
		Birthday:   time.Now(),
		TimeStamp3: "1985-08-28 11:22:33",
		Content:    "我爱北京天安门，结婚的和尚未结婚的",
		Location: struct {
			Lat float64 `json:"lat"`
			Lon float64 `json:"lon"`
		}{12, 12},
		TimeStamp: time.Now().UnixNano() / 1e6,
		YouYou: struct {
			Name string `json:"name"`
		}{Name: "SunYuzhang"},
	}

	bytes, e := json.Marshal(doc)

	fields, types, e := im.MapDocument(bytes, "")
	if e != nil {
		t.Fatal(e)
	}

	assert.True(t, len(types) == 0)

	var name *vearchpb.Field
	var youyouName *vearchpb.Field

	for _, f := range fields {
		if f.Name == "name" {
			name = f
		}
		if f.Name == "youyou.name" {
			youyouName = f
		}

	}

	assert.True(t, name != nil)
	assert.True(t, youyouName != nil)

}
