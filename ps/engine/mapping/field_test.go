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
	"reflect"
	"testing"
	"time"

	"github.com/vearch/vearch/proto/vearchpb"
)

func TestProcessString(t *testing.T) {
	m := NewIndexMapping()
	ctx := m.newWalkContext("false")

	fms := []FieldMappingI{
		NewTextFieldMapping("text"),
		NewKeywordFieldMapping("keyword"),
		NewFloatFieldMapping("float"),
		NewIntegerFieldMapping("int"),
		NewDateFieldMapping("time"),
		NewGeoPointFieldMapping("geo"),
		NewGeoPointFieldMapping("geo_hash"),
		NewDateFieldMapping("time_1"),
		NewDateFieldMapping("time_2"),
		NewDateFieldMapping("time_3"),
	}
	fns := []string{
		"text", "keyword", "float", "int", "time", "geo", "geo_hash", "time_1", "time_2", "time_3",
	}
	tt := time.Now()
	ttS := tt.UTC().Format(time.RFC3339Nano)

	fvs := []string{
		"hello", "hello", "12.4", "12", ttS, "41.12,-71.34", "drm3btev3e86", "1900-01-01", "1900-01-01 12:12:12", "1900-01-01 12:12:12.123",
	}
	lat, lon, err := parseStringToGeoPoint("drm3btev3e86")
	if err != nil {
		t.Fatal(err)
	}

	time_1, err := time.Parse("2006-01-02", "1900-01-01")
	if err != nil {
		t.Fatal(err)
	}
	time_2, err := time.Parse("2006-01-02 15:04:05", "1900-01-01 12:12:12")
	if err != nil {
		t.Fatal(err)
	}
	time_3, err := time.Parse("2006-01-02 15:04:05.999999999", "1900-01-01 12:12:12.123")
	if err != nil {
		t.Fatal(err)
	}

	expectFs := []*vearchpb.Field{
		{
			Name:   fns[0],
			Type:   vearchpb.FieldType_TEXT,
			Value:  &vearchpb.FieldValue{Text: "hello"},
			Option: fms[0].Options(),
		},
		{
			Name:   fns[1],
			Type:   vearchpb.FieldType_KEYWORD,
			Value:  &vearchpb.FieldValue{Text: "hello"},
			Option: fms[1].Options(),
		},
		{
			Name:   fns[2],
			Type:   vearchpb.FieldType_FLOAT,
			Value:  &vearchpb.FieldValue{Float: 12.4},
			Option: fms[2].Options(),
		},
		{
			Name:   fns[3],
			Type:   vearchpb.FieldType_INT,
			Value:  &vearchpb.FieldValue{Int: 12},
			Option: fms[3].Options(),
		},
		{
			Name: fns[4],
			Type: vearchpb.FieldType_DATE,
			// Value:  []byte(numeric.MustNewPrefixCodedInt64(tt.UTC().UnixNano(), 0)),
			Value:  &vearchpb.FieldValue{Time: &vearchpb.TimeStamp{Usec: tt.UTC().UnixNano()}},
			Option: fms[4].Options(),
		},
		{
			Name: fns[5],
			Type: vearchpb.FieldType_GEOPOINT,
			// Value:  []byte(numeric.MustNewPrefixCodedInt64(int64(geo.MortonHash(-71.34, 41.12)), 0)),
			Value:  &vearchpb.FieldValue{Geo: &vearchpb.Geo{Lon: -71.34, Lat: 41.12}},
			Option: fms[5].Options(),
		},
		{
			Name: fns[6],
			Type: vearchpb.FieldType_GEOPOINT,
			// Value:  []byte(numeric.MustNewPrefixCodedInt64(int64(geo.MortonHash(lon, lat)), 0)),
			Value:  &vearchpb.FieldValue{Geo: &vearchpb.Geo{Lon: lon, Lat: lat}},
			Option: fms[6].Options(),
		},
		// "1900-01-01", "1900-01-01 12:12:12", "1900-01-01 12:12:12.123",
		{
			Name:   fns[7],
			Type:   vearchpb.FieldType_DATE,
			Value:  &vearchpb.FieldValue{Time: &vearchpb.TimeStamp{Usec: time_1.UnixNano()}},
			Option: fms[7].Options(),
		},
		{
			Name:   fns[8],
			Type:   vearchpb.FieldType_DATE,
			Value:  &vearchpb.FieldValue{Time: &vearchpb.TimeStamp{Usec: time_2.UnixNano()}},
			Option: fms[8].Options(),
		},
		{
			Name:   fns[9],
			Type:   vearchpb.FieldType_DATE,
			Value:  &vearchpb.FieldValue{Time: &vearchpb.TimeStamp{Usec: time_3.UnixNano()}},
			Option: fms[9].Options(),
		},
	}
	for i, fm := range fms {
		field, err := processString(ctx, NewFieldMapping(fm.FieldName(), fm), fns[i], fvs[i])
		if err != nil {
			t.Fatal(err)
		}
		if field == nil {
			t.Fatal("processString failed")
		}
		if !reflect.DeepEqual(field, expectFs[i]) {
			t.Fatalf("processString failed %d %v %v", i, field, expectFs[i])
		}
	}
}

func TestProcessNumber(t *testing.T) {
	m := NewIndexMapping()
	ctx := m.newWalkContext("false")

	fms := []FieldMappingI{
		NewDateFieldMapping("time_float"),
	}
	fns := []string{
		"time_float",
	}
	t5 := time.Unix(0, int64(123)*1e6).UTC().UnixNano()

	fvs := []float64{
		123,
	}

	expectFs := []*vearchpb.Field{
		{
			Name:   fns[0],
			Type:   vearchpb.FieldType_DATE,
			Value:  &vearchpb.FieldValue{Time: &vearchpb.TimeStamp{Usec: t5}},
			Option: fms[0].Options(),
		},
	}
	for i, fm := range fms {
		field, err := processNumber(ctx, NewFieldMapping(fm.FieldName(), fm), fns[i], fvs[i])
		if err != nil {
			t.Fatal(err)
		}
		if field == nil {
			t.Fatal("processNumber failed")
		}
		if !reflect.DeepEqual(field, expectFs[i]) {
			t.Fatalf("processNumber failed %d %v %v", i, field, expectFs[i])
		}
	}
}
