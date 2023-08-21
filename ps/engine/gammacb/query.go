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
	"encoding/json"

	"github.com/vearch/vearch/engine/sdk/go/gamma"
	"github.com/vearch/vearch/proto/vearchpb"
	"github.com/vearch/vearch/ps/engine/mapping"
)

type queryBuilder struct {
	mapping *mapping.IndexMapping
}

type VectorQuery struct {
	Field        string          `json:"field"`
	FeatureData  json.RawMessage `json:"feature"`
	Feature      []float32       `json:"-"`
	FeatureUint8 []uint8         `json:"-"`
	Symbol       string          `json:"symbol"`
	Value        *float64        `json:"value"`
	Boost        *float64        `json:"boost"`
	Format       *string         `json:"format,omitempty"`
	MinScore     *float64        `json:"min_score,omitempty"`
	MaxScore     *float64        `json:"max_score,omitempty"`
}

func (qb *queryBuilder) parseQuery(data *vearchpb.SearchRequest, req *gamma.Request) error {
	vqs := make([]gamma.VectorQuery, len(data.VecFields))
	tfs := make([]gamma.TermFilter, len(data.TermFilters))
	rfs := make([]gamma.RangeFilter, len(data.RangeFilters))

	if len(data.Fields) > 0 {
		req.Fields = data.Fields
	}

	if len(data.VecFields) > 0 {
		for i, q := range data.VecFields {
			vq := gamma.VectorQuery{Name: q.Name, Value: q.Value, MinScore: q.MinScore, MaxScore: q.MaxScore, Boost: q.Boost, HasBoost: q.HasBoost}
			vqs[i] = vq
		}
	}

	if len(data.TermFilters) > 0 {
		for i, q := range data.TermFilters {
			tf := gamma.TermFilter{Field: q.Field, Value: q.Value, IsUnion: q.IsUnion}
			tfs[i] = tf
		}
	}

	if len(data.RangeFilters) > 0 {
		for i, q := range data.RangeFilters {
			rf := gamma.RangeFilter{Field: q.Field, LowerValue: q.LowerValue, UpperValue: q.UpperValue, IncludeLower: q.IncludeLower, IncludeUpper: q.IncludeUpper}
			rfs[i] = rf
		}
	}
	req.VecFields = vqs
	req.TermFilters = tfs
	req.RangeFilters = rfs
	return nil
}
