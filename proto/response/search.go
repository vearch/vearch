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

package response

import (
	"bytes"
	"math"
	"time"

	pkg "github.com/vearch/vearch/proto"
	sort "github.com/vearch/vearch/ps/engine/sortorder"
	"github.com/vearch/vearch/util/cbjson"
)

func NewSearchResponseErr(err error) *SearchResponse {
	return &SearchResponse{
		Status: &SearchStatus{
			Total:  1,
			Failed: 1,
			Errors: map[string]error{pkg.FormatErr(err): err},
		},
	}
}

type SearchResponses []*SearchResponse

type SearchResponse struct {
	PID       uint32            `json:"-"`
	Timeout   bool              `json:"time_out"`
	Status    *SearchStatus     `json:"status"`
	Hits      Hits              `json:"hits"`
	Total     uint64            `json:"total_hits"`
	MaxScore  float64           `json:"max_score"`
	MaxTook   int64             `json:"took"`
	MaxTookID uint32            `json:"max_took_id"`
	Explain   map[uint32]string `json:"explain,omitempty"`
}

// Merge will merge together multiple SearchResults during a MultiSearch, two args :[sortOrder, size]
func (sr *SearchResponse) Merge(other *SearchResponse, so sort.SortOrder, from, size int) (err error) {

	sr.Status.Merge(other.Status)

	sr.Total += other.Total
	if other.MaxScore > sr.MaxScore {
		sr.MaxScore = other.MaxScore
	}

	if other.MaxTook > sr.MaxTook {
		sr.MaxTook = other.MaxTook
		sr.MaxTookID = other.MaxTookID
	}

	sr.Timeout = sr.Timeout && other.Timeout

	if len(sr.Hits) > 0 || len(other.Hits) > 0 {
		sr.Hits = sr.Hits.Merge(sr.PID, other.PID, other.Hits, so, from, size)
	}

	if other.Explain != nil {
		if sr.Explain == nil {
			sr.Explain = other.Explain //impossibility
		}

		for k, v := range other.Explain {
			sr.Explain[k] = v
		}
	}

	return
}

type NameCache map[[2]int64][]string

func (srs SearchResponses) ToIDContent() []byte {
	sb := bytes.Buffer{}
	for i, sr := range srs {
		if i!=0{
			sb.WriteString("\n") ;
		}

		for j, hit := range sr.Hits{
			if j!=0{
				sb.WriteString(",") ;
			}
			sb.WriteString(hit.Id)
		}
	}
	return sb.Bytes()
}

func (srs SearchResponses) ToContent(from, size int, nameCache NameCache, typedKeys bool, took time.Duration) ([]byte, error) {
	var builder = cbjson.ContentBuilderFactory()

	builder.BeginObject()

	builder.Field("took")
	builder.ValueNumeric(int64(took) / 1e6)

	builder.More()

	builder.Field("results")

	builder.BeginArray()
	for i, sr := range srs {
		if bytes, err := sr.ToContent(from, size, nameCache, typedKeys, took); err != nil {
			return nil, err
		} else {
			builder.ValueRaw(string(bytes))
		}

		if i+1 < len(srs) {
			builder.More()
		}
	}
	builder.EndArray()

	builder.EndObject()

	return builder.Output()
}

func (sr *SearchResponse) ToContent(from, size int, nameCache NameCache, typedKeys bool, took time.Duration) ([]byte, error) {
	var builder = cbjson.ContentBuilderFactory()

	builder.BeginObject()

	builder.Field("took")
	builder.ValueNumeric(int64(took) / 1e6)

	builder.More()
	builder.Field("timed_out")
	builder.ValueBool(sr.Timeout)

	builder.More()
	builder.Field("_shards")
	builder.ValueInterface(sr.Status)

	builder.More()
	builder.BeginObjectWithField("hits")

	builder.Field("total")
	builder.ValueUNumeric(sr.Total)

	builder.More()
	builder.Field("max_score")
	builder.ValueFloat(sr.MaxScore)

	if sr.Hits != nil {
		builder.More()
		builder.BeginArrayWithField("hits")
		content, err := sr.Hits.ToContent(nameCache, from, size)
		if err != nil {
			return nil, err
		}
		builder.ValueRaw(string(content))

		builder.EndArray()
	}

	builder.EndObject()

	if sr.Explain != nil && len(sr.Explain) > 0 {
		builder.More()
		builder.Field("_explain")
		builder.ValueInterface(sr.Explain)
	}

	builder.EndObject()

	return builder.Output()

}

type Hits []*DocResult

func (dh Hits) Merge(spid, fpid uint32, sh Hits, so sort.SortOrder, from, size int) Hits {

	result := make([]*DocResult, 0, int(math.Min(float64(len(sh)+len(dh)), float64(from+size))))

	var d, s, c int

	var dd, sd *DocResult

	for i := 0; i < size; i++ {
		dd = dh.next(d)
		sd = sh.next(s)

		if dd == nil && sd == nil {
			break
		}

		if dd == nil {
			s++
			result = append(result, sd)
		} else if sd == nil {
			d++
			result = append(result, dd)
		} else {
			c = so.Compare(dd.SortValues, sd.SortValues)
			if c == 0 {
				if spid > fpid { // if compare is same , so sort by partition id
					c = -1
				} else {
					c = 1
				}
			}

			if c < 0 {
				d++
				result = append(result, dd)
			} else {
				s++
				result = append(result, sd)
			}
		}
	}

	return result
}

func (dh Hits) next(i int) *DocResult {
	if len(dh) <= i {
		return nil
	}

	return dh[i]

}

func (dh Hits) ToContent(nameCache NameCache, from, size int) ([]byte, error) {
	var builder = cbjson.ContentBuilderFactory()
	for i, u := range dh {

		if i != 0 {
			builder.More()
		}
		builder.BeginObject()

		names := nameCache[[2]int64{u.DB, u.Space}]

		builder.Field("_index")
		builder.ValueString(names[0])

		builder.More()
		builder.Field("_type")
		builder.ValueString(names[1])

		builder.More()
		builder.Field("_id")
		builder.ValueString(u.Id)

		if u.Found {
			builder.More()
			builder.Field("_score")
			builder.ValueFloat(u.Score)

			if u.Extra != nil && len(u.Extra) > 0 {
				builder.More()
				builder.Field("_extra")
				builder.ValueInterface(u.Extra)
			}

			builder.More()
			builder.Field("_version")
			builder.ValueNumeric(u.Version)

			builder.More()
			builder.Field("_source")
			builder.ValueInterface(u.Source)
		}

		if u.Highlight != nil {
			builder.More()
			builder.BeginObjectWithField("highlight")
			highlightCount := 0
			for k, vArray := range u.Highlight {
				builder.BeginArrayWithField(k)
				for i := 0; i < len(vArray); i++ {
					builder.ValueString(vArray[i])
					if i != len(vArray)-1 {
						builder.More()
					}
				}
				builder.EndArray()

				if highlightCount != len(u.Highlight)-1 {
					builder.More()
				}
				highlightCount++
			}
			builder.EndObject()
		}

		builder.EndObject()
	}

	return builder.Output()
}

// IndexErrMap tracks errors with the name of the index where it occurred
type IndexErrMap map[string]error

type SearchStatus struct {
	Total      int         `json:"total"`
	Failed     int         `json:"failed"`
	Successful int         `json:"successful"`
	Errors     IndexErrMap `json:"errors,omitempty"`
}

// Merge will merge together multiple SearchStatuses during a MultiSearch
func (ss *SearchStatus) Merge(other *SearchStatus) {
	ss.Total += other.Total
	ss.Failed += other.Failed
	ss.Successful += other.Successful
	if len(other.Errors) > 0 {
		if ss.Errors == nil {
			ss.Errors = make(map[string]error)
		}
		for otherIndex, otherError := range other.Errors {
			ss.Errors[otherIndex] = otherError
		}
	}
}
