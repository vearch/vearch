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
	"context"
	"encoding/json"
	"github.com/vearch/vearch/proto/entity"
	"github.com/vearch/vearch/proto/pspb"
	"github.com/vearch/vearch/ps/engine/sortorder"
	"github.com/vearch/vearch/util/cbjson"
)

func NewErrDocResult(id string, err error) *DocResult {
	return &DocResult{
		Id:      id,
		Failure: NewEngineErr(err),
	}
}

func NewNotFoundDocResult(id string) *DocResult {
	return &DocResult{
		Id:    id,
		Found: false,
	}
}

type DocResult struct {
	Id         string              `json:"id,omitempty"`
	DB         entity.DBID         `json:"db"`
	Space      entity.SpaceID      `json:"space"`
	Partition  entity.PartitionID  `json:"pid"`
	Version    int64               `json:"version,omitempty"`
	Found      bool                `json:"found,omitempty"`
	Replace    bool                `json:"replace,omitempty"`
	Score      float64             `json:"score,omitempty"`
	SortValues sortorder.SortValues     `json:"sort_value,omitempty"`
	SlotID     uint32              `json:"slot_id"`
	Source     json.RawMessage     `json:"source,omitempty"`
	Extra      json.RawMessage     `json:"extra,omitempty"`
	Failure    *pspb.EngineFailure `json:"failure,omitempty"`
	Type       pspb.OpType         `json:"type"`
	Highlight  HighlightResult     `json:"highlight,omitempty"`
}

func (this *DocResult) ToContent(dbName, spaceName string) ([]byte, error) {
	var builder = cbjson.ContentBuilderFactory()

	builder.BeginObject()

	builder.Field("_index")
	builder.ValueString(dbName)

	builder.More()
	builder.Field("_type")
	builder.ValueString(spaceName)

	builder.More()
	builder.Field("_id")
	builder.ValueString(this.Id)

	builder.More()
	builder.Field("found")
	builder.ValueBool(this.Found)

	if this.Found {
		builder.More()
		builder.Field("_version")
		builder.ValueNumeric(this.Version)

		builder.More()
		builder.Field("_source")
		builder.ValueInterface(this.Source)
	}

	builder.EndObject()

	return builder.Output()
}

func NewDocStreamResult(ctx context.Context) *DocStreamResult {
	ctx2, cancel := context.WithCancel(ctx)
	return &DocStreamResult{
		ctx:        ctx2,
		cancel:     cancel,
		resultChan: make(chan *DocResult, 100),
		errChan:    make(chan error),
	}
}

type DocStreamResult struct {
	ctx        context.Context
	resultChan chan *DocResult
	errChan    chan error
	cancel     context.CancelFunc
	Count      int
	close      bool
}

func (sdr *DocStreamResult) Close() {
	sdr.cancel()
	close(sdr.resultChan)
	close(sdr.errChan)
}

func (sdr *DocStreamResult) Next() (*DocResult, error) {
	select {
	case result, ok := <-sdr.resultChan:
		if !ok {
			return nil, nil
		}
		sdr.Count++
		return result, nil
	case <-sdr.ctx.Done():
		return nil, sdr.ctx.Err()
	case err := <-sdr.errChan:
		return nil, err
	}
}

func (sdr *DocStreamResult) AddErr(e error) {
	sdr.errChan <- e
}

func (sdr *DocStreamResult) AddDoc(result *DocResult) {
	sdr.resultChan <- result
}

//TODO to delete it
func TestResult() *DocResult {
	return &DocResult{
		Id:        "123",
		DB:        1,
		Space:     1,
		Found:     true,
		Partition: 1,
		Version:   1,
		SlotID:    1,
	}
}
