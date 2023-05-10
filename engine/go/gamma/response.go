/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

package gamma

import (
	"../../idl/fbs-gen/go/gamma_api"
	flatbuffers "github.com/google/flatbuffers/go"
)

type SearchResultCode uint8

const (
	SUCCESS           SearchResultCode = 0
	INDEX_NOT_TRAINED SearchResultCode = 1
	SEARCH_ERROR      SearchResultCode = 2
)

type Attribute struct {
	Name  string
	Value []byte
}

type ResultItem struct {
	Score      float64
	Attributes []Attribute
	Extra      string
}

type SearchResult struct {
	Total       int32
	ResultCode  SearchResultCode
	Msg         string
	ResultItems []ResultItem
}

type Response struct {
	Results          []SearchResult
	OnlineLogMessage string
	response         *gamma_api.Response
}

func (response *Response) Serialize(buffer *[]byte) int {
	builder := flatbuffers.NewBuilder(0)
	onlineLogMessage := builder.CreateString(response.OnlineLogMessage)

	var results []flatbuffers.UOffsetT
	results = make([]flatbuffers.UOffsetT, len(response.Results))

	for i := 0; i < len(response.Results); i++ {
		var resultItems []flatbuffers.UOffsetT
		resultItems = make([]flatbuffers.UOffsetT, len(response.Results[i].ResultItems))

		for j := 0; j < len(response.Results[i].ResultItems); j++ {
			var attributes []flatbuffers.UOffsetT
			attributes = make([]flatbuffers.UOffsetT, len(response.Results[i].ResultItems[j].Attributes))

			for k := 0; k < len(response.Results[i].ResultItems[j].Attributes); k++ {
				gamma_api.AttributeStartValueVector(builder, len(response.Results[i].ResultItems[j].Attributes[k].Value))
				for l := 0; l < len(response.Results[i].ResultItems[j].Attributes[k].Value); l++ {
					builder.PrependByte(response.Results[i].ResultItems[j].Attributes[k].Value[l])
				}
				value := builder.EndVector(len(response.Results[i].ResultItems[j].Attributes[k].Value))
				name := builder.CreateString(response.Results[i].ResultItems[j].Attributes[k].Name)
				gamma_api.AttributeStart(builder)
				gamma_api.AttributeAddName(builder, name)
				gamma_api.AttributeAddValue(builder, value)
				attributes[k] = gamma_api.AttributeEnd(builder)
			}

			gamma_api.ResultItemStartAttributesVector(builder, len(response.Results[i].ResultItems[j].Attributes))
			for i := 0; i < len(response.Results[i].ResultItems[j].Attributes); i++ {
				builder.PrependUOffsetT(attributes[i])
			}
			attrs := builder.EndVector(len(response.Results[i].ResultItems[j].Attributes))

			extra := builder.CreateString(response.Results[i].ResultItems[j].Extra)

			gamma_api.ResultItemStart(builder)
			gamma_api.ResultItemAddScore(builder, response.Results[i].ResultItems[j].Score)
			gamma_api.ResultItemAddAttributes(builder, attrs)
			gamma_api.ResultItemAddExtra(builder, extra)
			resultItems[j] = gamma_api.ResultItemEnd(builder)
		}

		gamma_api.SearchResultStartResultItemsVector(builder, len(response.Results[i].ResultItems))
		for i := 0; i < len(response.Results[i].ResultItems); i++ {
			builder.PrependUOffsetT(resultItems[i])
		}
		r := builder.EndVector(len(response.Results[i].ResultItems))

		msg := builder.CreateString(response.Results[i].Msg)
		gamma_api.SearchResultStart(builder)
		gamma_api.SearchResultAddTotal(builder, response.Results[i].Total)
		gamma_api.SearchResultAddResultCode(builder, int8(response.Results[i].ResultCode))
		gamma_api.SearchResultAddResultItems(builder, r)
		gamma_api.SearchResultAddMsg(builder, msg)
		results[i] = gamma_api.SearchResultEnd(builder)
	}

	gamma_api.ResponseStartResultsVector(builder, len(response.Results))
	for i := 0; i < len(response.Results); i++ {
		builder.PrependUOffsetT(results[i])
	}
	r := builder.EndVector(len(response.Results))

	gamma_api.ResponseStart(builder)
	gamma_api.ResponseAddResults(builder, r)
	gamma_api.ResponseAddOnlineLogMessage(builder, onlineLogMessage)

	builder.Finish(builder.EndObject())

	bufferLen := len(builder.FinishedBytes())
	*buffer = make([]byte, bufferLen)
	copy(*buffer, builder.FinishedBytes())
	return bufferLen
}

func (response *Response) DeSerialize(buffer []byte) {
	response.response = gamma_api.GetRootAsResponse(buffer, 0)
	response.OnlineLogMessage = string(response.response.OnlineLogMessage())
	response.Results = make([]SearchResult, response.response.ResultsLength())

	for i := 0; i < response.response.ResultsLength(); i++ {
		var result gamma_api.SearchResult

		response.response.Results(&result, i)
		response.Results[i].Total = result.Total()
		response.Results[i].ResultCode = SearchResultCode(result.ResultCode())
		response.Results[i].Msg = string(result.Msg())

		response.Results[i].ResultItems = make([]ResultItem, result.ResultItemsLength())

		for j := 0; j < result.ResultItemsLength(); j++ {
			var item gamma_api.ResultItem
			result.ResultItems(&item, j)

			response.Results[i].ResultItems[j].Score = item.Score()
			response.Results[i].ResultItems[j].Extra = string(item.Extra())
			response.Results[i].ResultItems[j].Attributes = make([]Attribute, item.AttributesLength())

			for k := 0; k < item.AttributesLength(); k++ {
				var attrs gamma_api.Attribute
				item.Attributes(&attrs, k)

				response.Results[i].ResultItems[j].Attributes[k].Name = string(attrs.Name())
				response.Results[i].ResultItems[j].Attributes[k].Value = attrs.ValueBytes()
			}
		}
	}
}