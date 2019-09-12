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

package init

import (
	"github.com/vearch/vearch/proto/request"
	"github.com/vearch/vearch/proto/response"
	"github.com/vearch/vearch/ps/engine/sortorder"
	"github.com/vearch/vearch/util/metrics/mserver"
	"github.com/vmihailenco/msgpack"
)

//in serizable by rpc you must set your entity in this init file
func init() {
	list := []interface{}{

		//req resp serizable
		(*request.SearchRequest)(nil),
		(*request.ObjRequest)(nil),
		(*response.SearchResponse)(nil),
		(response.SearchResponses)(nil),
		(*response.WriteResponse)(nil),
		(*response.ObjResponse)(nil),
		(*mserver.ServerStats)(nil),
		(*response.DocResult)(nil),

		//sort
		(*sortorder.FloatSortValue)(nil),
		(*sortorder.StringSortValue)(nil),
		(*sortorder.GeoDistanceSortValue)(nil),
		(*sortorder.InfinitySortValue)(nil),
		(*sortorder.IntSortValue)(nil),
		(*sortorder.DateSortValue)(nil),
	}

	for i, v := range list {
		if int8(i+1) < 0 {
			panic("fuck too may serizables ")
		}
		msgpack.RegisterExt(int8(i+1), v)
	}
}
