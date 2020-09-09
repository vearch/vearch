package rutil

import (
	"encoding/json"
	"fmt"
)

func RowDateToUInt8Array(data []byte, dimension int) ([]uint8, error) {

	if len(data) < dimension {
		return nil, fmt.Errorf("vector query length err, need feature num:[%d]", dimension)
	}

	var result []uint8

	if err := json.Unmarshal(data, &result); err != nil {
		return nil, err
	}

	return result, nil
}

func RowDateToFloatArray(data []byte, dimension int) ([]float32, error) {

	if len(data) < dimension {
		return nil, fmt.Errorf("vector query length err, need feature num:[%d]", dimension)
	}

	var result []float32

	if err := json.Unmarshal(data, &result); err != nil {
		return nil, err
	}

	if result != nil && (len(result)%dimension) != 0 {
		return nil, fmt.Errorf("vector query length err, not equals dimension multiple:[%d]", (len(result) % dimension))
	}

	return result, nil
}

//var empty = []byte{0}

/*func ToContent(sr *vearchpb.SearchResponse, nameCache response.NameCache, took time.Duration, idIsLong bool) ([]byte, error) {
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
		content, err := DocToContent(sr.Hits, nameCache, idIsLong)
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

func DocToContent(dh []*vearchpb.DocResult, nameCache response.NameCache, idIsLong bool) ([]byte, error) {
	var builder = cbjson.ContentBuilderFactory()
	for i, u := range dh {

		if i != 0 {
			builder.More()
		}
		builder.BeginObject()

		names := nameCache[[2]int64{int64(u.DB), int64(u.Space)}]

		builder.Field("_index")
		builder.ValueString(names[0])

		builder.More()
		builder.Field("_type")
		builder.ValueString(names[1])

		builder.More()
		builder.Field("_id")
		if idIsLong {
			idInt64, err := strconv.ParseInt(u.Id, 10, 64)
			if err == nil {
				builder.ValueNumeric(idInt64)
			}
		} else {
			builder.ValueString(u.Id)
		}

		if u.Found {
			builder.More()
			builder.Field("_score")
			builder.ValueFloat(float64(u.Score))

			if u.Extra != "" && len(u.Extra) > 0 {
				builder.More()
				builder.Field("_extra")
				builder.ValueInterface(u.Extra)
			}

			builder.More()
			builder.Field("_source")
			builder.ValueInterface("")
		}

		builder.EndObject()
	}

	return builder.Output()
}*/
