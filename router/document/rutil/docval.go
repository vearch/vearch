package rutil

import (
	"fmt"
	"strings"
	"sync"

	"github.com/mmcloughlin/geohash"
	"github.com/spf13/cast"
)

type DocVal struct {
	FieldName string
	Path      []string
}

var pool = sync.Pool{
	New: func() interface{} {
		return &DocVal{}
	},
}

func GetDocVal() *DocVal {
	docV := pool.Get().(*DocVal)
	clean(docV)
	return docV
}

func PutDocVal(docV *DocVal) {
	clean(docV)
	pool.Put(docV)
}

func clean(doc *DocVal) {
	doc.FieldName = ""
	doc.Path = doc.Path[:0]
}

func ParseStringToGeoPoint(val string) (lat float64, lon float64, err error) {
	// Geo-point expressed as a string with the format: "lat,lon".
	ls := strings.Split(val, ",")
	if len(ls) == 2 {
		lat, err = cast.ToFloat64E(ls[0])
		if err != nil {
			return
		}
		lon, err = cast.ToFloat64E(ls[1])
		if err != nil {
			return
		}
	} else {
		// Geo-point expressed as a geohash.
		if len(val) != 12 {
			err = fmt.Errorf("invalid geohash %s", val)
			return
		}
		lat, lon = geohash.Decode(val)
	}
	return
}
