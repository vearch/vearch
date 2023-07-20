package rutil

import (
	"sync"
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
