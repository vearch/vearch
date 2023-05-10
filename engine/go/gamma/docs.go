/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

package gamma

type Docs struct {
	DocArray []Doc
}

func (docs *Docs) AddDoc(doc Doc) int {
	docs.DocArray = append(docs.DocArray, doc)
	return 0
}

func (docs *Docs) Serialize(buffer *[][]byte) int {
	*buffer = make([][]byte, len(docs.DocArray))
	for i, doc := range docs.DocArray {
		doc.Serialize(&(*buffer)[i])
	}
	return len(docs.DocArray)
}

