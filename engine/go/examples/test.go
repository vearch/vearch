/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

package main

import (
	gamma "../gamma"
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"golang.org/x/exp/mmap"
	"io"
	"math"
	"os"
	"strconv"
	"strings"
	"time"
	"unsafe"
)

type Options struct {
	Nprobe          int32
	DocID           uint64
	DocID2          uint64
	D               int
	MaxDocSize      int32
	AddDocNum       int
	SearchNum       uint64
	IndexingSize    int
	Filter          bool
	PrintDoc        bool
	SearchThreadNum int
	FieldsVec       []string
	FieldsType      []gamma.DataType
	Path            string
	LogDir          string
	VectorName      string
	ModelID         string
	RetrievalType   string
	StoreType       string
	Profiles        []string
	Feature         []byte
	ProfileFile     string
	FeatureFile     string
	FeatureMmap     *mmap.ReaderAt
	Engine          unsafe.Pointer
}

var opt *Options

func Init() {
	MaxDocSize := 10 * 10000
	FieldsVec := []string{"img", "_id", "field1", "field2", "field3"}
	opt = &Options{
		Nprobe:          256,
		DocID:           0,
		DocID2:          0,
		D:               512,
		MaxDocSize:      10 * 10000,
		AddDocNum:       10 * 10000,
		SearchNum:       10000,
		IndexingSize:    10 * 10000,
		Filter:          false,
		PrintDoc:        false,
		SearchThreadNum: 1,
		FieldsVec:       FieldsVec,
		FieldsType:      []gamma.DataType{gamma.STRING, gamma.STRING, gamma.STRING, gamma.INT, gamma.INT},
		Path:            "./files",
		LogDir:          "./log",
		VectorName:      "abc",
		ModelID:         "model",
		RetrievalType:   "IVFPQ",
		StoreType:       "RocksDB",
		Profiles:        make([]string, MaxDocSize*len(FieldsVec)),
	}

	if len(os.Args) != 3 {
		fmt.Println("Usage: [Program] [profile_file] [vectors_file]")
		os.Exit(-1)
	}
	fmt.Println(strings.Join(os.Args[1:], "\n"))
	opt.ProfileFile = os.Args[1]
	opt.FeatureFile = os.Args[2]

	opt.FeatureMmap, _ = mmap.Open(opt.FeatureFile)

	config := gamma.Config{Path: opt.Path, LogDir: opt.LogDir}
	opt.Engine = gamma.Init(&config)
	fmt.Println("Inited")
}

func CreteTable() {
	kIVFPQParam :=
		string("{\"nprobe\" : 10, \"metric_type\" : \"InnerProduct\", \"ncentroids\" : 256,\"nsubvector\" : 64}")

	//kHNSWParam_str :=
	//	"{\"nlinks\" : 32, \"metric_type\" : \"InnerProduct\", \"efSearch\" : 64,\"efConstruction\" : 40}"
	//
	//kFLATParam_str := "{\"metric_type\" : \"InnerProduct\"}"
	table := gamma.Table{
		Name:           opt.VectorName,
		IndexingSize:   10000,
		RetrievalType:  opt.RetrievalType,
		RetrievalParam: kIVFPQParam}

	for i := 0; i < len(opt.FieldsVec); i++ {
		isIndex := false
		if opt.Filter && (i == 0 || i == 2 || i == 3 || i == 4) {
			isIndex = true
		}
		fieldInfo := gamma.FieldInfo{
			Name:     opt.FieldsVec[i],
			DataType: opt.FieldsType[i],
			IsIndex:  isIndex,
		}
		table.Fields = append(table.Fields, fieldInfo)
	}

	vectorInfo := gamma.VectorInfo{
		Name:       opt.VectorName,
		DataType:   gamma.FLOAT,
		IsIndex:    true,
		Dimension:  int32(opt.D),
		ModelId:    opt.ModelID,
		StoreType:  opt.StoreType,
		StoreParam: string("{\"cache_size\": 2048}"),
		HasSource:  false,
	}
	table.VectorsInfos = append(table.VectorsInfos, vectorInfo)
	gamma.CreateTable(opt.Engine, &table)
}

func AddDocToEngine(docNum int) {
	for i := 0; i < docNum; i++ {
		var doc gamma.Doc
		var url string
		for j := 0; j < len(opt.FieldsVec); j++ {
			field := gamma.Field{
				Name:     opt.FieldsVec[j],
				Datatype: opt.FieldsType[j],
			}
			profileStr := opt.Profiles[i*len(opt.FieldsVec)+j]
			if field.Datatype == gamma.INT {
				tmp, _ := strconv.Atoi(profileStr)
				bytesBuffer := bytes.NewBuffer([]byte{})
				binary.Write(bytesBuffer, binary.LittleEndian, &tmp)
				field.Value = bytesBuffer.Bytes()
			} else if field.Datatype == gamma.LONG {
				tmp, _ := strconv.ParseInt(profileStr, 10, 64)
				bytesBuffer := bytes.NewBuffer([]byte{})
				binary.Write(bytesBuffer, binary.LittleEndian, &tmp)
				field.Value = bytesBuffer.Bytes()
			} else {
				field.Value = []byte(profileStr)
				url = profileStr
			}
			field.Source = url
			doc.Fields = append(doc.Fields, field)
		}

		field := gamma.Field{
			Name:     opt.VectorName,
			Datatype: gamma.VECTOR,
			Source:   "",
		}
		field.Value = make([]byte, opt.D*4)
		_, _ = opt.FeatureMmap.ReadAt(field.Value, int64(i*opt.D)*4)
		doc.Fields = append(doc.Fields, field)

		//for i := 0; i < opt.D; i++ {
		//	a := Float64frombytes(field.Value[i*4:])
		//	fmt.Println(a)
		//}

		gamma.AddOrUpdateDoc(opt.Engine, &doc)
	}
}

func BatchAddDocToEngine(docNum int) {
	batchSize := 100

	for i := 0; i < docNum; i += batchSize {
		var docs gamma.Docs
		for k := 0; k < batchSize; k++ {
			var doc gamma.Doc
			var url string
			for j := 0; j < len(opt.FieldsVec); j++ {
				field := gamma.Field{
					Name:     opt.FieldsVec[j],
					Datatype: opt.FieldsType[j],
				}
				profileStr := opt.Profiles[i*len(opt.FieldsVec)+j]
				if field.Datatype == gamma.INT {
					tmp, _ := strconv.Atoi(profileStr)
					bytesBuffer := bytes.NewBuffer([]byte{})
					binary.Write(bytesBuffer, binary.LittleEndian, &tmp)
					field.Value = bytesBuffer.Bytes()
				} else if field.Datatype == gamma.LONG {
					tmp, _ := strconv.ParseInt(profileStr, 10, 64)
					bytesBuffer := bytes.NewBuffer([]byte{})
					binary.Write(bytesBuffer, binary.LittleEndian, &tmp)
					field.Value = bytesBuffer.Bytes()
				} else {
					field.Value = []byte(profileStr)
					url = profileStr
				}
				field.Source = url
				doc.Fields = append(doc.Fields, field)
			}

			field := gamma.Field{
				Name:     opt.VectorName,
				Datatype: gamma.VECTOR,
				Source:   "",
			}
			field.Value = make([]byte, opt.D*4)
			_, _ = opt.FeatureMmap.ReadAt(field.Value, int64(i*opt.D)*4)
			doc.Fields = append(doc.Fields, field)

			docs.AddDoc(doc)
		}
		//for i := 0; i < opt.D; i++ {
		//	a := Float64frombytes(field.Value[i*4:])
		//	fmt.Println(a)
		//}

		//result := gamma.AddOrUpdateDocs(opt.Engine, &docs)
		//fmt.Println(len(result.Codes))
		gamma.AddOrUpdateDocs(opt.Engine, &docs)
	}
}

func Add() {
	file, err := os.OpenFile(opt.ProfileFile, os.O_RDWR, 0666)
	if err != nil {
		fmt.Println("Open file error!", err)
		return
	}
	defer file.Close()

	stat, err := file.Stat()
	if err != nil {
		panic(err)
	}

	var size = stat.Size()
	fmt.Println("file size=", size)

	buf := bufio.NewReader(file)
	for idx := 0; idx < opt.AddDocNum; idx++ {
		line, err := buf.ReadString('\n')
		line = strings.TrimSpace(line)
		if err != nil {
			if err == io.EOF {
				fmt.Println("File read ok!")
				break
			} else {
				fmt.Println("Read file error!", err)
				return
			}
		}

		docs := strings.Split(line, "\t")
		i := 0
		for _, doc := range docs {
			opt.Profiles[idx*len(opt.FieldsVec)+i] = doc
			i++
			if i > len(opt.FieldsVec)-1 {
				break
			}
		}
	}
	//AddDocToEngine(opt.AddDocNum)
	BatchAddDocToEngine(opt.AddDocNum)
}

func Load() {
	gamma.Load(opt.Engine)
}

func Dump() {
	gamma.Dump(opt.Engine)
}

func GetDoc() {
	//tmp, _ := strconv.ParseInt("100", 10, 64)
	//bytesBuffer := bytes.NewBuffer([]byte{})
	//binary.Write(bytesBuffer, binary.LittleEndian, &tmp)
	//docID := bytesBuffer.Bytes()
	docID := []byte("100")
	var doc gamma.Doc
	gamma.GetDocByID(opt.Engine, docID, &doc)
	fmt.Printf("doc [%v]", doc)
}

func Float64frombytes(bytes []byte) float32 {
	bits := binary.LittleEndian.Uint32(bytes)
	float := math.Float32frombits(bits)
	return float
}

func Search() {
	request := gamma.Request{
		ReqNum:               1,
		TopN:                 100,
		BruteForceSearch:     0,
		OnlineLogLevel:       "",
		MultiVectorRank:      0,
		ParallelBasedOnQuery: true,
		RetrievalParams:      "{\"metric_type\" : \"InnerProduct\", \"recall_num\" : 100, \"nprobe\" : 10, \"ivf_flat\" : 0}",
		L2Sqrt:               false,
		IvfFlat:              false,
	}
	request.VecFields = make([]gamma.VectorQuery, 1)
	request.VecFields[0] = gamma.VectorQuery{
		Name:     opt.VectorName,
		MinScore: 0,
		MaxScore: 1000,
		Boost:    0.1,
		HasBoost: 0,
	}

	request.VecFields[0].Value = make([]byte, opt.D*4)
	_, _ = opt.FeatureMmap.ReadAt(request.VecFields[0].Value, int64(0*opt.D)*4)

	//for i := 0; i < opt.D; i++ {
	//	a := Float64frombytes(request.VecFields[0].Value[i*4:])
	//	fmt.Println(a)
	//}

	var response gamma.Response
	gamma.Search(opt.Engine, &request, &response)
}

func main() {
	Init()
	defer gamma.Close(opt.Engine)
	//Load()
	//return
	fmt.Println(opt.FeatureFile)
	CreteTable()
	Add()
	GetDoc()
	time.Sleep(time.Duration(1) * time.Second)
	var status gamma.EngineStatus
	for status.IndexStatus != 2 {
		gamma.GetEngineStatus(opt.Engine, &status)
	}
	Search()

	Dump()
	opt.FeatureMmap.Close()
}
