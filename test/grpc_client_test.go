package test

import (
	"bufio"
	"bytes"
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/vearch/vearch/proto/vearchpb"
	"github.com/vearch/vearch/util/cbbytes"
	"google.golang.org/grpc"
)

type Docs []*vearchpb.Document
type DocsList []Docs

var docsList DocsList

var (
	batchNum    int
	instertNum  int
	address     string
	pathProfile string
	pathFeature string
)

func init() {
	flag.IntVar(&batchNum, "batch", 50, "the num of batch, default 50")
	flag.IntVar(&instertNum, "total", 1000, "the total num, default 1 million")
	flag.StringVar(&address, "address", "127.0.0.1:9002", "ip address path")
	flag.StringVar(&pathProfile, "p", "data/vearch/profile.txt", "profile path")
	flag.StringVar(&pathFeature, "f", "data/vearch/feature.dat", "Feature path")
}

func TestMain(m *testing.M) {
	docsList = getDocs()
	os.Exit(m.Run())
}

func getDocs() DocsList {
	flag.Parse()
	// GetTest(address, 100)
	if pathProfile == "" || pathFeature == "" {
		fmt.Println("please input -p for profile path and -f for feature path")
		os.Exit(1)
	}
	printArgs()
	basePath := os.Getenv("HOME")
	pathProfile = path.Join(basePath, pathProfile)
	pathFeature = path.Join(basePath, pathFeature)
	t0 := time.Now()
	fmt.Println("begin load data")
	docs := loadData()
	fmt.Println("finish load data", time.Now().Sub(t0))
	return docs
}

func printArgs() {
	fmt.Println("################### param #############################")
	fmt.Println(fmt.Sprintf("\t batchNum: [%d], instertNum: [%d]", batchNum, instertNum))
	fmt.Println(fmt.Sprintf("\t address: [%s]", address))
	fmt.Println(fmt.Sprintf("\t profile path: [%s]", pathProfile))
	fmt.Println(fmt.Sprintf("\t feature path: [%s]", pathFeature))
}

func loadData() DocsList {
	fileProfile, err := os.OpenFile(pathProfile, os.O_RDWR, 0666)
	if err != nil {
		panic(err)
	}
	defer fileProfile.Close()
	bufProfile := bufio.NewReader(fileProfile)

	fileFeature, err := os.OpenFile(pathFeature, os.O_RDWR, 0666)
	if err != nil {
		panic(err)
	}
	defer fileFeature.Close()
	bufFeature := bufio.NewReader(fileFeature)

	docsList := make(DocsList, 0)
	docs := make(Docs, 0)
	count := 0
	for {
		// load feature
		cbuf := &bytes.Buffer{}
		// if _, err = cbuf.Write(cbbytes.UInt32ToByte(uint32(512))); err != nil {
		// break
		// }
		code := make([]byte, 512)
		bufFeature.Read(code)
		cbuf.Write(code)
		cbuf.WriteString("")
		vec_byte := cbuf.Bytes()

		// load profile
		line, err := bufProfile.ReadString('\n')
		if err != nil {
			panic(err)
		}
		line = strings.TrimSpace(line)
		d := strings.Split(line, "\t")
		cid1, _ := strconv.ParseInt(d[2], 10, 32)

		// package field
		fileds := make([]*vearchpb.Field, 0)
		fileds = append(fileds, &vearchpb.Field{Name: "sku", Type: vearchpb.FieldType_STRING, Value: cbbytes.StringToByte(d[0])})
		fileds = append(fileds, &vearchpb.Field{Name: "url", Type: vearchpb.FieldType_STRING, Value: cbbytes.StringToByte(d[1])})
		fileds = append(fileds, &vearchpb.Field{Name: "cid1", Type: vearchpb.FieldType_INT, Value: cbbytes.Int32ToByte(int32(cid1))})
		fileds = append(fileds, &vearchpb.Field{Name: "vector", Type: vearchpb.FieldType_VECTOR, Value: vec_byte})
		// document := &vearchpb.Document{Fields: fileds}
		document := &vearchpb.Document{PKey: strconv.FormatInt(int64(count), 10), Fields: fileds}
		docs = append(docs, document)
		if count%batchNum == 0 && count > 0 {
			docsList = append(docsList, docs)
			docs = make(Docs, 0)
		}
		if count%10000 == 0 {
			fmt.Println("count is ", count)
		}
		if count == instertNum {
			break
		}
		count++
	}
	return docsList
}

func Bulk(addr string, docs DocsList) {
	conn, err := grpc.Dial(addr, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer func() {
		if err := conn.Close(); err != nil {
			panic(err)
		}
	}()
	api := vearchpb.NewRouterGRPCServiceClient(conn)
	wg := sync.WaitGroup{}
	reqChain := make(chan int, 20)
	t0 := time.Now()
	for _, d := range docs {
		wg.Add(1)
		reqChain <- 1
		go func(dd Docs) {
			defer func() {
				<-reqChain
				wg.Done()
			}()
			api.Bulk(context.Background(), &vearchpb.BulkRequest{
				Head: &vearchpb.RequestHead{
					TimeOutMs: 10000,
					DbName:    "ts_db",
					SpaceName: "ts_space",
				},
				Docs: dd,
			})
		}(d)
	}
	wg.Wait()
	log.Println(fmt.Sprintf("total cost %v", time.Now().Sub(t0)))
}

func TestBulk(t *testing.T) {
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		t.Fatalf("did not connect: %v", err)
	}
	defer func() {
		if err := conn.Close(); err != nil {
			t.Fatalf("close conn failed, err: [%v]", err)
		}
	}()
	api := vearchpb.NewRouterGRPCServiceClient(conn)
	for _, docs := range docsList {
		resps, err := api.Bulk(context.Background(), &vearchpb.BulkRequest{
			Head: &vearchpb.RequestHead{
				TimeOutMs: 10000,
				DbName:    "ts_db",
				SpaceName: "ts_space",
			},
			Docs: docs,
		})
		if err != nil {
			t.Errorf("rpc return failed, doc: [%v], resps: [%v], err: [%v]", docs, resps, err)
		}
		for _, item := range resps.Items {
			if item.Err != nil && item.Err.Code != vearchpb.ErrorEnum_SUCCESS {
				t.Errorf("rpc return failed, docs: [%v], item: [%v]", docs, item)
			}
		}
	}
}

// TestCreate test create doc
func TestCreate(t *testing.T) {
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		t.Fatalf("did not connect: %v", err)
	}
	defer func() {
		if err := conn.Close(); err != nil {
			t.Fatalf("close conn failed, err: [%v]", err)
		}
	}()
	api := vearchpb.NewRouterGRPCServiceClient(conn)
	for _, docs := range docsList {
		for _, doc := range docs {
			resps, err := api.Add(context.Background(), &vearchpb.AddRequest{
				Head: &vearchpb.RequestHead{
					TimeOutMs: 10000,
					DbName:    "ts_db",
					SpaceName: "ts_space",
				},
				Doc: doc,
			})
			if err != nil {
				t.Errorf("rpc return failed, doc: [%v], resps: [%v], err: [%v]", doc, resps, err)
			}
			if resps.Head.Err != nil && resps.Head.Err.Code != vearchpb.ErrorEnum_SUCCESS {
				t.Errorf("rpc return failed, doc: [%v], err: [%v]", doc, resps.Head.Err)
			}
		}
	}
}

// TestUpdate test update doc
func TestUpdate(t *testing.T) {
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		t.Fatalf("did not connect: %v", err)
	}
	defer func() {
		if err := conn.Close(); err != nil {
			t.Fatalf("close conn failed, err: [%v]", err)
		}
	}()
	api := vearchpb.NewRouterGRPCServiceClient(conn)
	for _, docs := range docsList {
		for _, doc := range docs {
			resps, err := api.Update(context.Background(), &vearchpb.UpdateRequest{
				Head: &vearchpb.RequestHead{
					TimeOutMs: 10000,
					DbName:    "ts_db",
					SpaceName: "ts_space",
				},
				Doc: doc,
			})
			if err != nil {
				t.Errorf("rpc return failed, doc: [%v], resps: [%v], err: [%v]", doc, resps, err)
			}
			if resps.Head.Err != nil && resps.Head.Err.Code != vearchpb.ErrorEnum_SUCCESS {
				t.Errorf("rpc return failed, doc: [%v], err: [%v]", doc, resps.Head.Err)
			}
		}
	}
}

// TestGet test get doc
func TestGet(t *testing.T) {
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		t.Fatalf("did not connect: %v", err)
	}
	defer func() {
		if err := conn.Close(); err != nil {
			t.Fatalf("close conn failed, err: [%v]", err)
		}
	}()
	api := vearchpb.NewRouterGRPCServiceClient(conn)
	for n := 0; n < instertNum; n++ {
		resps, err := api.Get(context.Background(), &vearchpb.GetRequest{
			Head: &vearchpb.RequestHead{
				TimeOutMs: 10000,
				DbName:    "ts_db",
				SpaceName: "ts_space",
			},
			PrimaryKeys: []string{strconv.FormatInt(int64(n), 10)},
		})
		if err != nil {
			t.Errorf("rpc return failed, key: [%d], resps: [%v], err: [%v]", n, resps, err)
		}
		for _, item := range resps.Items {
			if item.Err != nil && item.Err.Code != vearchpb.ErrorEnum_SUCCESS {
				t.Errorf("rpc return failed, key: [%d], item: [%v]", n, item)
			}
		}
	}
}

func TestSearch(t *testing.T) {
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		t.Fatalf("did not connect: %v", err)
	}
	defer func() {
		if err := conn.Close(); err != nil {
			t.Fatalf("close conn failed, %v", err)
		}
	}()

	api := vearchpb.NewRouterGRPCServiceClient(conn)

	n, m := 10, 10
	queries := makeSearchRequests(n, m)
	for i := 0; i < n; i++ {
		query := queries[i]
		resp, err := api.Search(context.Background(), query)
		if err != nil {
			t.Errorf("request failed, response: %v, err: %s", resp.String(), err.Error())
		} else {
			t.Logf("request success, response: %s", resp.String())
		}
	}
}

func TestMSearch(t *testing.T) {
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		t.Fatalf("did not connect: %v", err)
	}
	defer func() {
		if err := conn.Close(); err != nil {
			t.Fatalf("close conn failed, %v", err)
		}
	}()
	api := vearchpb.NewRouterGRPCServiceClient(conn)

	n, m := 10, 10
	queries := makeSearchRequests(n, m)
	for i := 0; i < n; i++ {
		begin, end := i*m, (i+1)*m
		query := &vearchpb.MSearchRequest{}
		query.SearchRequests = queries[begin:end]
		query.Head = &vearchpb.RequestHead{
			TimeOutMs: 10000,
			DbName:    "ts_db",
			SpaceName: "ts_space",
		}

		resp, err := api.MSearch(context.Background(), query)
		if err != nil {
			t.Errorf("request failed, response: %v, err: %s", resp.String(), err.Error())
		} else {
			t.Logf("request success, response: %s", resp.String())
		}
	}
}

func makeSearchRequests(n, m int) []*vearchpb.SearchRequest {
	queries := make([]*vearchpb.SearchRequest, 0)
	for _, docs := range docsList[0:n] {
		for _, doc := range docs[0:m] {
			vectorQuery := &vearchpb.VectorQuery{}
			for _, field := range doc.Fields {
				if field.Type == vearchpb.FieldType_VECTOR {
					vectorQuery.Name = field.Name
					vectorQuery.Value = field.Value
					vectorQuery.MinScore = 0.5
					vectorQuery.Boost = 0.5
					break
				}
			}
			// range filter
			rf := &vearchpb.RangeFilter{}
			// query
			query := &vearchpb.SearchRequest{}
			query.ReqNum = 10
			query.TopN = 10
			query.VecFields = []*vearchpb.VectorQuery{vectorQuery}
			query.Fields = []string{"sku", "url"}

			query.RangeFilters = []*vearchpb.RangeFilter{rf}
			query.IsBruteSearch = 0
			query.OnlineLogLevel = "debug"
			query.HasRank = false
			query.IsVectorValue = false
			query.ParallelBasedOnQuery = false
			query.L2Sqrt = false
			query.IvfFlat = false
			query.RetrievalParams = `{"nprobe" : 10, "metric_type" : "InnerProduct", "ncentroids" : 4096,"nsubvector" : 32}`
			query.MultiVectorRank = 0
			query.Head = &vearchpb.RequestHead{
				TimeOutMs: 10000,
				DbName:    "ts_db",
				SpaceName: "ts_space",
			}

			queries = append(queries, query)
		}
	}
	return queries
}

// TestDelete test delete doc
func TestDelete(t *testing.T) {
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		t.Fatalf("did not connect: %v", err)
	}
	defer func() {
		if err := conn.Close(); err != nil {
			t.Fatalf("close conn failed, err: [%v]", err)
		}
	}()
	api := vearchpb.NewRouterGRPCServiceClient(conn)
	for n := 0; n < instertNum; n++ {
		resps, err := api.Delete(context.Background(), &vearchpb.DeleteRequest{
			Head: &vearchpb.RequestHead{
				TimeOutMs: 10000,
				DbName:    "ts_db",
				SpaceName: "ts_space",
			},
			PrimaryKeys: []string{strconv.FormatInt(int64(n), 10)},
		})
		if err != nil {
			t.Errorf("rpc return failed, key: [%d], resps: [%v], err: [%v]", n, resps, err)
		}
		for _, item := range resps.Items {
			if item.Err != nil && item.Err.Code != vearchpb.ErrorEnum_SUCCESS {
				t.Errorf("rpc return failed, key: [%d], item: [%v]", n, item)
			}
		}
	}
}
