package vearch_client_test

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	openapiclient "github.com/vearch/vearch/sdk/go"
)

type Document struct {
	ID     string  `json:"_id"`
	String string  `json:"string"`
	Int    int     `json:"int"`
	Float  float32 `json:"float"`
	Vector Vector  `json:"vector"`
}

type Vector struct {
	Feature []float32 `json:"feature"`
}

var routerURL string

func init() {
	routerURL = "http://127.0.0.1:9001"
}

func Test_vearch_client_APIService(t *testing.T) {
	configuration := openapiclient.NewConfiguration()
	configuration.Scheme = "http"
	configuration.Debug = true
	configuration.Servers = openapiclient.ServerConfigurations{
		{
			URL:         routerURL,
			Description: "Development server",
		},
	}
	dbName := "db"
	spaceName := "ts_space"

	apiClient := openapiclient.NewAPIClient(configuration)

	t.Run("Test DatabaseAPI CreateDatabase", func(t *testing.T) {
		req := *openapiclient.NewCreateDatabaseRequest()
		req.SetName(dbName)
		httpRes, err := apiClient.DatabaseAPI.CreateDatabase(context.Background()).CreateDatabaseRequest(req).Execute()
		require.Nil(t, err)
		assert.Equal(t, 200, httpRes.StatusCode)
	})

	t.Run("Test SpaceAPIService CreateSpace", func(t *testing.T) {
		req := *openapiclient.NewCreateSpaceRequest()
		req.SetName(spaceName)
		req.SetPartitionNum(1)
		req.SetReplicaNum(1)

		engine := openapiclient.NewCreateSpaceRequestEngine()
		engine.IndexSize = openapiclient.PtrInt32(10000)
		engine.RetrievalType = openapiclient.PtrString("IVFPQ")
		engine.RetrievalParam = map[string]interface{}{
			"metric_type": "InnerProduct",
			"ncentroids":  128,
			"nsubvector":  16,
		}

		properties := map[string]openapiclient.CreateSpaceRequestPropertiesValue{
			"string": {
				Type:  openapiclient.PtrString("keyword"),
				Index: openapiclient.PtrBool(true),
			},
			"int": {
				Type:  openapiclient.PtrString("integer"),
				Index: openapiclient.PtrBool(true),
			},
			"float": {
				Type:  openapiclient.PtrString("float"),
				Index: openapiclient.PtrBool(true),
			},
			"vector": {
				Type:      openapiclient.PtrString("vector"),
				Index:     openapiclient.PtrBool(true),
				Dimension: openapiclient.PtrInt32(128),
				StoreType: openapiclient.PtrString("MemoryOnly"),
				Format:    openapiclient.PtrString("normalization"),
			},
		}
		req.Engine = engine
		req.Properties = &properties
		httpRes, err := apiClient.SpaceAPI.CreateSpace(context.Background(), dbName).CreateSpaceRequest(req).Execute()
		require.Nil(t, err)
		assert.Equal(t, 200, httpRes.StatusCode)
	})

	t.Run("Test DocumentAPIService Upsert", func(t *testing.T) {
		fileName := "test_data.json"
		file, err := os.Open(fileName)
		require.Nil(t, err)
		defer file.Close()

		req := *openapiclient.NewUpsertRequest()
		req.SetDbName(dbName)
		req.SetSpaceName(spaceName)

		scanner := bufio.NewScanner(file)

		for scanner.Scan() {
			line := scanner.Text()
			var doc Document

			err := json.Unmarshal([]byte(line), &doc)
			if err != nil {
				fmt.Printf("Error unmarshaling JSON: %v\n", err)
				continue
			}

			docJSON, err := json.Marshal(doc)
			if err != nil {
				fmt.Printf("Error marshaling Document to JSON: %v\n", err)
				continue
			}

			var docMap map[string]interface{}
			err = json.Unmarshal(docJSON, &docMap)
			if err != nil {
				fmt.Printf("Error unmarshaling JSON to map: %v\n", err)
				continue
			}

			var documents []map[string]interface{}
			documents = append(documents, docMap)

			req.SetDocuments(documents)
			_, httpRes, err := apiClient.DocumentAPI.Upsert(context.Background()).UpsertRequest(req).Execute()
			require.Nil(t, err)
			assert.Equal(t, 200, httpRes.StatusCode)
		}

		if err := scanner.Err(); err != nil {
			fmt.Printf("Error reading lines: %v\n", err)
		}
	})

	t.Run("Test DocumentAPIService Search", func(t *testing.T) {
		v := openapiclient.SearchDocumentsRequest{}
		v.SetDbName(dbName)
		v.SetSpaceName(spaceName)
		v.SetSize(10)
		query := openapiclient.SearchDocumentsRequestQuery{}
		feat := openapiclient.NewSearchDocumentsRequestQueryVectorInner("vector", []float32{0.31051028, 0.96709436, 0.30954844, 0.41732585, 0.40673676, -0.9581764, 0.7916723, 0.015181277, 0.9047762, 0.042921644, 0.34785905, 0.8399474, 0.12436352, 0.8349475, 0.0076812063, 0.37026754, 0.64320403, 0.79112536, 0.34686387, 0.8220929, 0.5991942, 0.41358098, 0.9116137, 0.22891758, 0.35076514, 0.30380213, 0.67152655, 0.9631394, 0.8079411, 0.9477603, 0.95325875, 0.18159084, -0.831241, 0.8553103, 0.27349475, 0.9097613, 0.49863246, 0.9758953, 0.0449907, 0.95190614, 0.535791, 0.865175, 0.5785509, 0.5506158, 0.5098654, 0.490794, 0.26249066, 0.37148666, 0.41575196, 0.9016276, 0.27392453, 0.8554484, 0.9891321, 0.17094603, 0.53345144, 0.81359076, 0.25720754, 0.33509436, 0.0048797824, 0.22957359, 0.9499374, 0.9626242, 0.71266264, 0.4075321, 0.34746838, 0.5975662, 0.8328559, 0.93670756, 0.7203131, 0.45015392, 0.47249204, 0.36233148, 0.04786835, 0.7562102, 0.22099885, 0.39084244, 0.14968549, 0.7655885, 0.45544398, 0.08196947, 0.2663818, -0.09907018, 0.15920356, 0.6314766, 0.9675052, 0.3381811, 0.83746594, 0.5022208, 0.4630395, 0.7440726, 0.24841698, 0.4237022, 0.65684825, 0.6367909, 0.38686144, 0.12636437, 0.5113392, 0.9946064, 0.727319, 0.51402354, 0.5456026, -0.29328436, 0.14177877, 0.63134825, 0.001411544, 0.88779205, 0.59617865, 0.8298384, 0.06593483, 0.33551803, 0.6190297, 0.32464412, 0.08418688, 0.6875682, 0.022131132, 0.07824574, 0.2285752, 0.27204487, 0.66183007, 0.9687446, 0.36440006, 0.20617528, 0.7664313, 0.28790948, 0.45220393, 0.38905638, 0.0075284494, 0.0044893017})
		query.SetVector([]openapiclient.SearchDocumentsRequestQueryVectorInner{*feat})
		v.SetQuery(query)
		_, httpRes, err := apiClient.DocumentAPI.SearchDocuments(context.Background()).SearchDocumentsRequest(v).Execute()
		require.Nil(t, err)
		assert.Equal(t, 200, httpRes.StatusCode)
	})

	t.Run("Test DocumentAPIService Delete", func(t *testing.T) {
		v := openapiclient.DeleteDocumentsRequestQuery{}
		v.DocumentIds = append(v.DocumentIds, "1")
		req := *openapiclient.NewDeleteDocumentsRequest()
		req.SetDbName(dbName)
		req.SetSpaceName(spaceName)
		req.SetQuery(v)
		_, httpRes, err := apiClient.DocumentAPI.DeleteDocuments(context.Background()).DeleteDocumentsRequest(req).Execute()
		require.Nil(t, err)
		assert.Equal(t, 200, httpRes.StatusCode)
	})

	t.Run("Test SpaceAPI DeleteSpace", func(t *testing.T) {
		httpRes, err := apiClient.SpaceAPI.DeleteSpace(context.Background(), dbName, spaceName).Execute()
		require.Nil(t, err)
		assert.Equal(t, 200, httpRes.StatusCode)
	})

	t.Run("Test DatabaseAPI DeleteDB", func(t *testing.T) {
		httpRes, err := apiClient.DatabaseAPI.DeleteDB(context.Background(), dbName).Execute()
		require.Nil(t, err)
		assert.Equal(t, 200, httpRes.StatusCode)
	})
}
