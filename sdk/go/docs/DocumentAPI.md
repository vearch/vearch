# \DocumentAPI

All URIs are relative to *http://localhost:80*

Method | HTTP request | Description
------------- | ------------- | -------------
[**DeleteDocuments**](DocumentAPI.md#DeleteDocuments) | **Post** /document/delete | Delete documents from a space
[**SearchDocuments**](DocumentAPI.md#SearchDocuments) | **Post** /document/search | Search for documents using vector and filter parameters
[**Upsert**](DocumentAPI.md#Upsert) | **Post** /document/upsert | Upsert a document with dynamic fields into the Vearch database



## DeleteDocuments

> DeleteDocuments200Response DeleteDocuments(ctx).DeleteDocumentsRequest(deleteDocumentsRequest).Execute()

Delete documents from a space

### Example

```go
package main

import (
	"context"
	"fmt"
	"os"
	openapiclient "github.com/vearch/vearch/sdk/go"
)

func main() {
	deleteDocumentsRequest := *openapiclient.NewDeleteDocumentsRequest() // DeleteDocumentsRequest | 

	configuration := openapiclient.NewConfiguration()
	apiClient := openapiclient.NewAPIClient(configuration)
	resp, r, err := apiClient.DocumentAPI.DeleteDocuments(context.Background()).DeleteDocumentsRequest(deleteDocumentsRequest).Execute()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error when calling `DocumentAPI.DeleteDocuments``: %v\n", err)
		fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
	}
	// response from `DeleteDocuments`: DeleteDocuments200Response
	fmt.Fprintf(os.Stdout, "Response from `DocumentAPI.DeleteDocuments`: %v\n", resp)
}
```

### Path Parameters



### Other Parameters

Other parameters are passed through a pointer to a apiDeleteDocumentsRequest struct via the builder pattern


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **deleteDocumentsRequest** | [**DeleteDocumentsRequest**](DeleteDocumentsRequest.md) |  | 

### Return type

[**DeleteDocuments200Response**](DeleteDocuments200Response.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints)
[[Back to Model list]](../README.md#documentation-for-models)
[[Back to README]](../README.md)


## SearchDocuments

> SearchDocuments200Response SearchDocuments(ctx).SearchDocumentsRequest(searchDocumentsRequest).Execute()

Search for documents using vector and filter parameters

### Example

```go
package main

import (
	"context"
	"fmt"
	"os"
	openapiclient "github.com/vearch/vearch/sdk/go"
)

func main() {
	searchDocumentsRequest := *openapiclient.NewSearchDocumentsRequest(*openapiclient.NewSearchDocumentsRequestQuery([]openapiclient.SearchDocumentsRequestQueryVectorInner{*openapiclient.NewSearchDocumentsRequestQueryVectorInner("field_vector", []float32{float32(123)})}), *openapiclient.NewSearchDocumentsRequestRetrievalParam("L2"), int32(3), "ts_db", "ts_space") // SearchDocumentsRequest | 

	configuration := openapiclient.NewConfiguration()
	apiClient := openapiclient.NewAPIClient(configuration)
	resp, r, err := apiClient.DocumentAPI.SearchDocuments(context.Background()).SearchDocumentsRequest(searchDocumentsRequest).Execute()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error when calling `DocumentAPI.SearchDocuments``: %v\n", err)
		fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
	}
	// response from `SearchDocuments`: SearchDocuments200Response
	fmt.Fprintf(os.Stdout, "Response from `DocumentAPI.SearchDocuments`: %v\n", resp)
}
```

### Path Parameters



### Other Parameters

Other parameters are passed through a pointer to a apiSearchDocumentsRequest struct via the builder pattern


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **searchDocumentsRequest** | [**SearchDocumentsRequest**](SearchDocumentsRequest.md) |  | 

### Return type

[**SearchDocuments200Response**](SearchDocuments200Response.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints)
[[Back to Model list]](../README.md#documentation-for-models)
[[Back to README]](../README.md)


## Upsert

> Upsert200Response Upsert(ctx).UpsertRequest(upsertRequest).Execute()

Upsert a document with dynamic fields into the Vearch database

### Example

```go
package main

import (
	"context"
	"fmt"
	"os"
	openapiclient "github.com/vearch/vearch/sdk/go"
)

func main() {
	upsertRequest := *openapiclient.NewUpsertRequest() // UpsertRequest | 

	configuration := openapiclient.NewConfiguration()
	apiClient := openapiclient.NewAPIClient(configuration)
	resp, r, err := apiClient.DocumentAPI.Upsert(context.Background()).UpsertRequest(upsertRequest).Execute()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error when calling `DocumentAPI.Upsert``: %v\n", err)
		fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
	}
	// response from `Upsert`: Upsert200Response
	fmt.Fprintf(os.Stdout, "Response from `DocumentAPI.Upsert`: %v\n", resp)
}
```

### Path Parameters



### Other Parameters

Other parameters are passed through a pointer to a apiUpsertRequest struct via the builder pattern


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **upsertRequest** | [**UpsertRequest**](UpsertRequest.md) |  | 

### Return type

[**Upsert200Response**](Upsert200Response.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints)
[[Back to Model list]](../README.md#documentation-for-models)
[[Back to README]](../README.md)

