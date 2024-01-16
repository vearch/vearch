# \DatabaseAPI

All URIs are relative to *http://localhost:80*

Method | HTTP request | Description
------------- | ------------- | -------------
[**CreateDatabase**](DatabaseAPI.md#CreateDatabase) | **Put** /db/_create | Create a new database
[**DeleteDB**](DatabaseAPI.md#DeleteDB) | **Delete** /db/{DB_NAME} | Delete a specific db



## CreateDatabase

> CreateDatabase(ctx).CreateDatabaseRequest(createDatabaseRequest).Execute()

Create a new database

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
	createDatabaseRequest := *openapiclient.NewCreateDatabaseRequest() // CreateDatabaseRequest | 

	configuration := openapiclient.NewConfiguration()
	apiClient := openapiclient.NewAPIClient(configuration)
	r, err := apiClient.DatabaseAPI.CreateDatabase(context.Background()).CreateDatabaseRequest(createDatabaseRequest).Execute()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error when calling `DatabaseAPI.CreateDatabase``: %v\n", err)
		fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
	}
}
```

### Path Parameters



### Other Parameters

Other parameters are passed through a pointer to a apiCreateDatabaseRequest struct via the builder pattern


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **createDatabaseRequest** | [**CreateDatabaseRequest**](CreateDatabaseRequest.md) |  | 

### Return type

 (empty response body)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: Not defined

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints)
[[Back to Model list]](../README.md#documentation-for-models)
[[Back to README]](../README.md)


## DeleteDB

> DeleteDB(ctx, dBNAME).Execute()

Delete a specific db

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
	dBNAME := "dBNAME_example" // string | The name of the database

	configuration := openapiclient.NewConfiguration()
	apiClient := openapiclient.NewAPIClient(configuration)
	r, err := apiClient.DatabaseAPI.DeleteDB(context.Background(), dBNAME).Execute()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error when calling `DatabaseAPI.DeleteDB``: %v\n", err)
		fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
	}
}
```

### Path Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
**ctx** | **context.Context** | context for authentication, logging, cancellation, deadlines, tracing, etc.
**dBNAME** | **string** | The name of the database | 

### Other Parameters

Other parameters are passed through a pointer to a apiDeleteDBRequest struct via the builder pattern


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------


### Return type

 (empty response body)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: Not defined

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints)
[[Back to Model list]](../README.md#documentation-for-models)
[[Back to README]](../README.md)

