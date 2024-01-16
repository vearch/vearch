# \SpaceAPI

All URIs are relative to *http://localhost:80*

Method | HTTP request | Description
------------- | ------------- | -------------
[**CreateSpace**](SpaceAPI.md#CreateSpace) | **Put** /space/{DB_NAME}/_create | Create a new space
[**DeleteSpace**](SpaceAPI.md#DeleteSpace) | **Delete** /space/{DB_NAME}/{SPACE_NAME} | Delete a specific space



## CreateSpace

> CreateSpace(ctx, dBNAME).CreateSpaceRequest(createSpaceRequest).Execute()

Create a new space

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
	dBNAME := "dBNAME_example" // string | The name of the database where the space will be created.
	createSpaceRequest := *openapiclient.NewCreateSpaceRequest() // CreateSpaceRequest | 

	configuration := openapiclient.NewConfiguration()
	apiClient := openapiclient.NewAPIClient(configuration)
	r, err := apiClient.SpaceAPI.CreateSpace(context.Background(), dBNAME).CreateSpaceRequest(createSpaceRequest).Execute()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error when calling `SpaceAPI.CreateSpace``: %v\n", err)
		fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
	}
}
```

### Path Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
**ctx** | **context.Context** | context for authentication, logging, cancellation, deadlines, tracing, etc.
**dBNAME** | **string** | The name of the database where the space will be created. | 

### Other Parameters

Other parameters are passed through a pointer to a apiCreateSpaceRequest struct via the builder pattern


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------

 **createSpaceRequest** | [**CreateSpaceRequest**](CreateSpaceRequest.md) |  | 

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


## DeleteSpace

> DeleteSpace(ctx, dBNAME, sPACENAME).Execute()

Delete a specific space

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
	sPACENAME := "sPACENAME_example" // string | The name of the space to delete

	configuration := openapiclient.NewConfiguration()
	apiClient := openapiclient.NewAPIClient(configuration)
	r, err := apiClient.SpaceAPI.DeleteSpace(context.Background(), dBNAME, sPACENAME).Execute()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error when calling `SpaceAPI.DeleteSpace``: %v\n", err)
		fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
	}
}
```

### Path Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
**ctx** | **context.Context** | context for authentication, logging, cancellation, deadlines, tracing, etc.
**dBNAME** | **string** | The name of the database | 
**sPACENAME** | **string** | The name of the space to delete | 

### Other Parameters

Other parameters are passed through a pointer to a apiDeleteSpaceRequest struct via the builder pattern


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

