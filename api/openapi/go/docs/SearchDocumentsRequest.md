# SearchDocumentsRequest

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**Query** | [**SearchDocumentsRequestQuery**](SearchDocumentsRequestQuery.md) |  | 
**RetrievalParam** | [**SearchDocumentsRequestRetrievalParam**](SearchDocumentsRequestRetrievalParam.md) |  | 
**Size** | **int32** |  | 
**DbName** | **string** |  | 
**SpaceName** | **string** |  | 

## Methods

### NewSearchDocumentsRequest

`func NewSearchDocumentsRequest(query SearchDocumentsRequestQuery, retrievalParam SearchDocumentsRequestRetrievalParam, size int32, dbName string, spaceName string, ) *SearchDocumentsRequest`

NewSearchDocumentsRequest instantiates a new SearchDocumentsRequest object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewSearchDocumentsRequestWithDefaults

`func NewSearchDocumentsRequestWithDefaults() *SearchDocumentsRequest`

NewSearchDocumentsRequestWithDefaults instantiates a new SearchDocumentsRequest object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetQuery

`func (o *SearchDocumentsRequest) GetQuery() SearchDocumentsRequestQuery`

GetQuery returns the Query field if non-nil, zero value otherwise.

### GetQueryOk

`func (o *SearchDocumentsRequest) GetQueryOk() (*SearchDocumentsRequestQuery, bool)`

GetQueryOk returns a tuple with the Query field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetQuery

`func (o *SearchDocumentsRequest) SetQuery(v SearchDocumentsRequestQuery)`

SetQuery sets Query field to given value.


### GetRetrievalParam

`func (o *SearchDocumentsRequest) GetRetrievalParam() SearchDocumentsRequestRetrievalParam`

GetRetrievalParam returns the RetrievalParam field if non-nil, zero value otherwise.

### GetRetrievalParamOk

`func (o *SearchDocumentsRequest) GetRetrievalParamOk() (*SearchDocumentsRequestRetrievalParam, bool)`

GetRetrievalParamOk returns a tuple with the RetrievalParam field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetRetrievalParam

`func (o *SearchDocumentsRequest) SetRetrievalParam(v SearchDocumentsRequestRetrievalParam)`

SetRetrievalParam sets RetrievalParam field to given value.


### GetSize

`func (o *SearchDocumentsRequest) GetSize() int32`

GetSize returns the Size field if non-nil, zero value otherwise.

### GetSizeOk

`func (o *SearchDocumentsRequest) GetSizeOk() (*int32, bool)`

GetSizeOk returns a tuple with the Size field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetSize

`func (o *SearchDocumentsRequest) SetSize(v int32)`

SetSize sets Size field to given value.


### GetDbName

`func (o *SearchDocumentsRequest) GetDbName() string`

GetDbName returns the DbName field if non-nil, zero value otherwise.

### GetDbNameOk

`func (o *SearchDocumentsRequest) GetDbNameOk() (*string, bool)`

GetDbNameOk returns a tuple with the DbName field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetDbName

`func (o *SearchDocumentsRequest) SetDbName(v string)`

SetDbName sets DbName field to given value.


### GetSpaceName

`func (o *SearchDocumentsRequest) GetSpaceName() string`

GetSpaceName returns the SpaceName field if non-nil, zero value otherwise.

### GetSpaceNameOk

`func (o *SearchDocumentsRequest) GetSpaceNameOk() (*string, bool)`

GetSpaceNameOk returns a tuple with the SpaceName field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetSpaceName

`func (o *SearchDocumentsRequest) SetSpaceName(v string)`

SetSpaceName sets SpaceName field to given value.



[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


