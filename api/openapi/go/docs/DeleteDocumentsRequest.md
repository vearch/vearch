# DeleteDocumentsRequest

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**DbName** | Pointer to **string** |  | [optional] 
**SpaceName** | Pointer to **string** |  | [optional] 
**Query** | Pointer to [**DeleteDocumentsRequestQuery**](DeleteDocumentsRequestQuery.md) |  | [optional] 

## Methods

### NewDeleteDocumentsRequest

`func NewDeleteDocumentsRequest() *DeleteDocumentsRequest`

NewDeleteDocumentsRequest instantiates a new DeleteDocumentsRequest object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewDeleteDocumentsRequestWithDefaults

`func NewDeleteDocumentsRequestWithDefaults() *DeleteDocumentsRequest`

NewDeleteDocumentsRequestWithDefaults instantiates a new DeleteDocumentsRequest object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetDbName

`func (o *DeleteDocumentsRequest) GetDbName() string`

GetDbName returns the DbName field if non-nil, zero value otherwise.

### GetDbNameOk

`func (o *DeleteDocumentsRequest) GetDbNameOk() (*string, bool)`

GetDbNameOk returns a tuple with the DbName field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetDbName

`func (o *DeleteDocumentsRequest) SetDbName(v string)`

SetDbName sets DbName field to given value.

### HasDbName

`func (o *DeleteDocumentsRequest) HasDbName() bool`

HasDbName returns a boolean if a field has been set.

### GetSpaceName

`func (o *DeleteDocumentsRequest) GetSpaceName() string`

GetSpaceName returns the SpaceName field if non-nil, zero value otherwise.

### GetSpaceNameOk

`func (o *DeleteDocumentsRequest) GetSpaceNameOk() (*string, bool)`

GetSpaceNameOk returns a tuple with the SpaceName field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetSpaceName

`func (o *DeleteDocumentsRequest) SetSpaceName(v string)`

SetSpaceName sets SpaceName field to given value.

### HasSpaceName

`func (o *DeleteDocumentsRequest) HasSpaceName() bool`

HasSpaceName returns a boolean if a field has been set.

### GetQuery

`func (o *DeleteDocumentsRequest) GetQuery() DeleteDocumentsRequestQuery`

GetQuery returns the Query field if non-nil, zero value otherwise.

### GetQueryOk

`func (o *DeleteDocumentsRequest) GetQueryOk() (*DeleteDocumentsRequestQuery, bool)`

GetQueryOk returns a tuple with the Query field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetQuery

`func (o *DeleteDocumentsRequest) SetQuery(v DeleteDocumentsRequestQuery)`

SetQuery sets Query field to given value.

### HasQuery

`func (o *DeleteDocumentsRequest) HasQuery() bool`

HasQuery returns a boolean if a field has been set.


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


