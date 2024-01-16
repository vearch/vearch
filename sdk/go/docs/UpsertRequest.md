# UpsertRequest

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**DbName** | Pointer to **string** |  | [optional] [default to "db"]
**SpaceName** | Pointer to **string** |  | [optional] 
**Documents** | Pointer to **[]map[string]interface{}** |  | [optional] 

## Methods

### NewUpsertRequest

`func NewUpsertRequest() *UpsertRequest`

NewUpsertRequest instantiates a new UpsertRequest object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewUpsertRequestWithDefaults

`func NewUpsertRequestWithDefaults() *UpsertRequest`

NewUpsertRequestWithDefaults instantiates a new UpsertRequest object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetDbName

`func (o *UpsertRequest) GetDbName() string`

GetDbName returns the DbName field if non-nil, zero value otherwise.

### GetDbNameOk

`func (o *UpsertRequest) GetDbNameOk() (*string, bool)`

GetDbNameOk returns a tuple with the DbName field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetDbName

`func (o *UpsertRequest) SetDbName(v string)`

SetDbName sets DbName field to given value.

### HasDbName

`func (o *UpsertRequest) HasDbName() bool`

HasDbName returns a boolean if a field has been set.

### GetSpaceName

`func (o *UpsertRequest) GetSpaceName() string`

GetSpaceName returns the SpaceName field if non-nil, zero value otherwise.

### GetSpaceNameOk

`func (o *UpsertRequest) GetSpaceNameOk() (*string, bool)`

GetSpaceNameOk returns a tuple with the SpaceName field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetSpaceName

`func (o *UpsertRequest) SetSpaceName(v string)`

SetSpaceName sets SpaceName field to given value.

### HasSpaceName

`func (o *UpsertRequest) HasSpaceName() bool`

HasSpaceName returns a boolean if a field has been set.

### GetDocuments

`func (o *UpsertRequest) GetDocuments() []map[string]interface{}`

GetDocuments returns the Documents field if non-nil, zero value otherwise.

### GetDocumentsOk

`func (o *UpsertRequest) GetDocumentsOk() (*[]map[string]interface{}, bool)`

GetDocumentsOk returns a tuple with the Documents field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetDocuments

`func (o *UpsertRequest) SetDocuments(v []map[string]interface{})`

SetDocuments sets Documents field to given value.

### HasDocuments

`func (o *UpsertRequest) HasDocuments() bool`

HasDocuments returns a boolean if a field has been set.


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


