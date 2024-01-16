# SearchDocumentsRequestQuery

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**Vector** | [**[]SearchDocumentsRequestQueryVectorInner**](SearchDocumentsRequestQueryVectorInner.md) |  | 
**Filter** | Pointer to **[]map[string]interface{}** |  | [optional] 

## Methods

### NewSearchDocumentsRequestQuery

`func NewSearchDocumentsRequestQuery(vector []SearchDocumentsRequestQueryVectorInner, ) *SearchDocumentsRequestQuery`

NewSearchDocumentsRequestQuery instantiates a new SearchDocumentsRequestQuery object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewSearchDocumentsRequestQueryWithDefaults

`func NewSearchDocumentsRequestQueryWithDefaults() *SearchDocumentsRequestQuery`

NewSearchDocumentsRequestQueryWithDefaults instantiates a new SearchDocumentsRequestQuery object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetVector

`func (o *SearchDocumentsRequestQuery) GetVector() []SearchDocumentsRequestQueryVectorInner`

GetVector returns the Vector field if non-nil, zero value otherwise.

### GetVectorOk

`func (o *SearchDocumentsRequestQuery) GetVectorOk() (*[]SearchDocumentsRequestQueryVectorInner, bool)`

GetVectorOk returns a tuple with the Vector field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetVector

`func (o *SearchDocumentsRequestQuery) SetVector(v []SearchDocumentsRequestQueryVectorInner)`

SetVector sets Vector field to given value.


### GetFilter

`func (o *SearchDocumentsRequestQuery) GetFilter() []map[string]interface{}`

GetFilter returns the Filter field if non-nil, zero value otherwise.

### GetFilterOk

`func (o *SearchDocumentsRequestQuery) GetFilterOk() (*[]map[string]interface{}, bool)`

GetFilterOk returns a tuple with the Filter field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetFilter

`func (o *SearchDocumentsRequestQuery) SetFilter(v []map[string]interface{})`

SetFilter sets Filter field to given value.

### HasFilter

`func (o *SearchDocumentsRequestQuery) HasFilter() bool`

HasFilter returns a boolean if a field has been set.


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


