# DeleteDocuments200Response

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**Status** | Pointer to **string** |  | [optional] 
**ErrorMessage** | Pointer to **NullableString** |  | [optional] 

## Methods

### NewDeleteDocuments200Response

`func NewDeleteDocuments200Response() *DeleteDocuments200Response`

NewDeleteDocuments200Response instantiates a new DeleteDocuments200Response object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewDeleteDocuments200ResponseWithDefaults

`func NewDeleteDocuments200ResponseWithDefaults() *DeleteDocuments200Response`

NewDeleteDocuments200ResponseWithDefaults instantiates a new DeleteDocuments200Response object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetStatus

`func (o *DeleteDocuments200Response) GetStatus() string`

GetStatus returns the Status field if non-nil, zero value otherwise.

### GetStatusOk

`func (o *DeleteDocuments200Response) GetStatusOk() (*string, bool)`

GetStatusOk returns a tuple with the Status field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetStatus

`func (o *DeleteDocuments200Response) SetStatus(v string)`

SetStatus sets Status field to given value.

### HasStatus

`func (o *DeleteDocuments200Response) HasStatus() bool`

HasStatus returns a boolean if a field has been set.

### GetErrorMessage

`func (o *DeleteDocuments200Response) GetErrorMessage() string`

GetErrorMessage returns the ErrorMessage field if non-nil, zero value otherwise.

### GetErrorMessageOk

`func (o *DeleteDocuments200Response) GetErrorMessageOk() (*string, bool)`

GetErrorMessageOk returns a tuple with the ErrorMessage field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetErrorMessage

`func (o *DeleteDocuments200Response) SetErrorMessage(v string)`

SetErrorMessage sets ErrorMessage field to given value.

### HasErrorMessage

`func (o *DeleteDocuments200Response) HasErrorMessage() bool`

HasErrorMessage returns a boolean if a field has been set.

### SetErrorMessageNil

`func (o *DeleteDocuments200Response) SetErrorMessageNil(b bool)`

 SetErrorMessageNil sets the value for ErrorMessage to be an explicit nil

### UnsetErrorMessage
`func (o *DeleteDocuments200Response) UnsetErrorMessage()`

UnsetErrorMessage ensures that no value is present for ErrorMessage, not even an explicit nil

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


