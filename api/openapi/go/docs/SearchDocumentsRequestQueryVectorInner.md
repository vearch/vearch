# SearchDocumentsRequestQueryVectorInner

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**Field** | **string** |  | 
**Feature** | **[]float32** |  | 

## Methods

### NewSearchDocumentsRequestQueryVectorInner

`func NewSearchDocumentsRequestQueryVectorInner(field string, feature []float32, ) *SearchDocumentsRequestQueryVectorInner`

NewSearchDocumentsRequestQueryVectorInner instantiates a new SearchDocumentsRequestQueryVectorInner object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewSearchDocumentsRequestQueryVectorInnerWithDefaults

`func NewSearchDocumentsRequestQueryVectorInnerWithDefaults() *SearchDocumentsRequestQueryVectorInner`

NewSearchDocumentsRequestQueryVectorInnerWithDefaults instantiates a new SearchDocumentsRequestQueryVectorInner object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetField

`func (o *SearchDocumentsRequestQueryVectorInner) GetField() string`

GetField returns the Field field if non-nil, zero value otherwise.

### GetFieldOk

`func (o *SearchDocumentsRequestQueryVectorInner) GetFieldOk() (*string, bool)`

GetFieldOk returns a tuple with the Field field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetField

`func (o *SearchDocumentsRequestQueryVectorInner) SetField(v string)`

SetField sets Field field to given value.


### GetFeature

`func (o *SearchDocumentsRequestQueryVectorInner) GetFeature() []float32`

GetFeature returns the Feature field if non-nil, zero value otherwise.

### GetFeatureOk

`func (o *SearchDocumentsRequestQueryVectorInner) GetFeatureOk() (*[]float32, bool)`

GetFeatureOk returns a tuple with the Feature field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetFeature

`func (o *SearchDocumentsRequestQueryVectorInner) SetFeature(v []float32)`

SetFeature sets Feature field to given value.



[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


