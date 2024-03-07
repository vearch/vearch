# CreateSpaceRequest

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**Name** | Pointer to **string** |  | [optional] 
**PartitionNum** | Pointer to **int32** |  | [optional] 
**ReplicaNum** | Pointer to **int32** |  | [optional] 
**Engine** | Pointer to [**CreateSpaceRequestEngine**](CreateSpaceRequestEngine.md) |  | [optional] 
**Properties** | Pointer to [**map[string]CreateSpaceRequestPropertiesValue**](CreateSpaceRequestPropertiesValue.md) |  | [optional] 

## Methods

### NewCreateSpaceRequest

`func NewCreateSpaceRequest() *CreateSpaceRequest`

NewCreateSpaceRequest instantiates a new CreateSpaceRequest object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewCreateSpaceRequestWithDefaults

`func NewCreateSpaceRequestWithDefaults() *CreateSpaceRequest`

NewCreateSpaceRequestWithDefaults instantiates a new CreateSpaceRequest object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetName

`func (o *CreateSpaceRequest) GetName() string`

GetName returns the Name field if non-nil, zero value otherwise.

### GetNameOk

`func (o *CreateSpaceRequest) GetNameOk() (*string, bool)`

GetNameOk returns a tuple with the Name field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetName

`func (o *CreateSpaceRequest) SetName(v string)`

SetName sets Name field to given value.

### HasName

`func (o *CreateSpaceRequest) HasName() bool`

HasName returns a boolean if a field has been set.

### GetPartitionNum

`func (o *CreateSpaceRequest) GetPartitionNum() int32`

GetPartitionNum returns the PartitionNum field if non-nil, zero value otherwise.

### GetPartitionNumOk

`func (o *CreateSpaceRequest) GetPartitionNumOk() (*int32, bool)`

GetPartitionNumOk returns a tuple with the PartitionNum field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetPartitionNum

`func (o *CreateSpaceRequest) SetPartitionNum(v int32)`

SetPartitionNum sets PartitionNum field to given value.

### HasPartitionNum

`func (o *CreateSpaceRequest) HasPartitionNum() bool`

HasPartitionNum returns a boolean if a field has been set.

### GetReplicaNum

`func (o *CreateSpaceRequest) GetReplicaNum() int32`

GetReplicaNum returns the ReplicaNum field if non-nil, zero value otherwise.

### GetReplicaNumOk

`func (o *CreateSpaceRequest) GetReplicaNumOk() (*int32, bool)`

GetReplicaNumOk returns a tuple with the ReplicaNum field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetReplicaNum

`func (o *CreateSpaceRequest) SetReplicaNum(v int32)`

SetReplicaNum sets ReplicaNum field to given value.

### HasReplicaNum

`func (o *CreateSpaceRequest) HasReplicaNum() bool`

HasReplicaNum returns a boolean if a field has been set.

### GetEngine

`func (o *CreateSpaceRequest) GetEngine() CreateSpaceRequestEngine`

GetEngine returns the Engine field if non-nil, zero value otherwise.

### GetEngineOk

`func (o *CreateSpaceRequest) GetEngineOk() (*CreateSpaceRequestEngine, bool)`

GetEngineOk returns a tuple with the Engine field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetEngine

`func (o *CreateSpaceRequest) SetEngine(v CreateSpaceRequestEngine)`

SetEngine sets Engine field to given value.

### HasEngine

`func (o *CreateSpaceRequest) HasEngine() bool`

HasEngine returns a boolean if a field has been set.

### GetProperties

`func (o *CreateSpaceRequest) GetProperties() map[string]CreateSpaceRequestPropertiesValue`

GetProperties returns the Properties field if non-nil, zero value otherwise.

### GetPropertiesOk

`func (o *CreateSpaceRequest) GetPropertiesOk() (*map[string]CreateSpaceRequestPropertiesValue, bool)`

GetPropertiesOk returns a tuple with the Properties field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetProperties

`func (o *CreateSpaceRequest) SetProperties(v map[string]CreateSpaceRequestPropertiesValue)`

SetProperties sets Properties field to given value.

### HasProperties

`func (o *CreateSpaceRequest) HasProperties() bool`

HasProperties returns a boolean if a field has been set.


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


