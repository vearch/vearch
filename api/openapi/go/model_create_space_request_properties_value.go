/*
Vearch Database API

API for sending dynamic records to the Vearch database.

API version: 1.0.0
*/

// Code generated by OpenAPI Generator (https://openapi-generator.tech); DO NOT EDIT.

package vearch_client

import (
	"encoding/json"
)

// checks if the CreateSpaceRequestPropertiesValue type satisfies the MappedNullable interface at compile time
var _ MappedNullable = &CreateSpaceRequestPropertiesValue{}

// CreateSpaceRequestPropertiesValue struct for CreateSpaceRequestPropertiesValue
type CreateSpaceRequestPropertiesValue struct {
	Type *string `json:"type,omitempty"`
	Index *bool `json:"index,omitempty"`
	Dimension *int32 `json:"dimension,omitempty"`
	StoreType *string `json:"store_type,omitempty"`
	Format *string `json:"format,omitempty"`
}

// NewCreateSpaceRequestPropertiesValue instantiates a new CreateSpaceRequestPropertiesValue object
// This constructor will assign default values to properties that have it defined,
// and makes sure properties required by API are set, but the set of arguments
// will change when the set of required properties is changed
func NewCreateSpaceRequestPropertiesValue() *CreateSpaceRequestPropertiesValue {
	this := CreateSpaceRequestPropertiesValue{}
	return &this
}

// NewCreateSpaceRequestPropertiesValueWithDefaults instantiates a new CreateSpaceRequestPropertiesValue object
// This constructor will only assign default values to properties that have it defined,
// but it doesn't guarantee that properties required by API are set
func NewCreateSpaceRequestPropertiesValueWithDefaults() *CreateSpaceRequestPropertiesValue {
	this := CreateSpaceRequestPropertiesValue{}
	return &this
}

// GetType returns the Type field value if set, zero value otherwise.
func (o *CreateSpaceRequestPropertiesValue) GetType() string {
	if o == nil || IsNil(o.Type) {
		var ret string
		return ret
	}
	return *o.Type
}

// GetTypeOk returns a tuple with the Type field value if set, nil otherwise
// and a boolean to check if the value has been set.
func (o *CreateSpaceRequestPropertiesValue) GetTypeOk() (*string, bool) {
	if o == nil || IsNil(o.Type) {
		return nil, false
	}
	return o.Type, true
}

// HasType returns a boolean if a field has been set.
func (o *CreateSpaceRequestPropertiesValue) HasType() bool {
	if o != nil && !IsNil(o.Type) {
		return true
	}

	return false
}

// SetType gets a reference to the given string and assigns it to the Type field.
func (o *CreateSpaceRequestPropertiesValue) SetType(v string) {
	o.Type = &v
}

// GetIndex returns the Index field value if set, zero value otherwise.
func (o *CreateSpaceRequestPropertiesValue) GetIndex() bool {
	if o == nil || IsNil(o.Index) {
		var ret bool
		return ret
	}
	return *o.Index
}

// GetIndexOk returns a tuple with the Index field value if set, nil otherwise
// and a boolean to check if the value has been set.
func (o *CreateSpaceRequestPropertiesValue) GetIndexOk() (*bool, bool) {
	if o == nil || IsNil(o.Index) {
		return nil, false
	}
	return o.Index, true
}

// HasIndex returns a boolean if a field has been set.
func (o *CreateSpaceRequestPropertiesValue) HasIndex() bool {
	if o != nil && !IsNil(o.Index) {
		return true
	}

	return false
}

// SetIndex gets a reference to the given bool and assigns it to the Index field.
func (o *CreateSpaceRequestPropertiesValue) SetIndex(v bool) {
	o.Index = &v
}

// GetDimension returns the Dimension field value if set, zero value otherwise.
func (o *CreateSpaceRequestPropertiesValue) GetDimension() int32 {
	if o == nil || IsNil(o.Dimension) {
		var ret int32
		return ret
	}
	return *o.Dimension
}

// GetDimensionOk returns a tuple with the Dimension field value if set, nil otherwise
// and a boolean to check if the value has been set.
func (o *CreateSpaceRequestPropertiesValue) GetDimensionOk() (*int32, bool) {
	if o == nil || IsNil(o.Dimension) {
		return nil, false
	}
	return o.Dimension, true
}

// HasDimension returns a boolean if a field has been set.
func (o *CreateSpaceRequestPropertiesValue) HasDimension() bool {
	if o != nil && !IsNil(o.Dimension) {
		return true
	}

	return false
}

// SetDimension gets a reference to the given int32 and assigns it to the Dimension field.
func (o *CreateSpaceRequestPropertiesValue) SetDimension(v int32) {
	o.Dimension = &v
}

// GetStoreType returns the StoreType field value if set, zero value otherwise.
func (o *CreateSpaceRequestPropertiesValue) GetStoreType() string {
	if o == nil || IsNil(o.StoreType) {
		var ret string
		return ret
	}
	return *o.StoreType
}

// GetStoreTypeOk returns a tuple with the StoreType field value if set, nil otherwise
// and a boolean to check if the value has been set.
func (o *CreateSpaceRequestPropertiesValue) GetStoreTypeOk() (*string, bool) {
	if o == nil || IsNil(o.StoreType) {
		return nil, false
	}
	return o.StoreType, true
}

// HasStoreType returns a boolean if a field has been set.
func (o *CreateSpaceRequestPropertiesValue) HasStoreType() bool {
	if o != nil && !IsNil(o.StoreType) {
		return true
	}

	return false
}

// SetStoreType gets a reference to the given string and assigns it to the StoreType field.
func (o *CreateSpaceRequestPropertiesValue) SetStoreType(v string) {
	o.StoreType = &v
}

// GetFormat returns the Format field value if set, zero value otherwise.
func (o *CreateSpaceRequestPropertiesValue) GetFormat() string {
	if o == nil || IsNil(o.Format) {
		var ret string
		return ret
	}
	return *o.Format
}

// GetFormatOk returns a tuple with the Format field value if set, nil otherwise
// and a boolean to check if the value has been set.
func (o *CreateSpaceRequestPropertiesValue) GetFormatOk() (*string, bool) {
	if o == nil || IsNil(o.Format) {
		return nil, false
	}
	return o.Format, true
}

// HasFormat returns a boolean if a field has been set.
func (o *CreateSpaceRequestPropertiesValue) HasFormat() bool {
	if o != nil && !IsNil(o.Format) {
		return true
	}

	return false
}

// SetFormat gets a reference to the given string and assigns it to the Format field.
func (o *CreateSpaceRequestPropertiesValue) SetFormat(v string) {
	o.Format = &v
}

func (o CreateSpaceRequestPropertiesValue) MarshalJSON() ([]byte, error) {
	toSerialize,err := o.ToMap()
	if err != nil {
		return []byte{}, err
	}
	return json.Marshal(toSerialize)
}

func (o CreateSpaceRequestPropertiesValue) ToMap() (map[string]interface{}, error) {
	toSerialize := map[string]interface{}{}
	if !IsNil(o.Type) {
		toSerialize["type"] = o.Type
	}
	if !IsNil(o.Index) {
		toSerialize["index"] = o.Index
	}
	if !IsNil(o.Dimension) {
		toSerialize["dimension"] = o.Dimension
	}
	if !IsNil(o.StoreType) {
		toSerialize["store_type"] = o.StoreType
	}
	if !IsNil(o.Format) {
		toSerialize["format"] = o.Format
	}
	return toSerialize, nil
}

type NullableCreateSpaceRequestPropertiesValue struct {
	value *CreateSpaceRequestPropertiesValue
	isSet bool
}

func (v NullableCreateSpaceRequestPropertiesValue) Get() *CreateSpaceRequestPropertiesValue {
	return v.value
}

func (v *NullableCreateSpaceRequestPropertiesValue) Set(val *CreateSpaceRequestPropertiesValue) {
	v.value = val
	v.isSet = true
}

func (v NullableCreateSpaceRequestPropertiesValue) IsSet() bool {
	return v.isSet
}

func (v *NullableCreateSpaceRequestPropertiesValue) Unset() {
	v.value = nil
	v.isSet = false
}

func NewNullableCreateSpaceRequestPropertiesValue(val *CreateSpaceRequestPropertiesValue) *NullableCreateSpaceRequestPropertiesValue {
	return &NullableCreateSpaceRequestPropertiesValue{value: val, isSet: true}
}

func (v NullableCreateSpaceRequestPropertiesValue) MarshalJSON() ([]byte, error) {
	return json.Marshal(v.value)
}

func (v *NullableCreateSpaceRequestPropertiesValue) UnmarshalJSON(src []byte) error {
	v.isSet = true
	return json.Unmarshal(src, &v.value)
}


