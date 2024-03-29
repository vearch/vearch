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

// checks if the UpsertRequest type satisfies the MappedNullable interface at compile time
var _ MappedNullable = &UpsertRequest{}

// UpsertRequest struct for UpsertRequest
type UpsertRequest struct {
	DbName *string `json:"db_name,omitempty"`
	SpaceName *string `json:"space_name,omitempty"`
	Documents []map[string]interface{} `json:"documents,omitempty"`
}

// NewUpsertRequest instantiates a new UpsertRequest object
// This constructor will assign default values to properties that have it defined,
// and makes sure properties required by API are set, but the set of arguments
// will change when the set of required properties is changed
func NewUpsertRequest() *UpsertRequest {
	this := UpsertRequest{}
	var dbName string = "db"
	this.DbName = &dbName
	return &this
}

// NewUpsertRequestWithDefaults instantiates a new UpsertRequest object
// This constructor will only assign default values to properties that have it defined,
// but it doesn't guarantee that properties required by API are set
func NewUpsertRequestWithDefaults() *UpsertRequest {
	this := UpsertRequest{}
	var dbName string = "db"
	this.DbName = &dbName
	return &this
}

// GetDbName returns the DbName field value if set, zero value otherwise.
func (o *UpsertRequest) GetDbName() string {
	if o == nil || IsNil(o.DbName) {
		var ret string
		return ret
	}
	return *o.DbName
}

// GetDbNameOk returns a tuple with the DbName field value if set, nil otherwise
// and a boolean to check if the value has been set.
func (o *UpsertRequest) GetDbNameOk() (*string, bool) {
	if o == nil || IsNil(o.DbName) {
		return nil, false
	}
	return o.DbName, true
}

// HasDbName returns a boolean if a field has been set.
func (o *UpsertRequest) HasDbName() bool {
	if o != nil && !IsNil(o.DbName) {
		return true
	}

	return false
}

// SetDbName gets a reference to the given string and assigns it to the DbName field.
func (o *UpsertRequest) SetDbName(v string) {
	o.DbName = &v
}

// GetSpaceName returns the SpaceName field value if set, zero value otherwise.
func (o *UpsertRequest) GetSpaceName() string {
	if o == nil || IsNil(o.SpaceName) {
		var ret string
		return ret
	}
	return *o.SpaceName
}

// GetSpaceNameOk returns a tuple with the SpaceName field value if set, nil otherwise
// and a boolean to check if the value has been set.
func (o *UpsertRequest) GetSpaceNameOk() (*string, bool) {
	if o == nil || IsNil(o.SpaceName) {
		return nil, false
	}
	return o.SpaceName, true
}

// HasSpaceName returns a boolean if a field has been set.
func (o *UpsertRequest) HasSpaceName() bool {
	if o != nil && !IsNil(o.SpaceName) {
		return true
	}

	return false
}

// SetSpaceName gets a reference to the given string and assigns it to the SpaceName field.
func (o *UpsertRequest) SetSpaceName(v string) {
	o.SpaceName = &v
}

// GetDocuments returns the Documents field value if set, zero value otherwise.
func (o *UpsertRequest) GetDocuments() []map[string]interface{} {
	if o == nil || IsNil(o.Documents) {
		var ret []map[string]interface{}
		return ret
	}
	return o.Documents
}

// GetDocumentsOk returns a tuple with the Documents field value if set, nil otherwise
// and a boolean to check if the value has been set.
func (o *UpsertRequest) GetDocumentsOk() ([]map[string]interface{}, bool) {
	if o == nil || IsNil(o.Documents) {
		return nil, false
	}
	return o.Documents, true
}

// HasDocuments returns a boolean if a field has been set.
func (o *UpsertRequest) HasDocuments() bool {
	if o != nil && !IsNil(o.Documents) {
		return true
	}

	return false
}

// SetDocuments gets a reference to the given []map[string]interface{} and assigns it to the Documents field.
func (o *UpsertRequest) SetDocuments(v []map[string]interface{}) {
	o.Documents = v
}

func (o UpsertRequest) MarshalJSON() ([]byte, error) {
	toSerialize,err := o.ToMap()
	if err != nil {
		return []byte{}, err
	}
	return json.Marshal(toSerialize)
}

func (o UpsertRequest) ToMap() (map[string]interface{}, error) {
	toSerialize := map[string]interface{}{}
	if !IsNil(o.DbName) {
		toSerialize["db_name"] = o.DbName
	}
	if !IsNil(o.SpaceName) {
		toSerialize["space_name"] = o.SpaceName
	}
	if !IsNil(o.Documents) {
		toSerialize["documents"] = o.Documents
	}
	return toSerialize, nil
}

type NullableUpsertRequest struct {
	value *UpsertRequest
	isSet bool
}

func (v NullableUpsertRequest) Get() *UpsertRequest {
	return v.value
}

func (v *NullableUpsertRequest) Set(val *UpsertRequest) {
	v.value = val
	v.isSet = true
}

func (v NullableUpsertRequest) IsSet() bool {
	return v.isSet
}

func (v *NullableUpsertRequest) Unset() {
	v.value = nil
	v.isSet = false
}

func NewNullableUpsertRequest(val *UpsertRequest) *NullableUpsertRequest {
	return &NullableUpsertRequest{value: val, isSet: true}
}

func (v NullableUpsertRequest) MarshalJSON() ([]byte, error) {
	return json.Marshal(v.value)
}

func (v *NullableUpsertRequest) UnmarshalJSON(src []byte) error {
	v.isSet = true
	return json.Unmarshal(src, &v.value)
}


