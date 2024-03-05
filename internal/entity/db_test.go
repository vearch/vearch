// Copyright 2019 The Vearch Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package entity

import "testing"

func TestDB_Validate(t *testing.T) {
	type fields struct {
		Id   DBID
		Name string
		Ps   []string
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		{
			name: "Valid DB with non-empty Ps",
			fields: fields{
				Id:   0, // Assuming that the ID type is int for the example
				Name: "validDBName",
				Ps:   []string{"partitionServer1", "partitionServer2"},
			},
			wantErr: false,
		},
		{
			name: "Invalid DB with empty name",
			fields: fields{
				Id:   0,
				Name: "",
				Ps:   []string{"partitionServer1"},
			},
			wantErr: true,
		},
		{
			name: "Invalid DB with empty Id > 0",
			fields: fields{
				Id:   1,
				Name: "validDBName",
				Ps:   []string{},
			},
			wantErr: true,
		},
		{
			name: "Invalid DB with invalid name characters",
			fields: fields{
				Id:   0,
				Name: "invalid!DB@Name",
				Ps:   []string{"partitionServer1"},
			},
			wantErr: true,
		},
		// TODO: Add more test cases as needed.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := &DB{
				Id:   tt.fields.Id,
				Name: tt.fields.Name,
				Ps:   tt.fields.Ps,
			}
			if err := db.Validate(); (err != nil) != tt.wantErr {
				t.Errorf("DB.Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
