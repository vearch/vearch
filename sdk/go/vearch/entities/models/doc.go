package models

type Docs struct {
	DBName    string        `json:"db_name"`
	SpaceName string        `json:"space_name"`
	Documents []interface{} `json:"documents,omitempty"`
}
