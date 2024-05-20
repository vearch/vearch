package models

type DeleteRequest struct {
	DBName    string   `json:"db_name"`
	SpaceName string   `json:"space_name"`
	Filters   *Filters `json:"filters,omitempty"`
	IDs       []string `json:"document_ids"`
}
