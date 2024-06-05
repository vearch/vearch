package models

type Vector struct {
	Field   string    `json:"field"`
	Feature []float32 `json:"feature"`
}

type Condition struct {
	Operator string      `json:"operator"`
	Field    string      `json:"field"`
	Value    interface{} `json:"value"`
}

type Filters struct {
	Operator   string      `json:"operator"`
	Conditions []Condition `json:"conditions"`
}

type SearchRequest struct {
	DBName    string   `json:"db_name"`
	SpaceName string   `json:"space_name"`
	Limit     int      `json:"limit"`
	Vectors   []Vector `json:"vectors"`
	Filters   *Filters `json:"filters,omitempty"`
}
