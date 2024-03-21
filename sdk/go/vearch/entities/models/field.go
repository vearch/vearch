package models

type IndexParams struct {
	MetricType        string `json:"metric_type"`
	Nprobe            int    `json:"nprobe,omitempty"`
	Ncentroids        int    `json:"ncentroids,omitempty"`
	Nsubvector        int    `json:"nsubvector,omitempty"`
	EfConstruction    int    `json:"efConstruction,omitempty"`
	EfSearch          int    `json:"efSearch,omitempty"`
	TrainingThreshold int    `json:"training_threshold,omitempty"`
}

type Index struct {
	IndexName   string       `json:"name"`
	IndexType   string       `json:"type"`
	IndexParams *IndexParams `json:"params,omitempty"`
}

type Field struct {
	Name       string `json:"name"`
	Type       string `json:"type"`
	Dimension  int    `json:"dimension,omitempty"`
	StoreType  string `json:"store_type,omitempty"`
	Format     string `json:"format,omitempty"`
	Index      *Index `json:"index,omitempty"`
	StoreParam *struct {
		CacheSize string `json:"cache_size,omitempty"`
	} `json:"store_param,omitempty"`
}
