package models

type Space struct {
	Name         string   `json:"name"`
	PartitionNum int      `json:"partition_num"`
	ReplicaNum   int      `json:"replica_num"`
	Fields       []*Field `json:"fields"`
}
