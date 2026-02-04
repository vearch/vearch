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

import "time"

// BackupSpaceRequest backup space request
type BackupSpaceRequest struct {
	Command           string      `json:"command,omitempty"`
	BackupID          int         `json:"backup_id,omitempty"`
	VersionID         string      `json:"version_id,omitempty"`
	BackupType        string      `json:"backup_type,omitempty"` // Backup type: "full" for full backup, "incremental" for incremental backup (default: full)
	Part              PartitionID `json:"part"`
	SourceClusterName string      `json:"source_cluster_name,omitempty"` // Source cluster name (for cross-cluster restore)
	S3Param           struct {
		Region     string `json:"region"`
		BucketName string `json:"bucket_name"`
		EndPoint   string `json:"endpoint"`
		AccessKey  string `json:"access_key"`
		SecretKey  string `json:"secret_key"`
		UseSSL     bool   `json:"use_ssl"`
	} `json:"s3_param,omitempty"`
}

// BackupOrRestoreRequest backup or restore request
type BackupOrRestoreRequest struct {
	Database          string `json:"database"`
	Space             string `json:"space"`
	BackupID          string `json:"backup_id"`
	VersionID         string `json:"version_id"`
	Command           string `json:"command,omitempty"`
	BackupType        string `json:"backup_type,omitempty"`         // Backup type: "full" for full backup, "incremental" for incremental backup (default: incremental)
	S3PartitionID     uint32 `json:"s3_partition_id,omitempty"`     // Partition ID on S3 (used during restore when new partition ID differs from S3 partition ID)
	SourceClusterName string `json:"source_cluster_name,omitempty"` // Source cluster name (for cross-cluster restore)
	S3Param           struct {
		Region     string `json:"region"`
		BucketName string `json:"bucket_name"`
		EndPoint   string `json:"endpoint"`
		AccessKey  string `json:"access_key"`
		SecretKey  string `json:"secret_key"`
		UseSSL     bool   `json:"use_ssl"`
	} `json:"s3_param,omitempty"`
}

// BackupSpaceResponse backup space response
type BackupSpaceResponse struct {
	BackupID  int    `json:"backup_id,omitempty"`
	BackupIDs []int  `json:"backup_ids,omitempty"`
	VersionID string `json:"version_id,omitempty"`
}

// BackupProgressResponse backup progress response
type BackupProgressResponse struct {
	TotalTasks     int     `json:"total_tasks,omitempty"`     // Total number of tasks
	CompletedTasks int     `json:"completed_tasks,omitempty"` // Number of completed tasks
	SuccessRatio   float64 `json:"success_ratio,omitempty"`   // Success ratio of partitions (0.0-1.0)
	Status         string  `json:"status,omitempty"`          // Backup status: completed, failed, running
	VersionID      string  `json:"version_id,omitempty"`      // Version ID
}

// BackupStatusQuery backup status query
type BackupStatusQuery struct {
	SpaceKey string `json:"space_key"`
	BackupID string `json:"backup_id"`
}

// BackupStatusResponse backup status response
type BackupStatusResponse struct {
	Exists       bool   `json:"exists"`
	Status       int    `json:"status"` // 0=running, 1=completed, 2=failed
	ErrorMessage string `json:"error_message"`
}

// DeleteBackupVersionRequest delete backup version request
type DeleteBackupVersionRequest struct {
	SpaceKey  string `json:"space_key"`
	VersionID string `json:"version_id"`
}

// BackupVersion backup version
type BackupVersion struct {
	SpaceKey    string                 `json:"space_key"`   // dbName-spaceName
	VersionID   string                 `json:"version_id"`  // version ID
	BackupID    string                 `json:"backup_id"`   // backup ID
	CreateTime  time.Time              `json:"create_time"` // creation time
	Size        int64                  `json:"size"`        // version size
	Status      BackupVersionStatus    `json:"status"`      // version status, complete status flow
	Description string                 `json:"description"` // version description
	Checksum    string                 `json:"checksum"`    // checksum
	Partitions  []*PartitionBackupInfo `json:"partitions"`  // partition backup information
}

// BackupVersionStatus version status
type BackupVersionStatus int

// BackupTaskStatus backup task status
type BackupTaskStatus int

// PartitionBackupInfo partition backup information
type PartitionBackupInfo struct {
	PartitionID   PartitionID `json:"partition_id"`
	NodeID        NodeID      `json:"node_id"`
	S3PartitionID PartitionID `json:"s3_partition_id"`
	Size          int64       `json:"size"`
	Checksum      string      `json:"checksum"`
	S3Path        string      `json:"s3_path"`
	Status        string      `json:"status"` // BackupStatusRunning  BackupStatusCompleted  BackupStatusFailed
}

// PartitionBackupOrRestoreTask partition backup/restore task
type PartitionBackupOrRestoreTask struct {
	PartitionID   PartitionID             `json:"partition_id"`
	NodeID        NodeID                  `json:"node_id"`
	VersionID     string                  `json:"version_id"`
	PSNodeAddr    string                  `json:"ps_node_addr"`
	TaskType      string                  `json:"task_type"` // task type: backup, restore
	Status        BackupTaskStatus        `json:"status"`
	RetryCount    int                     `json:"retry_count"`
	MaxRetries    int                     `json:"max_retries"`
	LastError     error                   `json:"-"`
	LastErrorMsg  string                  `json:"last_error,omitempty"`
	StartTime     time.Time               `json:"start_time"`
	CompleteTime  time.Time               `json:"complete_time"`
	BackupRequest *BackupOrRestoreRequest `json:"backup_request,omitempty"`
}

// VersionInfo stores multiple BackupVersions for each space_key
type VersionInfo struct {
	SpaceKey    string           `json:"space_key"`
	Versions    []*BackupVersion `json:"versions"`
	LastUpdated time.Time        `json:"last_updated"`
	TotalCount  int64            `json:"total_size"`
}

// VersionStatusInfo stores the status of each versionId (for BackupManager's versionCache)
type VersionStatusInfo struct {
	VersionID   string              `json:"version_id"`
	Status      BackupVersionStatus `json:"status"`
	LastUpdated time.Time           `json:"last_updated"`
}

// CreateVersionRequest create version request
type CreateVersionRequest struct {
	ClusterName string                 `json:"cluster_name"`
	DBName      string                 `json:"db_name"`
	SpaceName   string                 `json:"space_name"`
	BackupID    string                 `json:"backup_id"`
	VersionID   string                 `json:"version_id"`
	Description string                 `json:"description"`
	Tags        map[string]string      `json:"tags"`
	Partitions  []*PartitionBackupInfo `json:"partitions"`
}
