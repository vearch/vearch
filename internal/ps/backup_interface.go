package ps

import (
	"context"
)

type BackupManager interface {
	StartPartitionBackup(spaceKey string, backupID string, versionID string, partitionID uint32, backupType string) error
	StartPartitionRestore(spaceKey string, backupID string, versionID string, partitionID uint32, s3PartitionID uint32, sourceClusterName string) error
	GetBackupTaskStatus(spaceKey string, backupID string, partitionID uint32) (status int, errorMsg string, exists bool)
	DeleteBackupVersion(ctx context.Context, spaceKey string, versionID string) error
}

func (s *Server) SetBackupManager(manager BackupManager) {
	s.backupManager = manager
}

func (s *Server) GetBackupManager() BackupManager {
	return s.backupManager
}
