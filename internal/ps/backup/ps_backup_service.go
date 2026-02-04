package backup

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/vearch/vearch/v3/internal/config"
	"github.com/vearch/vearch/v3/internal/entity"
	"github.com/vearch/vearch/v3/internal/master/services"
	"github.com/vearch/vearch/v3/internal/pkg/log"
	"github.com/vearch/vearch/v3/internal/ps/engine"
)

// GetPartitionFunc defines a function type to get PartitionStore, used to avoid circular dependencies
type GetPartitionFunc func(id entity.PartitionID) PartitionStore

// PartitionStore defines partition storage interface, used to avoid circular dependencies
// Only contains methods needed by the backup package
type PartitionStore interface {
	GetEngine() engine.Engine
}

// PartitionBackupMeta partition backup metadata
type PartitionBackupMeta struct {
	PartitionID uint32     `json:"partition_id"`
	BackupID    string     `json:"backup_id"`
	VersionID   string     `json:"version_id"`
	SpaceKey    string     `json:"space_key"`
	CreatedAt   time.Time  `json:"created_at"`
	Files       []FileMeta `json:"files"`
	TotalFiles  int        `json:"total_files"`
	TotalSize   int64      `json:"total_size"`
	CompletedAt *time.Time `json:"completed_at,omitempty"`
}

// FileMeta file metadata
type FileMeta struct {
	FilePath     string    `json:"file_path"`
	FileName     string    `json:"file_name"`
	CRC32        string    `json:"crc32"`
	Size         int64     `json:"size"`
	S3Path       string    `json:"s3_path"`
	LocalPath    string    `json:"local_path"`
	UploadedAt   time.Time `json:"uploaded_at"`
	UploadStatus string    `json:"upload_status"`
}

// ============================================================================
// Storage method on PS side
// SST files are managed uniformly under the cluster backup root directory, other metadata is saved according to directory structure
// ============================================================================
type PSShardManager struct {
	mu sync.RWMutex
	//spacekey-backupVersion
	versions map[string]services.VersionInfo
	// Records currently running shard tasks on this node, used to notify master side when shard tasks complete
	// Records task status: key is "spaceKey_backupID_partitionID"
	taskStatus     map[string]*SnapshotTask
	minioClient    *minio.Client
	getPartition   GetPartitionFunc
	refCountMgr    *RefCountManager            // Reference count manager
	refCountMgrs   map[string]*RefCountManager // Reference count managers stored by spaceKey (subdivided by database/table)
	bucketName     string                      // S3 bucket name
	clusterName    string                      // Cluster name
	sstStoragePath string                      // Unified storage path for SST files (on S3)
}

// NewPSShardManager creates a new PSShardManager instance
func NewPSShardManager(getPartition GetPartitionFunc, minioClient *minio.Client, bucketName, clusterName string) *PSShardManager {
	return &PSShardManager{
		getPartition: getPartition,
		minioClient:  minioClient,
		bucketName:   bucketName,
		clusterName:  clusterName,
		versions:     make(map[string]services.VersionInfo),
		taskStatus:   make(map[string]*SnapshotTask),
		refCountMgrs: make(map[string]*RefCountManager),
	}
}

type SnapshotTask struct {
	PartitionID       uint32         `json:"partition_id"`    // Current partition ID
	s3PartitionID     uint32         `json:"s3_partition_id"` // Partition ID on S3 (used during restore)
	status            SnapshotStatus `json:"status"`
	spaceKey          string         `json:"space_key"`
	backupID          string         `json:"backup_id"`                     // Task identifier (UUID)
	versionID         string         `json:"version_id"`                    // Version identifier (timestamp, used for reference counting)
	backupType        string         `json:"backup_type"`                   // Backup type: "full" for full backup, "incremental" for incremental backup
	sourceClusterName string         `json:"source_cluster_name,omitempty"` // Source cluster name (for cross-cluster restore)
	errorMessage      string         `json:"error_message,omitempty"`
	startTime         time.Time      `json:"start_time"`
	completeTime      time.Time      `json:"complete_time"`
}

type SnapshotStatus int

// Snapshot status at shard level, the entire shard is considered uploaded only when all files in the shard are uploaded
const (
	SnapshotStatusRunning SnapshotStatus = iota
	SnapshotStatusCompleted
	SnapshotStatusFailed
)

// startShardSnapshot starts shard snapshot on data node
func (s *PSShardManager) startShardSnapshot(
	task *SnapshotTask,
) {
	defer func() {
		if r := recover(); r != nil {
			log.Error("startShardSnapshot panic recovered for partition %d: %v", task.PartitionID, r)
			s.markTaskFailed(task, fmt.Errorf("panic: %v", r))
		}
	}()

	log.Info("Starting shard snapshot for partition %d, backupID=%s, versionID=%s", task.PartitionID, task.backupID, task.versionID)

	// Get partitionStore
	partitionID := task.PartitionID
	partitionStore := s.getPartition(partitionID)
	if partitionStore == nil {
		log.Error("Failed to get partition store: %v", partitionID)
		s.markTaskFailed(task, fmt.Errorf("partition store not found"))
		return
	}

	// Get partitionStore's engine
	engine := partitionStore.GetEngine()
	if engine == nil {
		log.Error("Failed to get engine: %v", partitionID)
		s.markTaskFailed(task, fmt.Errorf("engine not found"))
		return
	}

	// Get engine's configuration
	engineConfig := entity.SpaceConfig{}
	err := engine.GetEngineCfg(&engineConfig)

	if err != nil {
		log.Error("Failed to get engine config: %v", err)
		s.markTaskFailed(task, err)
		return
	}

	// Call gamma's backupspace method to create snapshot
	err = engine.BackupSpace("create")
	if err != nil {
		log.Error("failed to snapshot sst: %+v", err)
		s.markTaskFailed(task, err)
		return
	}

	// Get local absolute path
	filePath := *engineConfig.Path

	spaceKey := task.spaceKey
	backupID := task.backupID
	versionID := task.versionID
	backupType := task.backupType
	// Actually upload files
	err = s.executeShardSnapshot(context.Background(), filePath, spaceKey, backupID, versionID, partitionID, backupType)
	if err != nil {
		log.Error("Failed to execute shard snapshot: %v", err)
		s.markTaskFailed(task, err)
		return
	}

	s.markTaskCompleted(task)
	log.Info("Shard snapshot completed successfully for partition %d, backupID=%s, versionID=%s", task.PartitionID, task.backupID, task.versionID)
}

// startShardRestore starts shard restore on data node
func (s *PSShardManager) startShardRestore(
	task *SnapshotTask,
) {
	defer func() {
		if r := recover(); r != nil {
			log.Error("startShardRestore panic recovered for partition %d: %v", task.PartitionID, r)
			s.markTaskFailed(task, fmt.Errorf("panic: %v", r))
		}
	}()

	log.Info("Starting shard restore for partition %d, backupID=%s, versionID=%s", task.PartitionID, task.backupID, task.versionID)

	// Get partitionStore
	partitionID := task.PartitionID
	partitionStore := s.getPartition(partitionID)
	if partitionStore == nil {
		log.Error("Failed to get partition store: %v", partitionID)
		s.markTaskFailed(task, fmt.Errorf("partition store not found"))
		return
	}

	// Get partitionStore's engine
	engine := partitionStore.GetEngine()
	if engine == nil {
		log.Error("Failed to get engine: %v", partitionID)
		s.markTaskFailed(task, fmt.Errorf("engine not found"))
		return
	}

	// Get engine's configuration
	engineConfig := entity.SpaceConfig{}
	err := engine.GetEngineCfg(&engineConfig)
	if err != nil {
		log.Error("Failed to get engine config: %v", err)
		s.markTaskFailed(task, err)
		return
	}

	// Close Engine (must be closed before restore)
	engine.Close()

	// Ensure Engine is reloaded after completion
	defer func() {
		if err := engine.Load(); err != nil {
			log.Error("Failed to reload engine after restore: %v", err)
			s.markTaskFailed(task, err)
		}
	}()

	// Wait for Engine to fully close
	times := 0
	maxWaitTimes := 60
	for !engine.HasClosed() {
		time.Sleep(10 * time.Second)
		times++
		if times > maxWaitTimes {
			log.Error("Engine close timeout for partition %d", partitionID)
			s.markTaskFailed(task, fmt.Errorf("engine close timeout"))
			return
		}
		log.Debug("Waiting for engine to close, partition %d, times: %d", partitionID, times)
	}

	filePath := *engineConfig.Path

	spaceKey := task.spaceKey
	backupID := task.backupID
	versionID := task.versionID

	// Use S3 partition ID for restore, otherwise use current partition ID
	s3PartitionID := task.s3PartitionID
	if s3PartitionID == 0 {
		s3PartitionID = partitionID // Backup operation: S3 partition ID is the current partition ID
	}
	log.Info("Restore using S3 partition ID %d for current partition %d", s3PartitionID, partitionID)

	// Get source cluster name, if not specified use current cluster name (backward compatibility)
	sourceClusterName := task.sourceClusterName
	if sourceClusterName == "" {
		sourceClusterName = config.Conf().Global.Name
	}

	err = s.restoreShardSnapshot(context.Background(), filePath, spaceKey, backupID, versionID, partitionID, s3PartitionID, sourceClusterName)
	if err != nil {
		log.Error("Failed to execute shard restore: %v", err)
		s.markTaskFailed(task, err)
		return
	}

	s.markTaskCompleted(task)
	log.Info("Shard restore completed successfully for partition %d, backupID=%s, versionID=%s", task.PartitionID, task.backupID, task.versionID)
}

// getOrCreateRefCountManager gets or creates reference count manager for the corresponding spaceKey
// If creating a new instance, it will load corresponding reference count data from S3
func (s *PSShardManager) getOrCreateRefCountManager(ctx context.Context, spaceKey string) (*RefCountManager, error) {
	// If reference count manager for this spaceKey already exists, return directly
	s.mu.RLock()
	if rcm, exists := s.refCountMgrs[spaceKey]; exists {
		s.mu.RUnlock()
		return rcm, nil
	}
	s.mu.RUnlock()

	s.mu.Lock()
	defer s.mu.Unlock()

	if rcm, exists := s.refCountMgrs[spaceKey]; exists {
		return rcm, nil
	}

	// Initialize refCountMgrs map
	if s.refCountMgrs == nil {
		s.refCountMgrs = make(map[string]*RefCountManager)
	}

	clusterName := s.clusterName
	if clusterName == "" && config.Conf() != nil {
		clusterName = config.Conf().Global.Name
	}
	if clusterName == "" {
		clusterName = "default-cluster"
	}

	bucketName := s.bucketName
	if bucketName == "" && s.refCountMgr != nil {
		bucketName = s.refCountMgr.bucketName
	}
	if bucketName == "" {
		return nil, fmt.Errorf("bucketName is required but not set in PSShardManager")
	}

	rcm := NewRefCountManager(s.minioClient, bucketName, clusterName, spaceKey)

	if err := rcm.LoadFromS3(ctx); err != nil {
		log.Warn("Failed to load reference count data from S3 for spaceKey %s: %v, will start fresh", spaceKey, err)
	}

	s.refCountMgrs[spaceKey] = rcm

	if s.refCountMgr == nil {
		s.refCountMgr = rcm
	}

	return rcm, nil
}

func (s *PSShardManager) GetRefCountManager(ctx context.Context, spaceKey string) (*RefCountManager, error) {
	return s.getOrCreateRefCountManager(ctx, spaceKey)
}

// GetRefCountManagerIfExists gets existing reference count manager (does not create new instance, does not load data)
// Used for testing scenarios, returns instance if exists, otherwise returns nil
func (s *PSShardManager) GetRefCountManagerIfExists(spaceKey string) *RefCountManager {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.refCountMgrs[spaceKey]
}

// executeShardSnapshot executes actual shard snapshot logic
func (s *PSShardManager) executeShardSnapshot(
	ctx context.Context,
	localPath string,
	spaceKey string,
	backupID string,
	versionID string,
	partitionID uint32,
	backupType string,
) error {
	// Default to full backup
	if backupType == "" {
		backupType = "full"
	}
	// Get or create reference count manager for the corresponding spaceKey (subdivided by database/table)
	// If creating a new instance, it will automatically load corresponding reference count data from S3
	refCountMgr, err := s.getOrCreateRefCountManager(ctx, spaceKey)
	if err != nil {
		return fmt.Errorf("failed to get or create ref count manager: %v", err)
	}
	partitionMeta := PartitionBackupMeta{
		PartitionID: partitionID,
		SpaceKey:    spaceKey,
		BackupID:    backupID,
		VersionID:   versionID, // Use the provided versionID (timestamp)
		Files:       make([]FileMeta, 0),
	}

	// Build S3 path
	pathBuilder := NewS3PathBuilder(config.Conf().Global.Name)

	pathes := []string{
		"data",
		"bitmap",
	}

	const maxConcurrency = 20
	sem := make(chan struct{}, maxConcurrency)

	var uploadErrors []error

	type fileResult struct {
		fileMeta FileMeta
		err      error
	}

	for _, path := range pathes {
		backupDataLocalPath := filepath.Join(localPath, "backup", path)
		files, err := WalkDir(backupDataLocalPath)
		if err != nil {
			log.Error("Failed to walk directory: %v", err)
			return fmt.Errorf("failed to walk directory %s: %v", backupDataLocalPath, err)
		}

		resultChan := make(chan fileResult, len(files))
		s3Path := pathBuilder.BuildBackupPath(spaceKey, versionID, partitionID) + "/" + path

		for _, file := range files {
			// Acquire semaphore
			sem <- struct{}{}

			go func(filePath string) {

				defer func() { <-sem }()

				fileCRC32, err := calcFileCRC32(filePath)
				if err != nil {
					log.Error("Failed to calculate file CRC32: %v", err)
					resultChan <- fileResult{err: fmt.Errorf("failed to calculate CRC32 for %s: %v", filePath, err)}
					return
				}

				fileInfo, err := os.Stat(filePath)
				if err != nil {
					log.Error("Failed to get file info: %v", err)
					resultChan <- fileResult{err: fmt.Errorf("failed to get file info for %s: %v", filePath, err)}
					return
				}
				fileSize := fileInfo.Size()

				// Create file metadata
				fileMeta := FileMeta{
					FilePath:   filePath,
					FileName:   filepath.Base(filePath),
					CRC32:      fileCRC32,
					Size:       fileSize,
					LocalPath:  filePath,
					UploadedAt: time.Now(),
				}
				shouldReuse := backupType == "incremental"
				if shouldReuse {
					if refInfo, exists := refCountMgr.GetFileInfo(fileCRC32); exists {
						log.Info("File already exists in S3 (CRC32: %s), reusing with path: %s", fileCRC32, refInfo.S3Path)
						fileMeta.S3Path = refInfo.S3Path
						fileMeta.UploadedAt = time.Now()
						fileMeta.UploadStatus = "reused"
						if err := refCountMgr.AddReference(ctx, fileCRC32, refInfo.S3Path, versionID, fileSize); err != nil {
							log.Error("Failed to add reference: %v", err)
							resultChan <- fileResult{err: fmt.Errorf("failed to add reference for %s: %v", filePath, err)}
							return
						}
						resultChan <- fileResult{fileMeta: fileMeta}
						return
					}
				} else {
					log.Info("Full backup mode: uploading file %s (CRC32: %s) even if it exists in S3", filePath, fileCRC32)
				}

				fileName := filepath.Base(filePath)
				sstS3Path := filepath.Join(s3Path, fileName)
				fileMeta.S3Path = sstS3Path
				fileMeta.UploadedAt = time.Now()
				fileMeta.UploadStatus = "uploaded"
				if err := s.backupSstFilesInpath(ctx, filePath, sstS3Path); err != nil {
					log.Error("Failed to upload file to S3: %v", err)
					resultChan <- fileResult{err: fmt.Errorf("failed to upload %s to S3: %v", filePath, err)}
					return
				}

				if err := refCountMgr.AddReference(ctx, fileCRC32, sstS3Path, versionID, fileSize); err != nil {
					log.Error("Failed to add reference: %v", err)
					resultChan <- fileResult{err: fmt.Errorf("failed to add reference for %s: %v", filePath, err)}
					return
				}

				resultChan <- fileResult{fileMeta: fileMeta}
			}(file)
		}

		for i := 0; i < len(files); i++ {
			result := <-resultChan
			if result.err != nil {
				uploadErrors = append(uploadErrors, result.err)
			} else {
				partitionMeta.Files = append(partitionMeta.Files, result.fileMeta)
				partitionMeta.TotalFiles++
				partitionMeta.TotalSize += result.fileMeta.Size
			}
		}
	}

	if len(uploadErrors) > 0 {
		if err := s.savePartitionMeta(ctx, &partitionMeta, pathBuilder, spaceKey, versionID, partitionID); err != nil {
			log.Error("Failed to save partition meta for failed backup: %v", err)
		}
		return fmt.Errorf("encountered %d errors during backup: %v", len(uploadErrors), uploadErrors[0])
	}

	completedAt := time.Now()
	partitionMeta.CompletedAt = &completedAt

	if err := s.savePartitionMeta(ctx, &partitionMeta, pathBuilder, spaceKey, versionID, partitionID); err != nil {
		log.Error("Failed to save partition meta: %v", err)
		return fmt.Errorf("failed to save partition meta: %v", err)
	}

	backupDir := filepath.Join(localPath, "backup")
	if err := os.RemoveAll(backupDir); err != nil {
		log.Warn("Failed to remove backup directory %s after successful backup: %v", backupDir, err)
	} else {
		log.Info("Successfully removed backup directory: %s", backupDir)
	}

	log.Info("Successfully completed backup for partition %d with %d files (total size: %d bytes)",
		partitionID, partitionMeta.TotalFiles, partitionMeta.TotalSize)
	return nil
}

func (s *PSShardManager) backupSstFilesInpath(ctx context.Context, localFilePath string, s3ObjectPath string) error {
	bucketName := s.bucketName
	if bucketName == "" && s.refCountMgr != nil {
		bucketName = s.refCountMgr.bucketName
	}

	config := defaultRetryConfig
	err := retryOperation(func() error {
		_, err := s.minioClient.FPutObject(ctx, bucketName, s3ObjectPath, localFilePath,
			minio.PutObjectOptions{ContentType: "application/octet-stream"})
		if err != nil {
			return fmt.Errorf("failed to upload file to S3: %v", err)
		}
		return nil
	}, config)

	if err != nil {
		return err
	}

	log.Info("Successfully uploaded file %s to S3 path %s", localFilePath, s3ObjectPath)
	return nil
}

// savePartitionMeta saves partition metadata to JSON file and uploads to S3
func (s *PSShardManager) savePartitionMeta(ctx context.Context, meta *PartitionBackupMeta, pathBuilder *S3PathBuilder, spaceKey string, versionID string, partitionID uint32) error {
	jsonData, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal partition meta to JSON: %v", err)
	}

	tmpFile, err := os.CreateTemp("", "partition_meta_*.json")
	if err != nil {
		return fmt.Errorf("failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	if _, err := tmpFile.Write(jsonData); err != nil {
		return fmt.Errorf("failed to write partition meta to temp file: %v", err)
	}

	// Path format: {clusterName}/backup/{dbName}/{spaceName}/{versionID}/{partitionID}/partition_meta.json
	metaS3Path := pathBuilder.BuildBackupPath(spaceKey, versionID, partitionID) + "/partition_meta.json"
	log.Info("Uploading partition meta to S3: %s", metaS3Path)

	bucketName := s.bucketName
	if bucketName == "" && s.refCountMgr != nil {
		bucketName = s.refCountMgr.bucketName
	}

	config := defaultRetryConfig
	if err := retryOperation(func() error {
		_, err := s.minioClient.FPutObject(ctx, bucketName, metaS3Path, tmpFile.Name(),
			minio.PutObjectOptions{ContentType: "application/json"})
		if err != nil {
			return fmt.Errorf("failed to upload partition meta to S3: %v", err)
		}
		return nil
	}, config); err != nil {
		return err
	}

	log.Info("Successfully saved partition meta for partition %d (backupID: %s)",
		meta.PartitionID, meta.BackupID)
	return nil
}

// loadPartitionMeta loads partition metadata file from S3
func (s *PSShardManager) loadPartitionMeta(ctx context.Context, spaceKey string, versionID string, partitionID uint32, sourceClusterName string) (*PartitionBackupMeta, error) {
	// Path format: {clusterName}/backup/{dbName}/{spaceName}/{versionID}/{partitionID}/partition_meta.json
	pathBuilder := NewS3PathBuilder(sourceClusterName)
	metaS3Path := pathBuilder.BuildBackupPath(spaceKey, versionID, partitionID) + "/partition_meta.json"
	log.Info("Loading partition meta from S3: %s", metaS3Path)

	bucketName := s.bucketName
	if bucketName == "" && s.refCountMgr != nil {
		bucketName = s.refCountMgr.bucketName
	}

	tmpFile, err := os.CreateTemp("", "partition_meta_*.json")
	if err != nil {
		return nil, fmt.Errorf("failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	// Download metadata file using retry mechanism, minio's FGetObject and FPutObject already have built-in chunk transfer
	config := defaultRetryConfig
	err = retryOperation(func() error {
		downloadErr := s.minioClient.FGetObject(ctx, bucketName, metaS3Path, tmpFile.Name(), minio.GetObjectOptions{})
		if downloadErr != nil {
			return fmt.Errorf("failed to download partition meta from S3: %v", downloadErr)
		}
		return nil
	}, config)
	if err != nil {
		return nil, err
	}

	jsonData, err := os.ReadFile(tmpFile.Name())
	if err != nil {
		return nil, fmt.Errorf("failed to read partition meta file: %v", err)
	}

	var meta PartitionBackupMeta
	if err := json.Unmarshal(jsonData, &meta); err != nil {
		return nil, fmt.Errorf("failed to unmarshal partition meta: %v", err)
	}

	log.Info("Successfully loaded partition meta: partitionID=%d, backupID=%s, files=%d, totalSize=%d",
		meta.PartitionID, meta.BackupID, meta.TotalFiles, meta.TotalSize)
	return &meta, nil
}

// restoreShardSnapshot restores shard snapshot (downloads files to local based on metadata file)
func (s *PSShardManager) restoreShardSnapshot(
	ctx context.Context,
	localPath string,
	spaceKey string,
	backupID string,
	versionID string,
	partitionID uint32,
	s3PartitionID uint32,
	sourceClusterName string,
) error {
	log.Info("Starting restore for partition %d (backupID: %s, versionID: %s, spaceKey: %s, s3PartitionID: %d)",
		partitionID, backupID, versionID, spaceKey, s3PartitionID)

	meta, err := s.loadPartitionMeta(ctx, spaceKey, versionID, s3PartitionID, sourceClusterName)
	if err != nil {
		return fmt.Errorf("failed to load partition meta for S3 partition %d: %v", s3PartitionID, err)
	}

	if meta.PartitionID != s3PartitionID || meta.VersionID != versionID || meta.SpaceKey != spaceKey {
		return fmt.Errorf("partition meta mismatch: expected s3PartitionID=%d versionID=%s spaceKey=%s, got partitionID=%d versionID=%s spaceKey=%s",
			s3PartitionID, versionID, spaceKey, meta.PartitionID, meta.VersionID, meta.SpaceKey)
	}

	if err := os.MkdirAll(localPath, 0755); err != nil {
		return fmt.Errorf("failed to create local directory: %v", err)
	}

	dataDir := filepath.Join(localPath, "data")
	bitmapDir := filepath.Join(localPath, "bitmap")

	log.Info("Cleaning data and bitmap directories before restore: %s, %s", dataDir, bitmapDir)
	if err := os.RemoveAll(dataDir); err != nil {
		return fmt.Errorf("failed to clean data directory: %v", err)
	}
	if err := os.RemoveAll(bitmapDir); err != nil {
		return fmt.Errorf("failed to clean bitmap directory: %v", err)
	}

	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return fmt.Errorf("failed to create data directory: %v", err)
	}
	if err := os.MkdirAll(bitmapDir, 0755); err != nil {
		return fmt.Errorf("failed to create bitmap directory: %v", err)
	}

	bucketName := s.bucketName
	if bucketName == "" && s.refCountMgr != nil {
		bucketName = s.refCountMgr.bucketName
	}

	var downloadErrors []error
	var wg sync.WaitGroup

	log.Info("Starting to download %d files from S3", len(meta.Files))

	for _, fileMeta := range meta.Files {
		if fileMeta.FileName == "LOCK" {
			log.Debug("Skipping RocksDB LOCK file during restore: %s", fileMeta.FileName)
			continue
		}

		wg.Add(1)
		go func(fm FileMeta) {
			defer wg.Done()

			// Determine local file path
			// S3Path format: cluster/backup/db/space/version/partition/bitmap/file
			var localFilePath string
			if strings.Contains(fm.S3Path, "/bitmap/") {
				localFilePath = filepath.Join(bitmapDir, fm.FileName)
			} else if strings.Contains(fm.S3Path, "/data/") {
				localFilePath = filepath.Join(dataDir, fm.FileName)
			} else if strings.Contains(fm.FilePath, "/bitmap/") || strings.Contains(fm.LocalPath, "/bitmap/") {
				localFilePath = filepath.Join(bitmapDir, fm.FileName)
			} else if strings.Contains(fm.FilePath, "/data/") || strings.Contains(fm.LocalPath, "/data/") {
				localFilePath = filepath.Join(dataDir, fm.FileName)
			} else {
				localFilePath = filepath.Join(dataDir, fm.FileName)
			}

			// Ensure directory exists
			if err := os.MkdirAll(filepath.Dir(localFilePath), 0755); err != nil {
				downloadErrors = append(downloadErrors, fmt.Errorf("failed to create directory for %s: %v", fm.FileName, err))
				return
			}

			log.Debug("Downloading file from S3: %s -> %s", fm.S3Path, localFilePath)
			config := defaultRetryConfig
			err := retryOperation(func() error {
				err := s.minioClient.FGetObject(ctx, bucketName, fm.S3Path, localFilePath, minio.GetObjectOptions{})
				if err != nil {
					return fmt.Errorf("failed to download file %s from S3: %v", fm.S3Path, err)
				}
				return nil
			}, config)
			if err != nil {
				downloadErrors = append(downloadErrors, err)
				return
			}

			// Verify file
			// fileCRC32, err := calcFileCRC32(localFilePath)
			// if (fileCRC32 != fm.CRC32) {
			// 	errorMu.Lock()
			// 	downloadErrors = append(downloadErrors, fmt.Errorf("failed to stat downloaded file %s: %v", localFilePath, err))
			// 	errorMu.Unlock()
			// 	return
			// }
			if fm.Size > 0 {
				fileCRC32, err := calcFileCRC32(localFilePath)
				if err != nil || fileCRC32 != fm.CRC32 {
					downloadErrors = append(downloadErrors, fmt.Errorf("failed to stat downloaded file %s: %v", localFilePath, err))
					if fileCRC32 != fm.CRC32 {
						log.Warn("File size mismatch for %s: expected %d, got %d", localFilePath, fm.CRC32, fileCRC32)
					}
					return
				}
			}
			// if fm.Size > 0 {
			// 	fileInfo, err := os.Stat(localFilePath)
			// 	if err != nil {
			// 		errorMu.Lock()
			// 		downloadErrors = append(downloadErrors, fmt.Errorf("failed to stat downloaded file %s: %v", localFilePath, err))
			// 		errorMu.Unlock()
			// 		return
			// 	}
			// 	if fileInfo.Size() != fm.Size {
			// 		log.Warn("File size mismatch for %s: expected %d, got %d", localFilePath, fm.Size, fileInfo.Size())
			// 	}
			// }

			log.Debug("Successfully downloaded file: %s", localFilePath)
		}(fileMeta)
	}

	wg.Wait()

	if len(downloadErrors) > 0 {
		log.Error("Encountered %d errors during file download. First error: %v", len(downloadErrors), downloadErrors[0])
		maxErrorsToLog := 10
		if len(downloadErrors) < maxErrorsToLog {
			maxErrorsToLog = len(downloadErrors)
		}
		for i := 0; i < maxErrorsToLog; i++ {
			log.Error("Download error %d/%d: %v", i+1, len(downloadErrors), downloadErrors[i])
		}
		if len(downloadErrors) > maxErrorsToLog {
			log.Error("... and %d more download errors", len(downloadErrors)-maxErrorsToLog)
		}
		return fmt.Errorf("encountered %d errors during restore: %v", len(downloadErrors), downloadErrors[0])
	}

	log.Info("Successfully restored partition %d with %d files (total size: %d bytes) to %s",
		partitionID, meta.TotalFiles, meta.TotalSize, localPath)
	return nil
}

// DeleteBackupVersion deletes backup version
func (s *PSShardManager) DeleteBackupVersion(ctx context.Context, spaceKey string, versionID string) error {
	log.Info("Deleting backup version: spaceKey=%s, versionID=%s", spaceKey, versionID)

	// Get or create reference count manager for the corresponding spaceKey
	refCountMgr, err := s.getOrCreateRefCountManager(ctx, spaceKey)
	if err != nil {
		return fmt.Errorf("failed to get ref count manager: %v", err)
	}

	return refCountMgr.RemoveVersionReferences(ctx, versionID)
}

// ============================================================================
// =========================Helper Functions============================================
// ============================================================================

// retryConfig retry configuration
type retryConfig struct {
	maxRetries int
	retryDelay time.Duration
}

// defaultRetryConfig default retry configuration
var defaultRetryConfig = retryConfig{
	maxRetries: 3,
	retryDelay: 2 * time.Second,
}

func retryOperation(operation func() error, config retryConfig) error {
	var lastErr error
	retryDelay := config.retryDelay
	for attempt := 0; attempt <= config.maxRetries; attempt++ {
		if attempt > 0 {
			log.Info("Retrying operation (attempt %d/%d) after %v delay", attempt, config.maxRetries+1, retryDelay)
			time.Sleep(retryDelay)
			retryDelay *= 2
		}

		err := operation()
		if err == nil {
			if attempt > 0 {
				log.Info("Operation succeeded after %d retries", attempt)
			}
			return nil
		}

		lastErr = err
		log.Warn("Operation failed (attempt %d/%d): %v", attempt+1, config.maxRetries+1, err)
	}

	return fmt.Errorf("operation failed after %d attempts: %v", config.maxRetries+1, lastErr)
}

func WalkDir(root string) ([]string, error) {
	var files []string

	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {

			fileName := filepath.Base(path)
			if fileName == "LOCK" {
				log.Debug("Skipping RocksDB LOCK file: %s", path)
				return nil
			}
			files = append(files, path)
		}
		return nil
	})

	return files, err
}

func calcFileCRC32(filename string) (string, error) {

	file, err := os.Open(filename)
	if err != nil {
		return "", err
	}
	defer file.Close()

	hash := crc32.NewIEEE()

	buffer := make([]byte, 4096)
	for {
		n, err := file.Read(buffer)
		if n > 0 {
			hash.Write(buffer[:n])
		}
		if err != nil {
			break
		}
	}

	crc := hash.Sum32()
	return fmt.Sprintf("%08x", crc), nil
}

type S3PathBuilder struct {
	clusterName string
}

func NewS3PathBuilder(clusterName string) *S3PathBuilder {
	return &S3PathBuilder{
		clusterName: clusterName,
	}
}

func (b *S3PathBuilder) BuildBackupPath(spaceKey string, versionID string, pid uint32) string {
	parts := strings.Split(spaceKey, "-")
	return b.BuildObjectPath("backup", parts[0], parts[1], fmt.Sprintf("%s/%d", versionID, pid))
}

func (b *S3PathBuilder) BuildObjectPath(parts ...string) string {
	allParts := make([]string, 0, len(parts)+1)
	allParts = append(allParts, b.clusterName)

	for _, part := range parts {
		if part != "" {
			allParts = append(allParts, part)
		}
	}

	return filepath.ToSlash(filepath.Join(allParts...))
}

// ============================================================================
// =========================Task Status Management========================================
// ============================================================================

// getTaskKey generates unique key for task
func getTaskKey(spaceKey string, backupID string, partitionID uint32) string {
	return fmt.Sprintf("%s_%s_%d", spaceKey, backupID, partitionID)
}

// registerTask registers task
func (s *PSShardManager) registerTask(task *SnapshotTask) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.taskStatus == nil {
		s.taskStatus = make(map[string]*SnapshotTask)
	}

	key := getTaskKey(task.spaceKey, task.backupID, task.PartitionID)
	s.taskStatus[key] = task
	log.Info("Registered backup task: %s", key)
}

// markTaskCompleted marks task as completed
func (s *PSShardManager) markTaskCompleted(task *SnapshotTask) {
	s.mu.Lock()
	defer s.mu.Unlock()

	task.status = SnapshotStatusCompleted
	task.completeTime = time.Now()
	task.errorMessage = ""

	key := getTaskKey(task.spaceKey, task.backupID, task.PartitionID)
	log.Info("Task completed: %s, duration: %v", key, task.completeTime.Sub(task.startTime))
}

// markTaskFailed marks task as failed
func (s *PSShardManager) markTaskFailed(task *SnapshotTask, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	task.status = SnapshotStatusFailed
	task.completeTime = time.Now()
	if err != nil {
		task.errorMessage = err.Error()
	}

	key := getTaskKey(task.spaceKey, task.backupID, task.PartitionID)
	log.Error("Task failed: %s, error: %s, duration: %v", key, task.errorMessage, task.completeTime.Sub(task.startTime))
}

// GetTaskStatus gets task status (for Master to query)
func (s *PSShardManager) GetTaskStatus(spaceKey string, backupID string, partitionID uint32) *SnapshotTask {
	s.mu.RLock()
	defer s.mu.RUnlock()

	key := getTaskKey(spaceKey, backupID, partitionID)
	task, exists := s.taskStatus[key]
	if !exists {
		log.Warn("Task not found for key: %s", key)
		return nil
	}

	taskCopy := &SnapshotTask{
		PartitionID:  task.PartitionID,
		status:       task.status,
		spaceKey:     task.spaceKey,
		backupID:     task.backupID,
		errorMessage: task.errorMessage,
		startTime:    task.startTime,
		completeTime: task.completeTime,
	}
	return taskCopy
}

// CleanupTask cleans up completed or failed tasks
func (s *PSShardManager) CleanupTask(spaceKey string, backupID string, partitionID uint32) {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := getTaskKey(spaceKey, backupID, partitionID)
	delete(s.taskStatus, key)
	log.Info("Cleaned up task: %s", key)
}

// StartPartitionBackup starts partition backup
func (s *PSShardManager) StartPartitionBackup(spaceKey string, backupID string, versionID string, partitionID uint32, backupType string) error {
	// Default to full backup
	if backupType == "" {
		backupType = "full"
	}
	// Validate backup type
	if backupType != "full" && backupType != "incremental" {
		return fmt.Errorf("invalid backup type: %s, must be 'full' or 'incremental'", backupType)
	}

	log.Info("Starting partition backup: spaceKey=%s, backupID=%s, versionID=%s, partitionID=%d, backupType=%s", spaceKey, backupID, versionID, partitionID, backupType)

	// Check if task already exists
	if task := s.GetTaskStatus(spaceKey, backupID, partitionID); task != nil {
		if task.status == SnapshotStatusRunning {
			log.Error("Backup already running! spaceKey=%s, backupID=%s, partitionID=%d", spaceKey, backupID, partitionID)
			return fmt.Errorf("backup task already running for partition %d", partitionID)
		}
	}

	// Create new task
	task := &SnapshotTask{
		PartitionID:  partitionID,
		status:       SnapshotStatusRunning,
		spaceKey:     spaceKey,
		backupID:     backupID,
		versionID:    versionID,  // Set versionID
		backupType:   backupType, // Set backup type
		startTime:    time.Now(),
		errorMessage: "",
	}

	// Register task
	s.registerTask(task)

	// Execute backup asynchronously
	go s.startShardSnapshot(task)

	return nil
}

func (s *PSShardManager) StartPartitionRestore(spaceKey string, backupID string, versionID string, partitionID uint32, s3PartitionID uint32, sourceClusterName string) error {
	log.Info("Starting partition restore: spaceKey=%s, backupID=%s, versionID=%s, partitionID=%d, s3PartitionID=%d, sourceClusterName=%s",
		spaceKey, backupID, versionID, partitionID, s3PartitionID, sourceClusterName)

	// Check if task already exists
	if task := s.GetTaskStatus(spaceKey, backupID, partitionID); task != nil {
		if task.status == SnapshotStatusRunning {
			log.Error("Restore already running! spaceKey=%s, backupID=%s, partitionID=%d", spaceKey, backupID, partitionID)
			return fmt.Errorf("restore task already running for partition %d", partitionID)
		}
	}

	// Create new task, store S3 partition ID and source cluster name
	task := &SnapshotTask{
		PartitionID:       partitionID,
		status:            SnapshotStatusRunning,
		spaceKey:          spaceKey,
		backupID:          backupID,
		versionID:         versionID,         // Set versionID
		s3PartitionID:     s3PartitionID,     // Store S3 partition ID
		sourceClusterName: sourceClusterName, // Store source cluster name
		startTime:         time.Now(),
		errorMessage:      "",
	}

	// Register task
	s.registerTask(task)

	// Execute restore asynchronously
	go s.startShardRestore(task)

	return nil
}

// GetBackupTaskStatus queries backup task status (implements BackupManager interface)
// Returns: status (0=running, 1=completed, 2=failed), errorMsg, exists
func (s *PSShardManager) GetBackupTaskStatus(spaceKey string, backupID string, partitionID uint32) (status int, errorMsg string, exists bool) {
	task := s.GetTaskStatus(spaceKey, backupID, partitionID)
	if task == nil {
		return 0, "", false
	}

	return int(task.status), task.errorMessage, true
}
