package backup

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/vearch/vearch/v3/internal/pkg/log"
)

const (
	MetadataDirName  = "metadata"
	RefCountFileName = "file_refcount.json"
)

// FileRefCount file reference count structure
type FileRefCount struct {
	CRC32         string    `json:"crc32"`
	S3Path        string    `json:"s3_path"`
	RefCount      int       `json:"ref_count"`
	Size          int64     `json:"size"`
	CreatedAt     time.Time `json:"created_at"`
	LastUpdatedAt time.Time `json:"last_updated_at"`
	Versions      []string  `json:"versions"`
}

// RefCountManager reference count manager
type RefCountManager struct {
	mu           sync.RWMutex
	refCountMap  map[string]*FileRefCount // key: CRC32, value: FileRefCount
	minioClient  *minio.Client
	bucketName   string
	clusterName  string
	spaceKey     string // Database-table identifier, format: "dbName-spaceName"
	metadataPath string // S3 path for storing reference count metadata

	SaveToS3         func(ctx context.Context) error
	deleteFileFromS3 func(ctx context.Context, s3Path string) error
}

// NewRefCountManager creates a reference count manager
func NewRefCountManager(minioClient *minio.Client, bucketName string, clusterName string, spaceKey string) *RefCountManager {
	rcm := &RefCountManager{
		refCountMap: make(map[string]*FileRefCount),
		minioClient: minioClient,
		bucketName:  bucketName,
		clusterName: clusterName,
		spaceKey:    spaceKey,
	}

	// Build metadata path based on spaceKey
	rcm.metadataPath = rcm.buildMetadataPath(clusterName, spaceKey)

	// Set default implementations
	rcm.SaveToS3 = rcm.saveToS3Impl
	rcm.deleteFileFromS3 = rcm.deleteFileFromS3Impl
	return rcm
}

// buildMetadataPath builds metadata storage path
// If spaceKey is not empty: clusterName/metadata/{dbName}/{spaceName}/file_refcount.json
func (rcm *RefCountManager) buildMetadataPath(clusterName string, spaceKey string) string {
	if spaceKey == "" {
		return filepath.Join(clusterName, MetadataDirName, RefCountFileName)
	}

	// Subdivided by database/table: clusterName/metadata/{dbName}/{spaceName}/file_refcount.json
	parts := strings.Split(spaceKey, "-")
	if len(parts) >= 2 {
		dbName := parts[0]
		spaceName := parts[1]
		return filepath.Join(clusterName, MetadataDirName, dbName, spaceName, RefCountFileName)
	}

	log.Warn("Invalid spaceKey format: %s, falling back to cluster-level storage", spaceKey)
	return filepath.Join(clusterName, MetadataDirName, RefCountFileName)
}

// GetMetadataPath gets current metadata path
func (rcm *RefCountManager) GetMetadataPath() string {
	rcm.mu.RLock()
	defer rcm.mu.RUnlock()
	return rcm.metadataPath
}

// LoadFromS3 loads reference count metadata from S3
func (rcm *RefCountManager) LoadFromS3(ctx context.Context) error {
	rcm.mu.Lock()
	defer rcm.mu.Unlock()

	// Download reference count metadata using retry mechanism
	config := defaultRetryConfig
	var obj *minio.Object

	retryErr := retryOperation(func() error {
		var retryErr error
		obj, retryErr = rcm.minioClient.GetObject(ctx, rcm.bucketName, rcm.metadataPath, minio.GetObjectOptions{})
		if retryErr != nil {
			errResp := minio.ToErrorResponse(retryErr)
			if errResp.Code == "NoSuchKey" || errResp.Code == "" {
				log.Info("No existing reference count metadata found, starting fresh")
				obj = nil
				return nil
			}
			return fmt.Errorf("failed to get reference count metadata from S3: %v", retryErr)
		}
		return nil
	}, config)

	if retryErr != nil {
		return retryErr
	}

	if obj == nil {
		return nil
	}

	defer func(obj *minio.Object) {
		err := obj.Close()
		if err != nil {
			log.Error("Failed to close object: %v", err)
		}
	}(obj)

	decoder := json.NewDecoder(obj)
	if err := decoder.Decode(&rcm.refCountMap); err != nil {
		return fmt.Errorf("failed to decode reference count metadata: %v", err)
	}

	log.Info("Loaded %d file reference counts from S3", len(rcm.refCountMap))
	return nil
}

// saveToS3Impl actual implementation for saving reference count metadata to S3
func (rcm *RefCountManager) saveToS3Impl(ctx context.Context) error {
	saveCtx, saveCancel := context.WithTimeout(ctx, 5*time.Minute)
	defer saveCancel()

	rcm.mu.RLock()
	refCountMapSize := len(rcm.refCountMap)
	data, err := json.MarshalIndent(rcm.refCountMap, "", "  ")
	rcm.mu.RUnlock()

	if err != nil {
		return fmt.Errorf("failed to marshal reference count metadata: %v", err)
	}

	tmpFilePattern := "file_refcount_*.json"
	if rcm.spaceKey != "" {
		safeSpaceKey := strings.ReplaceAll(strings.ReplaceAll(rcm.spaceKey, "/", "_"), "-", "_")
		tmpFilePattern = fmt.Sprintf("file_refcount_%s_*.json", safeSpaceKey)
	}

	tmpFile, err := os.CreateTemp("", tmpFilePattern)
	if err != nil {
		return fmt.Errorf("failed to create temp file: %v", err)
	}
	tmpFilePath := tmpFile.Name()

	defer func() {
		if err := os.Remove(tmpFilePath); err != nil && !os.IsNotExist(err) {
			log.Error("Failed to remove temp file %s: %v", tmpFilePath, err)
		}
	}()

	if _, err := tmpFile.Write(data); err != nil {
		return fmt.Errorf("failed to write temp file: %v", err)
	}

	if err := tmpFile.Close(); err != nil {
		return fmt.Errorf("failed to close temp file: %v", err)
	}

	log.Info("Saving %d file reference counts to S3 (path: %s)", refCountMapSize, rcm.metadataPath)

	config := defaultRetryConfig
	err = retryOperation(func() error {
		_, err := rcm.minioClient.FPutObject(saveCtx, rcm.bucketName, rcm.metadataPath, tmpFilePath,
			minio.PutObjectOptions{ContentType: "application/json"})
		if err != nil {
			if saveCtx.Err() == context.DeadlineExceeded {
				return fmt.Errorf("timeout uploading reference count metadata to S3: %v", err)
			}
			return fmt.Errorf("failed to upload reference count metadata to S3: %v", err)
		}
		return nil
	}, config)
	if err != nil {
		return err
	}

	log.Info("Successfully saved %d file reference counts to S3", refCountMapSize)
	return nil
}

// AddReference increments file reference count
func (rcm *RefCountManager) AddReference(ctx context.Context, crc32 string, s3Path string, versionID string, fileSize int64) error {

	rcm.mu.Lock()
	if refCount, exists := rcm.refCountMap[crc32]; exists {
		refCount.RefCount++
		refCount.LastUpdatedAt = time.Now()
		if !contains(refCount.Versions, versionID) {
			refCount.Versions = append(refCount.Versions, versionID)
		}
		log.Info("Incremented reference count for file %s (CRC32: %s) to %d", s3Path, crc32, refCount.RefCount)
	} else {
		rcm.refCountMap[crc32] = &FileRefCount{
			CRC32:         crc32,
			S3Path:        s3Path,
			RefCount:      1,
			Size:          fileSize,
			CreatedAt:     time.Now(),
			LastUpdatedAt: time.Now(),
			Versions:      []string{versionID},
		}
		log.Info("Created reference count for new file %s (CRC32: %s)", s3Path, crc32)
	}
	rcm.mu.Unlock()

	return rcm.SaveToS3(ctx)
}

// RemoveReference decrements file reference count, deletes file from S3 if count reaches zero
func (rcm *RefCountManager) RemoveReference(ctx context.Context, crc32 string, versionID string) error {
	rcm.mu.Lock()
	defer rcm.mu.Unlock()

	refCount, exists := rcm.refCountMap[crc32]
	if !exists {
		return fmt.Errorf("file with CRC32 %s not found in reference count map", crc32)
	}

	refCount.RefCount--
	refCount.LastUpdatedAt = time.Now()

	refCount.Versions = removeString(refCount.Versions, versionID)

	log.Info("Decremented reference count for file %s (CRC32: %s) to %d", refCount.S3Path, crc32, refCount.RefCount)

	if refCount.RefCount <= 0 {
		log.Info("Reference count reached 0 for file %s (CRC32: %s), deleting from S3", refCount.S3Path, crc32)
		if err := rcm.deleteFileFromS3(ctx, refCount.S3Path); err != nil {
			log.Error("Failed to delete file %s from S3: %v", refCount.S3Path, err)
		}
		delete(rcm.refCountMap, crc32)
	}

	return rcm.SaveToS3(ctx)
}

// RemoveVersionReferences removes all file references for a version
func (rcm *RefCountManager) RemoveVersionReferences(ctx context.Context, versionID string) error {
	rcm.mu.Lock()
	filesToRemove := make([]string, 0)

	for crc32, refCount := range rcm.refCountMap {
		if contains(refCount.Versions, versionID) {
			filesToRemove = append(filesToRemove, crc32)
		}
	}
	rcm.mu.Unlock()

	var errors []error
	for _, crc32 := range filesToRemove {
		if err := rcm.RemoveReference(ctx, crc32, versionID); err != nil {
			log.Error("Failed to remove reference for CRC32 %s: %v", crc32, err)
			errors = append(errors, fmt.Errorf("failed to remove reference for CRC32 %s: %v", crc32, err))
		}
	}

	log.Info("Removed references for version %s, affected %d files", versionID, len(filesToRemove))

	if len(errors) > 0 {
		return fmt.Errorf("failed to remove %d out of %d file references: %v", len(errors), len(filesToRemove), errors[0])
	}

	return nil
}

// deleteFileFromS3Impl actual implementation for deleting file from S3
func (rcm *RefCountManager) deleteFileFromS3Impl(ctx context.Context, s3Path string) error {
	config := defaultRetryConfig
	err := retryOperation(func() error {
		err := rcm.minioClient.RemoveObject(ctx, rcm.bucketName, s3Path, minio.RemoveObjectOptions{})
		if err != nil {
			return fmt.Errorf("failed to delete file from S3: %v", err)
		}
		return nil
	}, config)

	if err != nil {
		return err
	}

	log.Info("Successfully deleted file %s from S3", s3Path)
	return nil
}

// GetFileInfo gets file reference count information
func (rcm *RefCountManager) GetFileInfo(crc32 string) (*FileRefCount, bool) {
	rcm.mu.RLock()
	defer rcm.mu.RUnlock()

	refCount, exists := rcm.refCountMap[crc32]
	return refCount, exists
}

// GetTotalFiles gets total number of files
func (rcm *RefCountManager) GetTotalFiles() int {
	rcm.mu.RLock()
	defer rcm.mu.RUnlock()
	return len(rcm.refCountMap)
}

// GetTotalSize gets total size of all files
func (rcm *RefCountManager) GetTotalSize() int64 {
	rcm.mu.RLock()
	defer rcm.mu.RUnlock()

	var totalSize int64
	for _, refCount := range rcm.refCountMap {
		totalSize += refCount.Size
	}
	return totalSize
}

// ListOrphanedFiles lists orphaned files with reference count 0
func (rcm *RefCountManager) ListOrphanedFiles() []*FileRefCount {
	rcm.mu.RLock()
	defer rcm.mu.RUnlock()

	orphaned := make([]*FileRefCount, 0)
	for _, refCount := range rcm.refCountMap {
		if refCount.RefCount <= 0 {
			orphaned = append(orphaned, refCount)
		}
	}
	return orphaned
}

// GetVersionFiles gets all files referenced by specified version
func (rcm *RefCountManager) GetVersionFiles(versionID string) []*FileRefCount {
	rcm.mu.RLock()
	defer rcm.mu.RUnlock()

	files := make([]*FileRefCount, 0)
	for _, refCount := range rcm.refCountMap {
		for _, version := range refCount.Versions {
			if version == versionID {
				files = append(files, refCount)
				break
			}
		}
	}
	return files
}

// ListAllFiles lists all files
func (rcm *RefCountManager) ListAllFiles() []*FileRefCount {
	rcm.mu.RLock()
	defer rcm.mu.RUnlock()

	files := make([]*FileRefCount, 0, len(rcm.refCountMap))
	for _, refCount := range rcm.refCountMap {
		files = append(files, refCount)
	}
	return files
}

// contains checks if string slice contains specified string
func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}

// removeString removes specified string from string slice
func removeString(slice []string, item string) []string {
	result := make([]string, 0, len(slice))
	for _, s := range slice {
		if s != item {
			result = append(result, s)
		}
	}
	return result
}
