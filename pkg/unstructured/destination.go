package unstructured

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/awsclienthandler"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/filestore"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/snowflake"
	"github.com/snowflakedb/gosnowflake"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

const (
	// Default artifact paths for destination sync
	DefaultProcessedDocumentsPath = "processed-documents"
	DefaultChunksPath             = "chunks"
	DefaultVectorEmbeddingsPath   = "vector-embeddings"
)

type Destination interface {
	// SyncFilesToDestination will sync the data to the destination
	SyncFilesToDestination(ctx context.Context, fs *filestore.FileStore, filePaths []string) error
}

type SnowflakeInternalStage struct {
	Client          *snowflake.Client
	Role            string
	Database        string
	Schema          string
	Stage           string
	ArtifactPathMap map[string]string // map[fileSuffix]artifactPath for stages structure
}

func (d *SnowflakeInternalStage) SyncFilesToDestination(ctx context.Context,
	fs *filestore.FileStore, filePaths []string) error {
	logger := log.FromContext(ctx)
	logger.Info("ingesting data to snowflake internal stage", "filePaths", filePaths)

	// get all files currently in the stage
	type row struct {
		Filename string `db:"filename"`
		Data     string `db:"data"`
	}
	rows := []row{}
	err := d.Client.ListFilesFromStage(ctx, d.Role, d.Database, d.Schema, d.Stage, &rows)
	if err != nil {
		// The query fails when non-JSON files exist in the stage
		// (e.g. manually uploaded test data). Log the error and proceed with
		// an empty stage — all local files will be re-uploaded (PUT uses
		// OVERWRITE=TRUE) and no extra-file cleanup will happen this cycle.
		logger.Error(err, "failed to list files from stage, will re-upload all files")
	}

	// map of stage path to file data (JSON string) in stage
	filesInStage := make(map[string]string)
	for _, row := range rows {
		// Normalize Snowflake filename to match our stagePath format
		// Snowflake returns: path/to/file.json/file.json.gz
		// We need: path/to/file.json
		if row.Filename != "" {
			normalizedPath := normalizeSnowflakeFilename(row.Filename)
			filesInStage[normalizedPath] = row.Data
		}
	}

	logger.Info("files currently in the snowflake internal stage", "count", len(filesInStage))
	logger.Info("list of files in the local file store to be stored", "files", filePaths)

	errorList := []error{}
	for _, filePathInFilestore := range filePaths {
		// read the file from filestore
		fileBytesInFilestore, err := fs.Retrieve(ctx, filePathInFilestore)
		if err != nil {
			logger.Error(err, "failed to retrieve file from filestore", "file", filePathInFilestore)
			errorList = append(errorList, err)
			continue
		}

		// Calculate stage path for this file
		stagePath := d.stagePathForFile(filePathInFilestore)

		// Check if file already exists in the stage
		if existingFileData, exists := filesInStage[stagePath]; exists {
			logger.Info("file already exists in the stage", "file", stagePath)

			// Mark as processed (will be removed from deletion list)
			delete(filesInStage, stagePath)

			// Compare metadata based on file type to see if upload is needed
			if filesAreEqual(filePathInFilestore, existingFileData, fileBytesInFilestore) {
				logger.Info("file metadata unchanged, skipping upload", "file", stagePath)
				continue
			}
		}

		// Upload the file to the stage
		streamCtx := gosnowflake.WithFileStream(ctx, bytes.NewReader(fileBytesInFilestore))
		fileRows := []snowflake.UploadedFileStatus{}

		if err := d.Client.Put(streamCtx,
			d.Role,
			filePathInFilestore,
			d.Database,
			d.Schema,
			d.Stage,
			stagePath,
			&fileRows); err != nil {
			logger.Error(err, "failed to upload file to snowflake internal stage", "file", stagePath)
			errorList = append(errorList, err)
			continue
		}
		logger.Info("successfully uploaded file to snowflake internal stage", "file", stagePath)

		if len(fileRows) == 0 {
			logger.Error(fmt.Errorf("no file rows returned while uploading file to snowflake internal stage: %s", stagePath),
				"file", stagePath)
			errorList = append(errorList,
				fmt.Errorf("no file rows returned while uploading file to snowflake internal stage: %s", stagePath))
			continue
		}
	}

	// Delete extra files that are in the stage but not in filestore
	extraFiles := make([]string, 0, len(filesInStage))
	for stagePath := range filesInStage {
		logger.Info("found extra file in the stage, marking for deletion", "file", stagePath)
		extraFiles = append(extraFiles, stagePath)
	}

	if err := d.Client.DeleteFilesFromStage(ctx, d.Role, d.Database, d.Schema, d.Stage, extraFiles); err != nil {
		logger.Error(err, "failed to delete extra files from snowflake internal stage")
		errorList = append(errorList, err)
	}

	if len(errorList) > 0 {
		return fmt.Errorf("encountered errors while syncing files to snowflake internal stage: %v", errorList)
	}

	return nil
}

// normalizeSnowflakeFilename converts Snowflake's internal filename format to our expected format.
// Snowflake stores files as: path/to/file.json/file.json.gz
// We need: path/to/file.json
func normalizeSnowflakeFilename(snowflakeFilename string) string {
	// Find last slash
	lastSlashIdx := strings.LastIndex(snowflakeFilename, "/")
	if lastSlashIdx == -1 {
		// No slash, return as-is
		return snowflakeFilename
	}

	// Get the part before the last slash
	pathWithoutLast := snowflakeFilename[:lastSlashIdx]

	// Get the last part (should be basename.gz)
	lastPart := snowflakeFilename[lastSlashIdx+1:]

	// Remove .gz suffix if present
	expectedBasename := strings.TrimSuffix(lastPart, ".gz")

	// Verify that pathWithoutLast ends with expectedBasename
	if strings.HasSuffix(pathWithoutLast, expectedBasename) {
		return pathWithoutLast
	}

	// If it doesn't match expected pattern, return original
	return snowflakeFilename
}

// getArtifactPathForFile determines the artifact path for a file based on its suffix
// Returns the configured path from artifactPathMap or a default path
func getArtifactPathForFile(filePathInFilestore string, artifactPathMap map[string]string) string {
	var suffix, defaultPath string

	if strings.HasSuffix(filePathInFilestore, ConvertedFileSuffix) {
		suffix = ConvertedFileSuffix
		defaultPath = DefaultProcessedDocumentsPath
	} else if strings.HasSuffix(filePathInFilestore, ChunksFileSuffix) {
		suffix = ChunksFileSuffix
		defaultPath = DefaultChunksPath
	} else if strings.HasSuffix(filePathInFilestore, VectorEmbeddingsFileSuffix) {
		suffix = VectorEmbeddingsFileSuffix
		defaultPath = DefaultVectorEmbeddingsPath
	} else {
		return ""
	}

	if path, ok := artifactPathMap[suffix]; ok && path != "" {
		return path
	}
	return defaultPath
}

// filesAreEqual compares files based on their type and returns true if metadata is unchanged
func filesAreEqual(filePath string, existingData string, newData []byte) bool {
	if strings.HasSuffix(filePath, VectorEmbeddingsFileSuffix) {
		var existing, incoming EmbeddingsFile
		if json.Unmarshal([]byte(existingData), &existing) == nil &&
			json.Unmarshal(newData, &incoming) == nil &&
			existing.EmbeddingDocument != nil && incoming.EmbeddingDocument != nil &&
			existing.EmbeddingDocument.Metadata != nil && incoming.EmbeddingDocument.Metadata != nil {
			return existing.EmbeddingDocument.Metadata.Equal(incoming.EmbeddingDocument.Metadata)
		}
	} else if strings.HasSuffix(filePath, ChunksFileSuffix) {
		var existing, incoming ChunksFile
		if json.Unmarshal([]byte(existingData), &existing) == nil &&
			json.Unmarshal(newData, &incoming) == nil &&
			existing.ChunksDocument != nil && incoming.ChunksDocument != nil &&
			existing.ChunksDocument.Metadata != nil && incoming.ChunksDocument.Metadata != nil {
			return existing.ChunksDocument.Metadata.Equal(incoming.ChunksDocument.Metadata)
		}
	} else if strings.HasSuffix(filePath, ConvertedFileSuffix) {
		var existing, incoming ConvertedFile
		if json.Unmarshal([]byte(existingData), &existing) == nil &&
			json.Unmarshal(newData, &incoming) == nil &&
			existing.ConvertedDocument != nil && incoming.ConvertedDocument != nil &&
			existing.ConvertedDocument.Metadata != nil && incoming.ConvertedDocument.Metadata != nil {
			return existing.ConvertedDocument.Metadata.Equal(incoming.ConvertedDocument.Metadata)
		}
	}
	return false
}

// stagePathForFile builds the stage path with stages subdirectory for Snowflake
// Format: {dataProductPrefix}/stages/{artifact-path}/{filename}
// Example: testproduct/stages/chunks/test.pdf-chunks.json
func (d *SnowflakeInternalStage) stagePathForFile(filePathInFilestore string) string {
	baseName := filepath.Base(filePathInFilestore)

	// Extract prefix from filePathInFilestore (everything before the last /)
	// filePathInFilestore example: "testunstructureddataproduct/12.pdf-converted.json"
	// We want to preserve: "testunstructureddataproduct"
	var dataProductPrefix string
	if idx := strings.LastIndex(filePathInFilestore, "/"); idx != -1 {
		dataProductPrefix = filePathInFilestore[:idx]
	}

	// Determine artifact path based on file suffix
	artifactPath := getArtifactPathForFile(filePathInFilestore, d.ArtifactPathMap)

	// Build path: {dataProductPrefix}/stages/{artifact-path}/{filename}
	var stagePath string
	if dataProductPrefix != "" && artifactPath != "" {
		stagePath = filepath.Join(dataProductPrefix, "stages", artifactPath, baseName)
	} else if artifactPath != "" {
		stagePath = filepath.Join("stages", artifactPath, baseName)
	} else {
		stagePath = baseName
	}

	if filepath.Separator != '/' {
		stagePath = filepath.ToSlash(stagePath)
	}
	return stagePath
}

// S3Destination syncs processed JSON artifacts to an S3 bucket (converted, chunks,
// and vector embeddings — whichever paths the controller passes, based on artifacts configuration).
type S3Destination struct {
	S3Client        *s3.Client
	Bucket          string
	Prefix          string
	DataProductName string            // used as default prefix when Prefix is empty (CR name)
	ArtifactPathMap map[string]string // map[fileSuffix]artifactPath for stages structure
}

func (d *S3Destination) getPrefix() string {
	if d.Prefix != "" {
		return d.Prefix
	}
	return d.DataProductName
}

// s3KeyForFile maps a filestore object key to an S3 object key.
// Format: {prefix}/stages/{artifact-path}/{filename}
func (d *S3Destination) s3KeyForFile(filePathInFilestore string) string {
	prefix := d.getPrefix()
	baseName := filepath.Base(filePathInFilestore)

	// Determine artifact path based on file suffix
	artifactPath := getArtifactPathForFile(filePathInFilestore, d.ArtifactPathMap)

	// Build path: prefix/stages/{artifact-path}/{filename}
	var key string
	if prefix != "" {
		key = filepath.Join(prefix, "stages", artifactPath, baseName)
	} else {
		key = filepath.Join("stages", artifactPath, baseName)
	}

	if filepath.Separator != '/' {
		key = filepath.ToSlash(key)
	}
	return key
}

func (d *S3Destination) SyncFilesToDestination(ctx context.Context, fs *filestore.FileStore,
	filePaths []string) error {
	logger := log.FromContext(ctx)
	logger.Info("syncing data to S3 destination",
		"bucket", d.Bucket, "prefix", d.getPrefix(), "filePaths", filePaths)

	s3Client := d.S3Client
	if s3Client == nil {
		var err error
		s3Client, err = awsclienthandler.GetDestinationS3Client()
		if err != nil {
			return fmt.Errorf("failed to get S3 client: %w", err)
		}
	}

	// Keys currently in the destination (same idea as Snowflake: one map, trim as we sync, delete the rest).
	keysInDestination := make(map[string]bool)
	paginator := s3.NewListObjectsV2Paginator(s3Client, &s3.ListObjectsV2Input{
		Bucket: aws.String(d.Bucket),
		Prefix: aws.String(d.getPrefix()),
	})
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return fmt.Errorf("list objects s3://%s prefix %q: %w", d.Bucket, d.getPrefix(), err)
		}
		for _, obj := range page.Contents {
			if obj.Key != nil {
				keysInDestination[*obj.Key] = true
			}
		}
	}

	for _, filePath := range filePaths {
		data, err := fs.Retrieve(ctx, filePath)
		if err != nil {
			logger.Error(err, "failed to retrieve file from filestore", "file", filePath)
			return fmt.Errorf("retrieve %s: %w", filePath, err)
		}

		key := d.s3KeyForFile(filePath)
		delete(keysInDestination, key)

		// Check if file exists and compare ETag (MD5) to skip unchanged files
		headResp, err := s3Client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(d.Bucket),
			Key:    aws.String(key),
		})
		if err == nil && headResp.ETag != nil {
			// S3 ETag is MD5 hash wrapped in quotes for single-part uploads
			localMD5 := fmt.Sprintf("\"%x\"", md5.Sum(data))
			if localMD5 == *headResp.ETag {
				logger.Info("file unchanged, skipping upload",
					"file", filePath, "key", key)
				continue
			}
		}

		// File is new or changed, upload it
		_, err = s3Client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(d.Bucket),
			Key:    aws.String(key),
			Body:   bytes.NewReader(data),
		})
		if err != nil {
			logger.Error(err, "failed to upload file to S3", "bucket", d.Bucket, "key", key)
			return fmt.Errorf("put object s3://%s/%s: %w", d.Bucket, key, err)
		}
		logger.Info("uploaded file to S3 destination", "key", key)
	}

	// Remaining keys = files in destination but no longer in ingestion; delete
	for key := range keysInDestination {
		logger.Info("found file in destination no longer in ingestion, deleting", "key", key)
		_, err := s3Client.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: aws.String(d.Bucket),
			Key:    aws.String(key),
		})
		if err != nil {
			logger.Error(err, "failed to delete file from S3 destination", "key", key)
			return fmt.Errorf("delete object s3://%s/%s: %w", d.Bucket, key, err)
		}
	}

	return nil
}
