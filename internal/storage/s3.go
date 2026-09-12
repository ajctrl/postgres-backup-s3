// Package storage implements the S3 operations needed by backup and restore.
package storage

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager"
	transfertypes "github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type Object struct {
	Key          string
	LastModified time.Time
}

type Store interface {
	VersioningEnabled(context.Context) (bool, error)
	Upload(ctx context.Context, key string, body io.ReadCloser) error
	Download(ctx context.Context, key, versionID, path string) error
	List(ctx context.Context, prefix string) ([]Object, error)
	Delete(ctx context.Context, key string) error
}

type Config struct {
	Bucket              string
	Region              string
	Endpoint            string
	AccessKeyID         string
	SecretAccessKey     string
	SessionToken        string
	UploadPartSizeBytes int64
}

type S3 struct {
	bucket         string
	client         *s3.Client
	transfer       *transfermanager.Client
	uploadPartSize int64
}

var _ Store = (*S3)(nil)

// New preserves the SDK credential chain (environment, profiles, web identity,
// container and instance roles). Explicit credentials and the legacy S3 aliases
// override their respective AWS environment values without mutating the process.
func New(ctx context.Context, cfg Config) (*S3, error) {
	return newS3(ctx, cfg, 60*time.Second)
}

func newS3(ctx context.Context, cfg Config, idleTimeout time.Duration) (*S3, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if cfg.Bucket == "" {
		return nil, fmt.Errorf("S3 bucket must not be empty")
	}
	if cfg.UploadPartSizeBytes == 0 {
		cfg.UploadPartSizeBytes = 8 * 1024 * 1024
	}
	if cfg.UploadPartSizeBytes < 5*1024*1024 || cfg.UploadPartSizeBytes > 5*1024*1024*1024 {
		return nil, fmt.Errorf("S3 upload part size must be between 5 MiB and 5 GiB")
	}
	if cfg.Endpoint != "" {
		u, err := url.Parse(cfg.Endpoint)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
			return nil, fmt.Errorf("S3_ENDPOINT must be an HTTP or HTTPS URL without credentials, query, or fragment")
		}
	}
	options := []func(*config.LoadOptions) error{}
	if cfg.Region != "" {
		options = append(options, config.WithRegion(cfg.Region))
	}
	accessKey := firstNonempty(cfg.AccessKeyID, os.Getenv("S3_ACCESS_KEY_ID"))
	secretKey := firstNonempty(cfg.SecretAccessKey, os.Getenv("S3_SECRET_ACCESS_KEY"))
	token := firstNonempty(cfg.SessionToken, os.Getenv("S3_SESSION_TOKEN"))
	if accessKey != "" || secretKey != "" || token != "" {
		accessKey = firstNonempty(accessKey, os.Getenv("AWS_ACCESS_KEY_ID"), os.Getenv("AWS_ACCESS_KEY"))
		secretKey = firstNonempty(secretKey, os.Getenv("AWS_SECRET_ACCESS_KEY"), os.Getenv("AWS_SECRET_KEY"))
		token = firstNonempty(token, os.Getenv("AWS_SESSION_TOKEN"))
		if accessKey == "" || secretKey == "" {
			return nil, fmt.Errorf("S3 credential overrides require both an access key ID and secret access key (S3_* or AWS_* variables)")
		}
		options = append(options, config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(accessKey, secretKey, token)))
	}
	awsConfig, err := config.LoadDefaultConfig(ctx, options...)
	if err != nil {
		return nil, fmt.Errorf("load AWS configuration: %w", err)
	}
	if awsConfig.Region == "" {
		awsConfig.Region = "us-west-1"
	}
	client := s3.NewFromConfig(awsConfig, func(o *s3.Options) {
		o.HTTPClient = withIdleTimeout(o.HTTPClient, idleTimeout)
		if cfg.Endpoint != "" {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
			// S3-compatible endpoints often lack wildcard DNS/certificates.
			o.UsePathStyle = true
		}
	})
	transfer := transfermanager.New(client, func(o *transfermanager.Options) {
		// Range downloads work with AWS and compatible stores, including objects
		// originally uploaded without multipart metadata.
		o.GetObjectType = transfertypes.GetObjectRanges
		// Failed/canceled uploads must still be able to abort uploaded parts.
		o.FailTimeout = 10 * time.Second
		// Respect AWS_REQUEST_CHECKSUM_CALCULATION and profile configuration.
		o.RequestChecksumCalculation = awsConfig.RequestChecksumCalculation
	})
	return &S3{bucket: cfg.Bucket, client: client, transfer: transfer, uploadPartSize: cfg.UploadPartSizeBytes}, nil
}

func (s *S3) VersioningEnabled(ctx context.Context) (bool, error) {
	out, err := s.client.GetBucketVersioning(ctx, &s3.GetBucketVersioningInput{Bucket: aws.String(s.bucket)})
	if err != nil {
		return false, fmt.Errorf("get S3 bucket versioning: %w", err)
	}
	return out.Status == types.BucketVersioningStatusEnabled, nil
}

func (s *S3) Download(ctx context.Context, key, versionID, path string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	f, err := os.CreateTemp(filepath.Dir(path), ".s3-download-*")
	if err != nil {
		return fmt.Errorf("create download file: %w", err)
	}
	defer os.Remove(f.Name())
	defer f.Close()
	input := &transfermanager.DownloadObjectInput{
		Bucket: aws.String(s.bucket), Key: aws.String(key), WriterAt: f,
	}
	if versionID != "" {
		input.VersionID = aws.String(versionID)
	}
	if _, err := s.transfer.DownloadObject(ctx, input); err != nil {
		return fmt.Errorf("download S3 object %q: %w", key, err)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("close download file: %w", err)
	}
	// Never expose an incomplete download to pg_restore, including on retries.
	if err := os.Rename(f.Name(), path); err != nil {
		return fmt.Errorf("finish download file: %w", err)
	}
	return nil
}

func (s *S3) List(ctx context.Context, prefix string) ([]Object, error) {
	paginator := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(s.bucket), Prefix: aws.String(prefix),
	})
	var objects []Object
	tokens := make(map[string]bool)
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("list S3 objects: %w", err)
		}
		for _, object := range page.Contents {
			// Missing dates must not turn an unknown age into an expired backup.
			if object.Key == nil || object.LastModified == nil {
				return nil, fmt.Errorf("S3 list response contains an object without a key or modification time")
			}
			objects = append(objects, Object{Key: *object.Key, LastModified: *object.LastModified})
		}
		if aws.ToBool(page.IsTruncated) {
			token := aws.ToString(page.NextContinuationToken)
			if token == "" || tokens[token] {
				return nil, fmt.Errorf("S3 list response contains an invalid continuation token")
			}
			tokens[token] = true
		}
	}
	return objects, nil
}

func (s *S3) Delete(ctx context.Context, key string) error {
	// Deliberately delete the current key only. Noncurrent versions remain
	// governed by the bucket lifecycle, matching the previous aws s3 rm command.
	_, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(key)})
	if err != nil {
		return fmt.Errorf("delete S3 object %q: %w", key, err)
	}
	return nil
}

func firstNonempty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}
