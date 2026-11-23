// Package storage implements the S3 operations needed by backup and restore.
package storage

import (
	"context"
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsmiddleware "github.com/aws/aws-sdk-go-v2/aws/middleware"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager"
	transfertypes "github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

type Object struct {
	Key          string
	LastModified time.Time
}

type Listing struct {
	Objects []Object
	// ServerTime is the first page's HTTP Date, never the local wall clock.
	// A missing/invalid Date leaves it zero: restore can list, retention must stop.
	ServerTime time.Time
}

type Store interface {
	VersioningEnabled(context.Context) (bool, error)
	Upload(ctx context.Context, key string, body io.ReadCloser, options UploadOptions) error
	Modified(ctx context.Context, key string) (time.Time, bool, error)
	DownloadTo(ctx context.Context, key, versionID string, file *os.File) error
	ListPages(ctx context.Context, prefix string, visit func(Listing) error) error
	DeleteBatch(ctx context.Context, keys []string, beforeDelete func() error) error
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

// New uses a complete explicit S3 credential set or the SDK credential chain.
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
		cfg.UploadPartSizeBytes = DefaultUploadPartSizeBytes
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
	accessKey, secretKey, token := cfg.AccessKeyID, cfg.SecretAccessKey, cfg.SessionToken
	if accessKey == "" && secretKey == "" && token == "" {
		accessKey, secretKey, token = os.Getenv("S3_ACCESS_KEY_ID"), os.Getenv("S3_SECRET_ACCESS_KEY"), os.Getenv("S3_SESSION_TOKEN")
	}
	if accessKey != "" || secretKey != "" || token != "" {
		if accessKey == "" || secretKey == "" {
			return nil, fmt.Errorf("S3 credential overrides require both an access key ID and secret access key (S3_ACCESS_KEY_ID and S3_SECRET_ACCESS_KEY)")
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

// Modified distinguishes a missing fixed backup from permission and transport errors.
func (s *S3) Modified(ctx context.Context, key string) (time.Time, bool, error) {
	out, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(key)})
	if err != nil {
		var res *smithyhttp.ResponseError
		if errors.As(err, &res) && res.HTTPStatusCode() == http.StatusNotFound {
			return time.Time{}, false, nil
		}
		return time.Time{}, false, fmt.Errorf("inspect fixed backup: %w", err)
	}
	if out.LastModified == nil || out.LastModified.IsZero() {
		return time.Time{}, false, fmt.Errorf("fixed backup has no LastModified")
	}
	return *out.LastModified, true, nil
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
	if err := s.DownloadTo(ctx, key, versionID, f); err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("close download file: %w", err)
	}
	if err := os.Rename(f.Name(), path); err != nil {
		return fmt.Errorf("finish download file: %w", err)
	}
	return nil
}

// DownloadTo writes to a caller-owned scratch file. Consume it only on success.
func (s *S3) DownloadTo(ctx context.Context, key, versionID string, f *os.File) error {
	return s.downloadTo(ctx, key, versionID, f, true)
}

func (s *S3) downloadTo(ctx context.Context, key, versionID string, f *os.File, resolveReference bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := f.Truncate(0); err != nil {
		return err
	}
	input := &transfermanager.DownloadObjectInput{
		Bucket: aws.String(s.bucket), Key: aws.String(key), WriterAt: f,
	}
	if versionID != "" {
		input.VersionID = aws.String(versionID)
	}
	if versionID == "null" {
		head, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: input.Bucket, Key: input.Key, VersionId: input.VersionID})
		if err != nil {
			return fmt.Errorf("inspect mutable null version: %w", err)
		}
		if aws.ToString(head.ETag) == "" {
			return fmt.Errorf("mutable null version has no ETag")
		}
		input.IfMatch = head.ETag
	}
	if _, err := s.transfer.DownloadObject(ctx, input); err != nil {
		return fmt.Errorf("download S3 object %q: %w", key, err)
	}
	if resolveReference {
		var ref [len(referencePrefix) + 32]byte
		n, err := f.ReadAt(ref[:], 0)
		if err != nil && err != io.EOF {
			return err
		}
		if strings.HasPrefix(string(ref[:n]), referencePrefix) {
			info, err := f.Stat()
			if err != nil {
				return err
			}
			id := string(ref[len(referencePrefix):n])
			if _, err := hex.DecodeString(id); err != nil || info.Size() != int64(len(ref)) || strings.ToLower(id) != id {
				return fmt.Errorf("invalid fixed backup reference at %q", key)
			}
			return s.downloadTo(ctx, key+".generations/"+id, "", f, false)
		}
	}
	return ctx.Err()
}

func (s *S3) ListPages(ctx context.Context, prefix string, visit func(Listing) error) error {
	paginator := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(s.bucket), Prefix: aws.String(prefix), EncodingType: types.EncodingTypeUrl,
	})
	var listing Listing
	firstPage := true
	tokens := make(map[string]bool)
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return fmt.Errorf("list S3 objects: %w", err)
		}
		if firstPage {
			if response, ok := awsmiddleware.GetRawResponse(page.ResultMetadata).(*smithyhttp.Response); ok && response != nil && response.Response != nil {
				listing.ServerTime, _ = http.ParseTime(response.Header.Get("Date"))
			}
			firstPage = false
		}
		listing.Objects = nil
		for _, object := range page.Contents {
			// Missing dates must not turn an unknown age into an expired backup.
			if object.Key == nil || object.LastModified == nil {
				return fmt.Errorf("S3 list response contains an object without a key or modification time")
			}
			key := *object.Key
			if page.EncodingType == types.EncodingTypeUrl {
				key, err = url.PathUnescape(key)
				if err != nil {
					return fmt.Errorf("invalid URL-encoded S3 key: %w", err)
				}
			}
			listing.Objects = append(listing.Objects, Object{Key: key, LastModified: *object.LastModified})
		}
		if !aws.ToBool(page.IsTruncated) && page.NextContinuationToken != nil {
			return fmt.Errorf("S3 list response supplies a continuation token without IsTruncated=true")
		}
		if aws.ToBool(page.IsTruncated) {
			token := aws.ToString(page.NextContinuationToken)
			if token == "" || tokens[token] {
				return fmt.Errorf("S3 list response contains an invalid continuation token")
			}
			tokens[token] = true
		}
		if err := visit(listing); err != nil {
			return err
		}
	}
	return nil
}

func (s *S3) DeleteBatch(ctx context.Context, keys []string, guard func() error) error {
	if len(keys) == 1 {
		return s.Delete(ctx, keys[0], guard)
	}
	objects := make([]types.ObjectIdentifier, 0, len(keys))
	for _, key := range keys {
		// XML 1.0 cannot represent every S3 key. Keep these keys in the URL
		// of a single-object request so serialization cannot change the key.
		if strings.ContainsFunc(key, func(r rune) bool {
			return (r < 0x20 && r != '\t' && r != '\n' && r != '\r') || r == 0xfffe || r == 0xffff
		}) {
			if err := s.Delete(ctx, key, guard); err != nil {
				return err
			}
			continue
		}
		objects = append(objects, types.ObjectIdentifier{Key: aws.String(key)})
	}
	if len(objects) == 0 {
		return nil
	}
	out, err := s.client.DeleteObjects(ctx, &s3.DeleteObjectsInput{Bucket: aws.String(s.bucket),
		Delete: &types.Delete{Objects: objects, Quiet: aws.Bool(true)}}, withAttemptGuard(guard), func(o *s3.Options) { o.APIOptions = append(o.APIOptions, deleteContentMD5) })
	if err != nil {
		return fmt.Errorf("delete expired backups: %w", err)
	}
	if len(out.Errors) > 0 {
		return fmt.Errorf("failed to delete %d expired backups; first key %q: %s", len(out.Errors), aws.ToString(out.Errors[0].Key), aws.ToString(out.Errors[0].Code))
	}
	return nil
}

// Hash the SDK-serialized XML, including its namespace and escaped key names.
func deleteContentMD5(stack *middleware.Stack) error {
	return stack.Build.Add(middleware.BuildMiddlewareFunc("DeleteContentMD5", func(ctx context.Context, in middleware.BuildInput, next middleware.BuildHandler) (middleware.BuildOutput, middleware.Metadata, error) {
		req := in.Request.(*smithyhttp.Request)
		digest := md5.New()
		if _, err := io.Copy(digest, req.GetStream()); err != nil {
			return middleware.BuildOutput{}, middleware.Metadata{}, err
		}
		if err := req.RewindStream(); err != nil {
			return middleware.BuildOutput{}, middleware.Metadata{}, err
		}
		req.Header.Set("Content-MD5", base64.StdEncoding.EncodeToString(digest.Sum(nil)))
		return next.HandleBuild(ctx, in)
	}), middleware.After)
}

func (s *S3) Delete(ctx context.Context, key string, beforeDelete func() error) error {
	// Deliberately delete the current key only. Noncurrent versions remain
	// governed by the bucket lifecycle, matching the previous aws s3 rm command.
	_, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(key)}, withAttemptGuard(beforeDelete))
	if err != nil {
		return fmt.Errorf("delete S3 object %q: %w", key, err)
	}
	return nil
}
