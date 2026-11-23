package storage

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
)

const (
	DefaultUploadPartSizeBytes int64 = 8 * 1024 * 1024
	MaxUploadParts             int64 = 10000
)

var errEmptyBackup = errors.New("backup stream is empty")

// Upload consumes and closes body. The producer must return EOF only after all
// processing succeeds; a read error aborts the upload without publishing a key.
func (s *S3) Upload(ctx context.Context, key string, body io.ReadCloser, options UploadOptions) error {
	if !options.FixedReference {
		return s.upload(ctx, key, body, MaxUploadParts, options)
	}
	var generation [16]byte
	if _, err := rand.Read(generation[:]); err != nil {
		body.Close()
		return err
	}
	id := hex.EncodeToString(generation[:])
	dataKey := key + ".generations/" + id
	dataOptions := UploadOptions{IfAbsent: true, BeforePublish: options.BeforePublish}
	if err := s.upload(ctx, dataKey, body, MaxUploadParts, dataOptions); err != nil {
		return err
	}
	ref := io.NopCloser(strings.NewReader(referencePrefix + id))
	if err := s.upload(ctx, key, ref, MaxUploadParts, options); err != nil {
		return fmt.Errorf("publish fixed reference %q to saved generation %q: %w", key, dataKey, err)
	}
	return nil
}

const referencePrefix = "postgres-backup-s3 reference v1\n"

// UploadOptions controls the final publication of a streamed object.
type UploadOptions struct {
	FixedReference bool
	IfAbsent       bool
	BeforePublish  func() error
}

func (s *S3) upload(ctx context.Context, key string, body io.ReadCloser, maxParts int64, options UploadOptions) error {
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	var closeOnce sync.Once
	closeBody := func() { closeOnce.Do(func() { body.Close() }) }
	defer closeBody()
	if err := ctx.Err(); err != nil {
		return err
	}
	// SDK reads from unknown-length streams can block independently of HTTP.
	stop := context.AfterFunc(ctx, closeBody)
	defer stop()
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return err
	}
	client := &streamUploadClient{publicationID: hex.EncodeToString(id[:]), Client: s.client, cancel: cancel, beforePublish: options.BeforePublish}
	maxBytes := s.uploadPartSize * maxParts
	remedy := "increase S3_UPLOAD_PART_SIZE_MB (MiB, maximum 5120) and retry the backup"
	if s.uploadPartSize == 5*1024*1024*1024 {
		remedy = "S3_UPLOAD_PART_SIZE_MB is already at its maximum (5120 MiB); reduce the size of the backup"
	}
	limited := &uploadLimitReader{
		reader: body, remaining: maxBytes,
		limitErr: fmt.Errorf("backup exceeds the multipart upload limit of %d bytes (%.3f GiB): %d parts at %d MiB; %s",
			maxBytes, float64(maxBytes)/(1024*1024*1024), maxParts, s.uploadPartSize/(1024*1024), remedy),
	}
	input := &transfermanager.UploadObjectInput{
		Bucket: aws.String(s.bucket), Key: aws.String(key), Body: limited, Metadata: map[string]string{"publication-id": client.publicationID},
	}
	if options.IfAbsent {
		input.IfNoneMatch = aws.String("*")
	}
	_, err := s.transfer.UploadObject(ctx, input, func(o *transfermanager.Options) {
		o.S3 = client
		o.PartSizeBytes = s.uploadPartSize
		o.MultipartUploadThreshold = s.uploadPartSize
		o.MaxUploadParts = maxParts
		o.Concurrency = 2
	})
	if cause := context.Cause(ctx); cause != nil {
		err = errors.Join(cause, err)
	}
	if err != nil {
		return fmt.Errorf("upload S3 object %q: %w", key, err)
	}
	return nil
}

// Unlike io.LimitReader, this reader does not manufacture EOF at the limit.
// Probe one more byte so an oversized stream cannot publish a truncated backup,
// and preserve producer errors reported after the last permitted byte.
type uploadLimitReader struct {
	reader    io.Reader
	remaining int64
	limitErr  error
	hasData   bool
}

func (r *uploadLimitReader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if r.remaining == 0 {
		n, err := r.reader.Read(p[:1])
		if n > 0 {
			return 0, errors.Join(r.limitErr, err)
		}
		return 0, err
	}
	if int64(len(p)) > r.remaining {
		p = p[:r.remaining]
	}
	n, err := r.reader.Read(p)
	if n > 0 {
		r.hasData = true
		r.remaining -= int64(n)
	} else if err == io.EOF && !r.hasData {
		return 0, errEmptyBackup
	}
	return n, err
}

type streamUploadClient struct {
	*s3.Client
	publicationID string
	cancel        context.CancelCauseFunc
	beforePublish func() error
}

func (c *streamUploadClient) PutObject(ctx context.Context, in *s3.PutObjectInput, opts ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	if in.IfNoneMatch == nil {
		// Replaying a fixed-key PUT can create a second version.
		opts = append(opts, func(o *s3.Options) { o.RetryMaxAttempts = 1 })
	}
	attempts := 0
	out, err := c.Client.PutObject(ctx, in, append(opts, c.countAttempts(&attempts))...)
	if err != nil {
		err = c.publicationError(ctx, in.Bucket, in.Key, attempts, err)
		if err == nil {
			return &s3.PutObjectOutput{}, nil
		}
	}
	return out, err
}

func (c *streamUploadClient) CompleteMultipartUpload(ctx context.Context, in *s3.CompleteMultipartUploadInput, opts ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error) {
	attempts := 0
	out, err := c.Client.CompleteMultipartUpload(ctx, in, append(opts, c.countAttempts(&attempts))...)
	if err != nil {
		err = c.publicationError(ctx, in.Bucket, in.Key, attempts, err)
		if err == nil {
			return &s3.CompleteMultipartUploadOutput{}, nil
		}
	}
	return out, err
}

func (c *streamUploadClient) countAttempts(attempts *int) func(*s3.Options) {
	return withAttemptGuard(func() error {
		if c.beforePublish != nil {
			if err := c.beforePublish(); err != nil {
				return err
			}
		}
		(*attempts)++
		return nil
	})
}

func (c *streamUploadClient) publicationError(ctx context.Context, bucket, key *string, attempts int, err error) error {
	var guardErr *attemptGuardError
	if attempts == 0 || ctx.Err() != nil || errors.As(err, &guardErr) {
		return err
	}
	var apiErr smithy.APIError
	var status interface{ HTTPStatusCode() int }
	if attempts == 1 && errors.As(err, &apiErr) && errors.As(err, &status) &&
		status.HTTPStatusCode() >= http.StatusBadRequest && status.HTTPStatusCode() < 500 && status.HTTPStatusCode() != http.StatusRequestTimeout {
		return err
	}
	head, headErr := c.HeadObject(ctx, &s3.HeadObjectInput{Bucket: bucket, Key: key})
	if headErr != nil {
		return errors.Join(err, fmt.Errorf("could not verify publication using HeadObject: %w", headErr))
	}
	if head.Metadata["publication-id"] == c.publicationID {
		return nil
	}
	return err
}

func (c *streamUploadClient) UploadPart(ctx context.Context, in *s3.UploadPartInput, opts ...func(*s3.Options)) (*s3.UploadPartOutput, error) {
	out, err := c.Client.UploadPart(ctx, in, opts...)
	if err != nil {
		// After SDK retries fail, interrupt a producer stalled between parts.
		// Otherwise the transfer manager can remain blocked reading the next part.
		c.cancel(err)
	}
	return out, err
}
