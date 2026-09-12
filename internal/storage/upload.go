package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// Upload consumes and closes body. The producer must return EOF only after all
// processing succeeds; a read error aborts the upload without publishing a key.
func (s *S3) Upload(ctx context.Context, key string, body io.ReadCloser) error {
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
	client := &streamUploadClient{Client: s.client, cancel: cancel}
	_, err := s.transfer.UploadObject(ctx, &transfermanager.UploadObjectInput{
		Bucket: aws.String(s.bucket), Key: aws.String(key), Body: body,
	}, func(o *transfermanager.Options) {
		o.S3 = client
		o.PartSizeBytes = s.uploadPartSize
		o.MultipartUploadThreshold = s.uploadPartSize
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

type streamUploadClient struct {
	*s3.Client
	cancel context.CancelCauseFunc
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
