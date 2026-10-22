package storage

import (
	"net/http"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// Operation-scoped HTTP clients run inside the SDK retry loop, after signing
// and backoff, immediately before each attempt reaches the network. Only
// publication/deletion operations use this guard; abort requests must remain
// possible after a clock rollback.
func withAttemptGuard(check func() error) func(*s3.Options) {
	return func(o *s3.Options) {
		if check != nil {
			o.HTTPClient = &guardedHTTPClient{HTTPClient: o.HTTPClient, check: check}
		}
	}
}

type guardedHTTPClient struct {
	aws.HTTPClient
	check func() error
}

func (c *guardedHTTPClient) Do(request *http.Request) (*http.Response, error) {
	if err := c.check(); err != nil {
		if request.Body != nil {
			request.Body.Close()
		}
		return nil, &attemptGuardError{err: err}
	}
	return c.HTTPClient.Do(request)
}

// Prevent the SDK from treating a rejected attempt as a transient send error.
// Unwrap preserves the clock rollback sentinel for the application.
type attemptGuardError struct{ err error }

func (e *attemptGuardError) Error() string        { return e.err.Error() }
func (e *attemptGuardError) Unwrap() error        { return e.err }
func (e *attemptGuardError) RetryableError() bool { return false }
