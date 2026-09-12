package storage

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
)

// Keep the SDK's proxy, custom CA and connection settings, but bound inactivity
// without imposing a deadline on the total transfer. NewFromConfig resolves the
// SDK's BuildableClient before invoking our option; Config exposes no custom client.
func withIdleTimeout(client aws.HTTPClient, timeout time.Duration) aws.HTTPClient {
	return client.(*awshttp.BuildableClient).WithReadTimeout(0).WithTransportOptions(func(tr *http.Transport) {
		// One request per connection ensures traffic from another HTTP/2 stream
		// cannot keep a stalled request alive. This matches the previous AWS CLI.
		tr.ForceAttemptHTTP2 = false
		tr.TLSNextProto = map[string]func(string, *tls.Conn) http.RoundTripper{}
		tr.TLSClientConfig.NextProtos = []string{"http/1.1"}
		dial := tr.DialContext
		tr.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
			conn, err := dial(ctx, network, address)
			if err != nil {
				return nil, err
			}
			return &idleConn{Conn: conn, timeout: timeout}, nil
		}
	}).Freeze()
}

type idleConn struct {
	net.Conn
	timeout time.Duration
}

func (c *idleConn) Read(p []byte) (int, error) {
	if err := c.SetDeadline(time.Now().Add(c.timeout)); err != nil {
		return 0, err
	}
	n, err := c.Conn.Read(p)
	if n > 0 {
		c.SetDeadline(time.Now().Add(c.timeout))
	}
	return n, err
}

func (c *idleConn) Write(p []byte) (int, error) {
	// HTTP starts reading the response while it is still sending the request.
	// Refresh both deadlines so a progressing upload does not time out its read.
	if err := c.SetDeadline(time.Now().Add(c.timeout)); err != nil {
		return 0, err
	}
	n, err := c.Conn.Write(p)
	if n > 0 {
		c.SetDeadline(time.Now().Add(c.timeout))
	}
	return n, err
}
