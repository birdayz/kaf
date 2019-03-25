package schemaregistry

import (
	"crypto/tls"
	"errors"
	"net/http"
	"net/url"
)

// DefaultUrl is the address where a local schema registry listens by default.
//
// DEPRECATED: Use `schemaregistry.DefaultURL` instead.
const DefaultUrl = DefaultURL

// GetSchemaById returns the schema for some id.
// The schema registry only provides the schema itself, not the id, subject or version.
//
// DEPRECATED: Use `Client#GetSchemaByID` instead.
func (c *Client) GetSchemaById(id int) (string, error) {
	return c.GetSchemaByID(id)
}

// NewTlsClient returns a new Client that securely connects to baseurl.
//
// DEPRECATED: Use `schemaregistry.NewClient(baseURL, schemaregistry.UsingClient(customHTTPSClient))` instead.
func NewTlsClient(baseurl string, tlsConfig *tls.Config) (*Client, error) {
	u, err := url.Parse(baseurl)
	if err != nil {
		return nil, err
	}

	if u.Scheme != "https" {
		return nil, errors.New("func NewTlsClient: This method only accepts HTTPS URLs")
	}

	// TODO: Consider using golang.org/x/net/http2 to enable HTTP/2 with HTTPS connections
	httpsClientTransport := &http.Transport{
		TLSClientConfig: tlsConfig,
	}

	httpsClient := &http.Client{
		Transport: httpsClientTransport,
	}

	return NewClient(baseurl, UsingClient(httpsClient))
}
