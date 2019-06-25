// Package schemaregistry provides a client for Confluent's Kafka Schema Registry REST API.
package schemaregistry

import (
	"bytes"
	"compress/gzip"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// DefaultURL is the address where a local schema registry listens by default.
const DefaultURL = "http://localhost:8081"

type (
	httpDoer interface {
		Do(req *http.Request) (resp *http.Response, err error)
	}
	// Client is the registry schema REST API client.
	Client struct {
		baseURL string

		// the client is created on the `NewClient` function, it can be customized via options.
		client httpDoer
	}

	// Option describes an optional runtime configurator that can be passed on `NewClient`.
	// Custom `Option` can be used as well, it's just a type of `func(*schemaregistry.Client)`.
	//
	// Look `UsingClient`.
	Option func(*Client)
)

// UsingClient modifies the underline HTTP Client that schema registry is using for contact with the backend server.
func UsingClient(httpClient *http.Client) Option {
	return func(c *Client) {
		if httpClient == nil {
			return
		}

		transport := getTransportLayer(httpClient, 0)
		httpClient.Transport = transport

		c.client = httpClient
	}
}

func getTransportLayer(httpClient *http.Client, timeout time.Duration) (t http.RoundTripper) {
	if t := httpClient.Transport; t != nil {
		return t
	}

	httpTransport := &http.Transport{
		TLSNextProto: make(map[string]func(authority string, c *tls.Conn) http.RoundTripper),
	}

	if timeout > 0 {
		httpTransport.Dial = func(network string, addr string) (net.Conn, error) {
			return net.DialTimeout(network, addr, timeout)
		}
	}

	return httpTransport
}

// formatBaseURL will try to make sure that the schema:host:port pattern is followed on the `baseURL` field.
func formatBaseURL(baseURL string) string {
	if baseURL == "" {
		return ""
	}

	// remove last slash, so the API can append the path with ease.
	if baseURL[len(baseURL)-1] == '/' {
		baseURL = baseURL[0 : len(baseURL)-1]
	}

	portIdx := strings.LastIndexByte(baseURL, ':')

	schemaIdx := strings.Index(baseURL, "://")
	hasSchema := schemaIdx >= 0
	hasPort := portIdx > schemaIdx+1

	var port = "80"
	if hasPort {
		port = baseURL[portIdx+1:]
	}

	// find the schema based on the port.
	if !hasSchema {
		if port == "443" {
			baseURL = "https://" + baseURL
		} else {
			baseURL = "http://" + baseURL
		}
	} else if !hasPort {
		// has schema but not port.
		if strings.HasPrefix(baseURL, "https://") {
			port = "443"
		}
	}

	// finally, append the port part if it wasn't there.
	if !hasPort {
		baseURL += ":" + port
	}

	return baseURL
}

// NewClient creates & returns a new Registry Schema Client
// based on the passed url and the options.
func NewClient(baseURL string, options ...Option) (*Client, error) {
	baseURL = formatBaseURL(baseURL)
	if _, err := url.Parse(baseURL); err != nil {
		return nil, err
	}

	c := &Client{baseURL: baseURL}
	for _, opt := range options {
		opt(c)
	}

	if c.client == nil {
		httpClient := &http.Client{}
		UsingClient(httpClient)(c)
	}

	return c, nil
}

const (
	contentTypeHeaderKey = "Content-Type"
	contentTypeJSON      = "application/json"

	acceptHeaderKey          = "Accept"
	acceptEncodingHeaderKey  = "Accept-Encoding"
	contentEncodingHeaderKey = "Content-Encoding"
	gzipEncodingHeaderValue  = "gzip"
)

// ResourceError is being fired from all API calls when an error code is received.
type ResourceError struct {
	ErrorCode int    `json:"error_code"`
	Method    string `json:"method,omitempty"`
	URI       string `json:"uri,omitempty"`
	Message   string `json:"message,omitempty"`
}

func (err ResourceError) Error() string {
	return fmt.Sprintf("client: (%s: %s) failed with error code %d%s",
		err.Method, err.URI, err.ErrorCode, err.Message)
}

func newResourceError(errCode int, uri, method, body string) ResourceError {
	unescapedURI, _ := url.QueryUnescape(uri)

	return ResourceError{
		ErrorCode: errCode,
		URI:       unescapedURI,
		Method:    method,
		Message:   body,
	}
}

// These numbers are used by the schema registry to communicate errors.
const (
	subjectNotFoundCode = 40401
	schemaNotFoundCode  = 40403
)

// IsSubjectNotFound checks the returned error to see if it is kind of a subject not found  error code.
func IsSubjectNotFound(err error) bool {
	if err == nil {
		return false
	}

	if resErr, ok := err.(ResourceError); ok {
		return resErr.ErrorCode == subjectNotFoundCode
	}

	return false
}

// IsSchemaNotFound checks the returned error to see if it is kind of a schema not found error code.
func IsSchemaNotFound(err error) bool {
	if err == nil {
		return false
	}

	if resErr, ok := err.(ResourceError); ok {
		return resErr.ErrorCode == schemaNotFoundCode
	}

	return false
}

// isOK is called inside the `Client#do` and it closes the body reader if no accessible.
func isOK(resp *http.Response) bool {
	return !(resp.StatusCode < 200 || resp.StatusCode >= 300)
}

var noOpBuffer = new(bytes.Buffer)

func acquireBuffer(b []byte) *bytes.Buffer {
	if len(b) > 0 {
		return bytes.NewBuffer(b)
	}

	return noOpBuffer
}

const schemaAPIVersion = "v1"
const contentTypeSchemaJSON = "application/vnd.schemaregistry." + schemaAPIVersion + "+json"

func (c *Client) do(method, path, contentType string, send []byte) (*http.Response, error) {
	if path[0] == '/' {
		path = path[1:]
	}

	uri := c.baseURL + "/" + path

	req, err := http.NewRequest(method, uri, acquireBuffer(send))
	if err != nil {
		return nil, err
	}

	// set the content type if any.
	if contentType != "" {
		req.Header.Set(contentTypeHeaderKey, contentType)
	}

	// response accept gziped content.
	req.Header.Add(acceptEncodingHeaderKey, gzipEncodingHeaderValue)
	req.Header.Add(acceptHeaderKey, contentTypeSchemaJSON+", application/vnd.schemaregistry+json, application/json")

	// send the request and check the response for any connection & authorization errors here.
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}

	if !isOK(resp) {
		defer resp.Body.Close()
		var errBody string
		respContentType := resp.Header.Get(contentTypeHeaderKey)

		if strings.Contains(respContentType, "text/html") {
			// if the body is html, then don't read it, it doesn't contain the raw info we need.
		} else if strings.Contains(respContentType, "json") {
			// if it's json try to read it as confluent's specific error json.
			var resErr ResourceError
			c.readJSON(resp, &resErr)
			return nil, resErr
		} else {
			// else give the whole body to the error context.
			b, err := c.readResponseBody(resp)
			if err != nil {
				errBody = " unable to read body: " + err.Error()
			} else {
				errBody = "\n" + string(b)
			}
		}

		return nil, newResourceError(resp.StatusCode, uri, method, errBody)
	}

	return resp, nil
}

type gzipReadCloser struct {
	respReader io.ReadCloser
	gzipReader io.ReadCloser
}

func (rc *gzipReadCloser) Close() error {
	if rc.gzipReader != nil {
		defer rc.gzipReader.Close()
	}

	return rc.respReader.Close()
}

func (rc *gzipReadCloser) Read(p []byte) (n int, err error) {
	if rc.gzipReader != nil {
		return rc.gzipReader.Read(p)
	}

	return rc.respReader.Read(p)
}

func (c *Client) acquireResponseBodyStream(resp *http.Response) (io.ReadCloser, error) {
	// check for gzip and read it, the right way.
	var (
		reader = resp.Body
		err    error
	)

	if encoding := resp.Header.Get(contentEncodingHeaderKey); encoding == gzipEncodingHeaderValue {
		reader, err = gzip.NewReader(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("client: failed to read gzip compressed content, trace: %v", err)
		}
		// we wrap the gzipReader and the underline response reader
		// so a call of .Close() can close both of them with the correct order when finish reading, the caller decides.
		// Must close manually using a defer on the callers before the `readResponseBody` call,
		// note that the `readJSON` can decide correctly by itself.
		return &gzipReadCloser{
			respReader: resp.Body,
			gzipReader: reader,
		}, nil
	}

	// return the stream reader.
	return reader, err
}

func (c *Client) readResponseBody(resp *http.Response) ([]byte, error) {
	reader, err := c.acquireResponseBodyStream(resp)
	if err != nil {
		return nil, err
	}

	body, err := ioutil.ReadAll(reader)
	if err = reader.Close(); err != nil {
		return nil, err
	}

	// return the body.
	return body, err
}

func (c *Client) readJSON(resp *http.Response, valuePtr interface{}) error {
	b, err := c.readResponseBody(resp)
	if err != nil {
		return err
	}

	return json.Unmarshal(b, valuePtr)
}

var errRequired = func(field string) error {
	return fmt.Errorf("client: %s is required", field)
}

const (
	subjectsPath = "subjects"
	subjectPath  = subjectsPath + "/%s"
	schemaPath   = "schemas/ids/%d"
)

// Subjects returns a list of the available subjects(schemas).
// https://docs.confluent.io/current/schema-registry/docs/api.html#subjects
func (c *Client) Subjects() (subjects []string, err error) {
	// # List all available subjects
	// GET /subjects
	resp, respErr := c.do(http.MethodGet, subjectsPath, "", nil)
	if respErr != nil {
		err = respErr
		return
	}

	err = c.readJSON(resp, &subjects)
	return
}

// Versions returns all schema version numbers registered for this subject.
func (c *Client) Versions(subject string) (versions []int, err error) {
	if subject == "" {
		err = errRequired("subject")
		return
	}

	// # List all versions of a particular subject
	// GET /subjects/(string: subject)/versions
	path := fmt.Sprintf(subjectPath, subject+"/versions")
	resp, respErr := c.do(http.MethodGet, path, "", nil)
	if respErr != nil {
		err = respErr
		return
	}

	err = c.readJSON(resp, &versions)
	return
}

// DeleteSubject deletes the specified subject and its associated compatibility level if registered.
// It is recommended to use this API only when a topic needs to be recycled or in development environment.
// Returns the versions of the schema deleted under this subject.
func (c *Client) DeleteSubject(subject string) (versions []int, err error) {
	if subject == "" {
		err = errRequired("subject")
		return
	}

	// DELETE /subjects/(string: subject)
	path := fmt.Sprintf(subjectPath, subject)
	resp, respErr := c.do(http.MethodDelete, path, "", nil)
	if respErr != nil {
		err = respErr
		return
	}

	err = c.readJSON(resp, &versions)
	return
}

// IsRegistered tells if the given "schema" is registered for this "subject".
func (c *Client) IsRegistered(subject, schema string) (bool, Schema, error) {
	var fs Schema

	sc := schemaOnlyJSON{schema}
	send, err := json.Marshal(sc)
	if err != nil {
		return false, fs, err
	}

	path := fmt.Sprintf(subjectPath, subject)
	resp, err := c.do(http.MethodPost, path, "", send)
	if err != nil {
		// schema not found?
		if IsSchemaNotFound(err) {
			return false, fs, nil
		}
		// error?
		return false, fs, err
	}

	if err = c.readJSON(resp, &fs); err != nil {
		return true, fs, err // found but error when unmarshal.
	}

	// so we have a schema.
	return true, fs, nil
}

type (
	schemaOnlyJSON struct {
		Schema string `json:"schema"`
	}

	idOnlyJSON struct {
		ID int `json:"id"`
	}

	isCompatibleJSON struct {
		IsCompatible bool `json:"is_compatible"`
	}

	// Schema describes a schema, look `GetSchema` for more.
	Schema struct {
		// Schema is the Avro schema string.
		Schema string `json:"schema"`
		// Subject where the schema is registered for.
		Subject string `json:"subject"`
		// Version of the returned schema.
		Version int `json:"version"`
		ID      int `json:"id,omitempty"`
	}

	// Config describes a subject or globa schema-registry configuration
	Config struct {
		// CompatibilityLevel mode of subject or global
		CompatibilityLevel string `json:"compatibilityLevel"`
	}
)

// RegisterNewSchema registers a schema.
// The returned identifier should be used to retrieve
// this schema from the schemas resource and is different from
// the schema’s version which is associated with that name.
func (c *Client) RegisterNewSchema(subject string, avroSchema string) (int, error) {
	if subject == "" {
		return 0, errRequired("subject")
	}
	if avroSchema == "" {
		return 0, errRequired("avroSchema")
	}

	schema := schemaOnlyJSON{
		Schema: avroSchema,
	}

	send, err := json.Marshal(schema)
	if err != nil {
		return 0, err
	}

	// # Register a new schema under a particular subject
	// POST /subjects/(string: subject)/versions

	path := fmt.Sprintf(subjectPath+"/versions", subject)
	resp, err := c.do(http.MethodPost, path, contentTypeSchemaJSON, send)
	if err != nil {
		return 0, err
	}

	var res idOnlyJSON
	err = c.readJSON(resp, &res)
	return res.ID, err
}

// JSONAvroSchema converts and returns the json form of the "avroSchema" as []byte.
func JSONAvroSchema(avroSchema string) (json.RawMessage, error) {
	var raw json.RawMessage
	err := json.Unmarshal(json.RawMessage(avroSchema), &raw)
	if err != nil {
		return nil, err
	}
	return raw, err
}

// GetSchemaByID returns the Auro schema string identified by the id.
// id (int) – the globally unique identifier of the schema.
func (c *Client) GetSchemaByID(subjectID int) (string, error) {
	// # Get the schema for a particular subject id
	// GET /schemas/ids/{int: id}
	path := fmt.Sprintf(schemaPath, subjectID)
	resp, err := c.do(http.MethodGet, path, "", nil)
	if err != nil {
		return "", err
	}

	var res schemaOnlyJSON
	if err = c.readJSON(resp, &res); err != nil {
		return "", err
	}

	return res.Schema, nil
}

// SchemaLatestVersion is the only one valid string for the "versionID", it's the "latest" version string and it's used on `GetLatestSchema`.
const SchemaLatestVersion = "latest"

func checkSchemaVersionID(versionID interface{}) error {
	if versionID == nil {
		return errRequired("versionID (string \"latest\" or int)")
	}

	if verStr, ok := versionID.(string); ok {
		if verStr != SchemaLatestVersion {
			return fmt.Errorf("client: %v string is not a valid value for the versionID input parameter [versionID == \"latest\"]", versionID)
		}
	}

	if verInt, ok := versionID.(int); ok {
		if verInt <= 0 || verInt > 2^31-1 { // it's the max of int32, math.MaxInt32 already but do that check.
			return fmt.Errorf("client: %v integer is not a valid value for the versionID input parameter [ versionID > 0 && versionID <= 2^31-1]", versionID)
		}
	}

	return nil
}

// subject (string) – Name of the subject
// version (versionId [string "latest" or 1,2^31-1]) – Version of the schema to be returned.
// Valid values for versionId are between [1,2^31-1] or the string “latest”.
// The string “latest” refers to the last registered schema under the specified subject.
// Note that there may be a new latest schema that gets registered right after this request is served.
//
// It's not safe to use just an interface to the high-level API, therefore we split this method
// to two, one which will retrieve the latest versioned schema and the other which will accept
// the version as integer and it will retrieve by a specific version.
//
// See `GetLatestSchema` and `GetSchemaAtVersion` instead.
func (c *Client) getSubjectSchemaAtVersion(subject string, versionID interface{}) (s Schema, err error) {
	if subject == "" {
		err = errRequired("subject")
		return
	}

	if err = checkSchemaVersionID(versionID); err != nil {
		return
	}

	// # Get the schema at a particular version
	// GET /subjects/(string: subject)/versions/(versionId: "latest" | int)
	path := fmt.Sprintf(subjectPath+"/versions/%v", subject, versionID)
	resp, respErr := c.do(http.MethodGet, path, "", nil)
	if respErr != nil {
		err = respErr
		return
	}

	err = c.readJSON(resp, &s)
	return
}

// GetSchemaBySubject returns the schema for a particular subject and version.
func (c *Client) GetSchemaBySubject(subject string, versionID int) (Schema, error) {
	return c.getSubjectSchemaAtVersion(subject, versionID)
}

// GetLatestSchema returns the latest version of a schema.
// See `GetSchemaAtVersion` to retrieve a subject schema by a specific version.
func (c *Client) GetLatestSchema(subject string) (Schema, error) {
	return c.getSubjectSchemaAtVersion(subject, SchemaLatestVersion)
}

// getConfigSubject returns the Config of global or for a given subject. It handles 404 error in a
// different way, since not-found for a subject configuration means it's using global.
func (c *Client) getConfigSubject(subject string) (Config, error) {
	var err error
	var config = Config{}

	path := fmt.Sprintf("/config/%s", subject)
	resp, respErr := c.do(http.MethodGet, path, "", nil)
	if respErr != nil && respErr.(ResourceError).ErrorCode != 404 {
		return config, respErr
	}
	if resp != nil {
		err = c.readJSON(resp, &config)
	}

	return config, err
}

// GetConfig returns the configuration (Config type) for global Schema-Registry or a specific
// subject. When Config returned has "compatibilityLevel" empty, it's using global settings.
func (c *Client) GetConfig(subject string) (Config, error) {
	return c.getConfigSubject(subject)
}

// subject (string) – Name of the subject
// version (versionId [string "latest" or 1,2^31-1]) – Version of the schema to be returned.
// Valid values for versionId are between [1,2^31-1] or the string “latest”.
// The string “latest” refers to the last registered schema under the specified subject.
// Note that there may be a new latest schema that gets registered right after this request is served.
//
// It's not safe to use just an interface to the high-level API, therefore we split this method
// to two, one which will retrieve the latest versioned schema and the other which will accept
// the version as integer and it will retrieve by a specific version.
//
// See `IsSchemaCompatible` and `IsLatestSchemaCompatible` instead.
func (c *Client) isSchemaCompatibleAtVersion(subject string, avroSchema string, versionID interface{}) (combatible bool, err error) {
	if subject == "" {
		err = errRequired("subject")
		return
	}
	if avroSchema == "" {
		err = errRequired("avroSchema")
		return
	}

	if err = checkSchemaVersionID(versionID); err != nil {
		return
	}

	schema := schemaOnlyJSON{
		Schema: avroSchema,
	}

	send, err := json.Marshal(schema)
	if err != nil {
		return
	}

	// # Test input schema against a particular version of a subject’s schema for compatibility
	// POST /compatibility/subjects/(string: subject)/versions/(versionId: "latest" | int)
	path := fmt.Sprintf("compatibility/"+subjectPath+"/versions/%v", subject, versionID)
	resp, err := c.do(http.MethodPost, path, contentTypeSchemaJSON, send)
	if err != nil {
		return
	}

	var res isCompatibleJSON
	err = c.readJSON(resp, &res)
	return res.IsCompatible, err
}

// IsSchemaCompatible tests compatibility with a specific version of a subject's schema.
func (c *Client) IsSchemaCompatible(subject string, avroSchema string, versionID int) (bool, error) {
	return c.isSchemaCompatibleAtVersion(subject, avroSchema, versionID)
}

// IsLatestSchemaCompatible tests compatibility with the latest version of a subject's schema.
func (c *Client) IsLatestSchemaCompatible(subject string, avroSchema string) (bool, error) {
	return c.isSchemaCompatibleAtVersion(subject, avroSchema, SchemaLatestVersion)
}
