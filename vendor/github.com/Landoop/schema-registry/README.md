Schema Registry CLI and client
==============================================

This repository contains a Command Line Interface (CLI) and a Go client for the REST API of Confluent's Kafka Schema Registry.

[![Build Status](https://travis-ci.org/Landoop/schema-registry.svg?branch=master)](https://travis-ci.org/Landoop/schema-registry)
[![GoDoc](https://godoc.org/github.com/Landoop/schema-registry?status.svg)](https://godoc.org/github.com/Landoop/schema-registry)
[![Chat](https://img.shields.io/badge/join-%20chat-00BCD4.svg?style=flat-square)](https://slackpass.io/landoop-community)

CLI
---

To install the CLI, assuming a properly setup Go installation, do:

`go get -u github.com/landoop/schema-registry/schema-registry-cli`

After that, the CLI is found in `$GOPATH/bin/schema-registry-cli`. Running `schema-registry-cli` without arguments gives:

```
A command line interface for the Confluent schema registry

Usage:
  schema-registry-cli [command]

Available Commands:
  add         registers the schema provided through stdin
  compatible  tests compatibility between a schema from stdin and a given subject
  exists      checks if the schema provided through stdin exists for the subject
  get         retrieves a schema specified by id or subject
  get-config  retrieves global or suject specific configuration
  subjects    lists all registered subjects
  versions    lists all available versions

Flags:
  -h, --help         help for schema-registry-cli
  -n, --no-color     dont color output
  -e, --url string   schema registry url, overrides SCHEMA_REGISTRY_URL (default "http://localhost:8081")
  -v, --verbose      be verbose

Use "schema-registry-cli [command] --help" for more information about a command.
```

The schema registry url can be configured through the `SCHEMA_REGISTRY_URL` environment variable, and overridden through `--url`. When none is provided, `http://localhost:8081` is used as default.

Client
------

The client package provides a client to deal with the registry from code. It is used by the CLI internally. Usage looks like:

```go
import "github.com/landoop/schema-registry"

client, _ := schemaregistry.NewClient(schemaregistry.DefaultUrl)
client.Subjects()
```

Or, to use with a Schema Registry endpoint listening on HTTPS:

```go
import (
    "crypto/tls"
    "crypto/x509"
    "io/ioutil"

    "github.com/landoop/schema-registry"
)

// Create a TLS config to use to connect to Schema Registry. This config will permit TLS connections to an endpoint
// whose TLS cert is signed by the given caFile.
caCert, err := ioutil.ReadFile("/path/to/ca/file")
if err != nil {
    panic(err)
}

caCertPool := x509.NewCertPool()
caCertPool.AppendCertsFromPEM(caCert)

tlsConfig :=  &tls.Config{
    RootCAs:            caCertPool,
    InsecureSkipVerify: true,
}

httpsClientTransport := &http.Transport{
  TLSClientConfig: tlsConfig,
}

httpsClient := &http.Client{
  Transport: httpsClientTransport,
}

// Create the Schema Registry client
client, _ := schemaregistry.NewClient(baseurl, UsingClient(httpsClient))
client.Subjects()
```

The documentation of the package can be found here: [![GoDoc](https://godoc.org/github.com/Landoop/schema-registry?status.svg)](https://godoc.org/github.com/Landoop/schema-registry)
