package client

import (
	"crypto/tls"
	"testing"

	"github.com/birdayz/kaf/pkg/config"
)

func TestBuildTLS_NoTLS(t *testing.T) {
	cluster := &config.Cluster{
		Brokers: []string{"localhost:9092"},
	}
	cfg, err := buildTLS(cluster)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg != nil {
		t.Fatal("expected nil TLS config for plain cluster")
	}
}

func TestBuildTLS_SASLSSL(t *testing.T) {
	cluster := &config.Cluster{
		Brokers:          []string{"localhost:9092"},
		SecurityProtocol: "SASL_SSL",
	}
	cfg, err := buildTLS(cluster)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg == nil {
		t.Fatal("expected non-nil TLS config for SASL_SSL")
	}
	if cfg.MinVersion != tls.VersionTLS12 {
		t.Fatalf("expected MinVersion TLS 1.2, got %d", cfg.MinVersion)
	}
}

func TestBuildTLS_SSL(t *testing.T) {
	cluster := &config.Cluster{
		Brokers:          []string{"localhost:9092"},
		SecurityProtocol: "SSL",
	}
	cfg, err := buildTLS(cluster)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg == nil {
		t.Fatal("expected non-nil TLS config for SSL")
	}
}

func TestBuildTLS_ExplicitTLSBlock(t *testing.T) {
	cluster := &config.Cluster{
		Brokers: []string{"localhost:9092"},
		TLS: &config.TLS{
			Insecure: true,
		},
	}
	cfg, err := buildTLS(cluster)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg == nil {
		t.Fatal("expected non-nil TLS config")
	}
	if !cfg.InsecureSkipVerify {
		t.Fatal("expected InsecureSkipVerify=true")
	}
}

func TestBuildTLS_CAFile_NotFound(t *testing.T) {
	cluster := &config.Cluster{
		Brokers: []string{"localhost:9092"},
		TLS: &config.TLS{
			Cafile: "/nonexistent/ca.pem",
		},
	}
	_, err := buildTLS(cluster)
	if err == nil {
		t.Fatal("expected error for nonexistent CA file")
	}
}

func TestBuildSASL_NoSASL(t *testing.T) {
	cluster := &config.Cluster{
		Brokers: []string{"localhost:9092"},
	}
	mech, err := buildSASL(cluster)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if mech != nil {
		t.Fatal("expected nil SASL mechanism")
	}
}

func TestBuildSASL_Plain(t *testing.T) {
	cluster := &config.Cluster{
		Brokers: []string{"localhost:9092"},
		SASL: &config.SASL{
			Mechanism: "PLAIN",
			Username:  "user",
			Password:  "pass",
		},
	}
	mech, err := buildSASL(cluster)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if mech == nil {
		t.Fatal("expected non-nil SASL mechanism for PLAIN")
	}
	if mech.Name() != "PLAIN" {
		t.Fatalf("expected mechanism name PLAIN, got %s", mech.Name())
	}
}

func TestBuildSASL_ScramSHA256(t *testing.T) {
	cluster := &config.Cluster{
		SASL: &config.SASL{
			Mechanism: "SCRAM-SHA-256",
			Username:  "user",
			Password:  "pass",
		},
	}
	mech, err := buildSASL(cluster)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if mech == nil {
		t.Fatal("expected non-nil SASL mechanism")
	}
	if mech.Name() != "SCRAM-SHA-256" {
		t.Fatalf("expected SCRAM-SHA-256, got %s", mech.Name())
	}
}

func TestBuildSASL_ScramSHA512(t *testing.T) {
	cluster := &config.Cluster{
		SASL: &config.SASL{
			Mechanism: "SCRAM-SHA-512",
			Username:  "user",
			Password:  "pass",
		},
	}
	mech, err := buildSASL(cluster)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if mech == nil {
		t.Fatal("expected non-nil SASL mechanism")
	}
	if mech.Name() != "SCRAM-SHA-512" {
		t.Fatalf("expected SCRAM-SHA-512, got %s", mech.Name())
	}
}

func TestBuildSASL_CaseInsensitive(t *testing.T) {
	cluster := &config.Cluster{
		SASL: &config.SASL{
			Mechanism: "plain",
			Username:  "user",
			Password:  "pass",
		},
	}
	mech, err := buildSASL(cluster)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if mech == nil {
		t.Fatal("expected non-nil SASL mechanism for lowercase 'plain'")
	}
}

func TestBuildSASL_Unsupported(t *testing.T) {
	cluster := &config.Cluster{
		SASL: &config.SASL{
			Mechanism: "KERBEROS",
		},
	}
	_, err := buildSASL(cluster)
	if err == nil {
		t.Fatal("expected error for unsupported mechanism")
	}
}
