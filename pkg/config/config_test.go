package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReadConfig_LegacyYAML(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config")
	err := os.WriteFile(path, []byte(`current-cluster: local
clusters:
  - name: local
    brokers:
      - localhost:9092
    SASL:
      mechanism: PLAIN
      username: admin
      password: secret
    TLS:
      cafile: /etc/ssl/ca.pem
      insecure: true
    security-protocol: SASL_SSL
    schema-registry-url: http://localhost:8081
    schema-registry-credentials:
      username: sr-user
      password: sr-pass
`), 0644)
	require.NoError(t, err)

	cfg, err := ReadConfig(path)
	require.NoError(t, err)
	require.Equal(t, "local", cfg.CurrentCluster)
	require.Len(t, cfg.Clusters, 1)

	c := cfg.Clusters[0]
	require.Equal(t, "local", c.Name)
	require.Equal(t, []string{"localhost:9092"}, c.Brokers)
	require.Equal(t, "SASL_SSL", c.SecurityProtocol)
	require.Equal(t, "http://localhost:8081", c.SchemaRegistryURL)

	require.NotNil(t, c.SASL)
	require.Equal(t, "PLAIN", c.SASL.Mechanism)
	require.Equal(t, "admin", c.SASL.Username)
	require.Equal(t, "secret", c.SASL.Password)

	require.NotNil(t, c.TLS)
	require.Equal(t, "/etc/ssl/ca.pem", c.TLS.Cafile)
	require.True(t, c.TLS.Insecure)

	require.NotNil(t, c.SchemaRegistryCredentials)
	require.Equal(t, "sr-user", c.SchemaRegistryCredentials.Username)
	require.Equal(t, "sr-pass", c.SchemaRegistryCredentials.Password)
}

func TestReadConfig_ExplicitPathMustExist(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "nonexistent")
	_, err := ReadConfig(path)
	require.Error(t, err)
}

func TestHasCluster(t *testing.T) {
	cfg := Config{
		Clusters: []*Cluster{
			{Name: "a"},
			{Name: "b"},
		},
	}
	require.True(t, cfg.HasCluster("a"))
	require.True(t, cfg.HasCluster("b"))
	require.False(t, cfg.HasCluster("c"))
}

func TestActiveCluster(t *testing.T) {
	cfg := Config{
		CurrentCluster: "prod",
		Clusters: []*Cluster{
			{Name: "dev", Brokers: []string{"dev:9092"}},
			{Name: "prod", Brokers: []string{"prod:9092"}},
		},
	}

	c := cfg.ActiveCluster()
	require.NotNil(t, c)
	require.Equal(t, "prod", c.Name)

	// ClusterOverride takes precedence.
	cfg.ClusterOverride = "dev"
	c = cfg.ActiveCluster()
	require.NotNil(t, c)
	require.Equal(t, "dev", c.Name)
}

func TestActiveCluster_NotFound(t *testing.T) {
	cfg := Config{
		CurrentCluster: "missing",
		Clusters:       []*Cluster{{Name: "other"}},
	}
	require.Nil(t, cfg.ActiveCluster())
}
