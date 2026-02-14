package client

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/birdayz/kaf/pkg/config"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

// Client wraps a franz-go kgo.Client and kadm.Client.
type Client struct {
	KGO   *kgo.Client
	Admin *kadm.Client
}

// Close closes both the admin and kgo clients.
func (c *Client) Close() {
	if c.Admin != nil {
		c.Admin.Close()
	}
	if c.KGO != nil {
		c.KGO.Close()
	}
}

// New creates a new Client from a Cluster config.
func New(cluster *config.Cluster, opts ...kgo.Opt) (*Client, error) {
	baseOpts := []kgo.Opt{
		kgo.SeedBrokers(cluster.Brokers...),
		kgo.ClientID("kaf"),
		kgo.DialTimeout(10 * time.Second),
		kgo.RequestTimeoutOverhead(10 * time.Second),
		kgo.ConnIdleTimeout(60 * time.Second),
	}

	tlsCfg, err := buildTLS(cluster)
	if err != nil {
		return nil, fmt.Errorf("TLS config: %w", err)
	}
	if tlsCfg != nil {
		baseOpts = append(baseOpts, kgo.DialTLSConfig(tlsCfg))
	}

	saslMech, err := buildSASL(cluster)
	if err != nil {
		return nil, fmt.Errorf("SASL config: %w", err)
	}
	if saslMech != nil {
		if strings.EqualFold(cluster.SASL.Mechanism, "PLAIN") && tlsCfg == nil {
			fmt.Fprintf(os.Stderr, "WARNING: SASL PLAIN without TLS sends credentials in cleartext\n")
		}
		baseOpts = append(baseOpts, kgo.SASL(saslMech))
	}

	baseOpts = append(baseOpts, opts...)

	cl, err := kgo.NewClient(baseOpts...)
	if err != nil {
		return nil, fmt.Errorf("create kgo client: %w", err)
	}

	adm := kadm.NewClient(cl)

	return &Client{
		KGO:   cl,
		Admin: adm,
	}, nil
}

func buildTLS(cluster *config.Cluster) (*tls.Config, error) {
	needsTLS := cluster.TLS != nil ||
		cluster.SecurityProtocol == "SASL_SSL" ||
		cluster.SecurityProtocol == "SSL"

	if !needsTLS {
		return nil, nil
	}

	cfg := &tls.Config{
		MinVersion: tls.VersionTLS12,
	}

	if cluster.TLS != nil {
		cfg.InsecureSkipVerify = cluster.TLS.Insecure
		if cluster.TLS.Insecure {
			fmt.Fprintf(os.Stderr, "WARNING: TLS certificate verification is disabled (insecure)\n")
		}

		if cluster.TLS.Cafile != "" {
			caCert, err := os.ReadFile(cluster.TLS.Cafile)
			if err != nil {
				return nil, fmt.Errorf("read CA file: %w", err)
			}
			pool := x509.NewCertPool()
			pool.AppendCertsFromPEM(caCert)
			cfg.RootCAs = pool
		}

		if cluster.TLS.Clientfile != "" && cluster.TLS.Clientkeyfile != "" {
			cert, err := tls.LoadX509KeyPair(cluster.TLS.Clientfile, cluster.TLS.Clientkeyfile)
			if err != nil {
				return nil, fmt.Errorf("load client cert: %w", err)
			}
			cfg.Certificates = []tls.Certificate{cert}
		}
	}

	return cfg, nil
}

func buildSASL(cluster *config.Cluster) (sasl.Mechanism, error) {
	if cluster.SASL == nil {
		return nil, nil
	}

	switch strings.ToUpper(cluster.SASL.Mechanism) {
	case "PLAIN":
		return plain.Plain(func(_ context.Context) (plain.Auth, error) {
			return plain.Auth{
				User: cluster.SASL.Username,
				Pass: cluster.SASL.Password,
			}, nil
		}), nil

	case "SCRAM-SHA-256":
		return scram.Sha256(func(_ context.Context) (scram.Auth, error) {
			return scram.Auth{
				User: cluster.SASL.Username,
				Pass: cluster.SASL.Password,
			}, nil
		}), nil

	case "SCRAM-SHA-512":
		return scram.Sha512(func(_ context.Context) (scram.Auth, error) {
			return scram.Auth{
				User: cluster.SASL.Username,
				Pass: cluster.SASL.Password,
			}, nil
		}), nil

	case "OAUTHBEARER":
		return oauthMechanism(cluster), nil

	case "AWS_MSK_IAM":
		return awsMSKMechanism(cluster)

	default:
		return nil, fmt.Errorf("unsupported SASL mechanism: %s", cluster.SASL.Mechanism)
	}
}
