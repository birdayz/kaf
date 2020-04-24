package connection

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"time"

	"github.com/Shopify/sarama"
	"github.com/birdayz/kaf/pkg/config"
)

type ConnManager struct{}

func NewConnManager() *ConnManager {
	return &ConnManager{}
}

func (c *ConnManager) GetAdminClient(cluster string) (sarama.ClusterAdmin, error) {
	configTotal, err := config.ReadConfig("")
	if err != nil {
		return nil, err
	}

	var cl *config.Cluster
	if cluster == "" {
		cl = configTotal.ActiveCluster()
	} else {
		for _, cx := range configTotal.Clusters {
			if cx.Name == cluster {
				cl = cx
			}
		}
	}
	if cl == nil {
		cl = configTotal.ActiveCluster()
	}

	cfg, err := toSaramaConfig(cl)
	if err != nil {
		return nil, err
	}

	client, err := sarama.NewClient(cl.Brokers, cfg)
	if err != nil {
		return nil, err
	}

	return sarama.NewClusterAdminFromClient(client)
}

func toSaramaConfig(cluster *config.Cluster) (saramaConfig *sarama.Config, err error) {
	saramaConfig = sarama.NewConfig()
	saramaConfig.Version = sarama.V1_1_0_0
	saramaConfig.Producer.Return.Successes = true
	saramaConfig.Metadata.Full = true
	saramaConfig.Metadata.RefreshFrequency = time.Minute
	saramaConfig.Metadata.Retry.Max = 99

	if cluster.Version != "" {
		parsedVersion, err := sarama.ParseKafkaVersion(cluster.Version)
		if err != nil {
			return nil, fmt.Errorf("Unable to parse Kafka version: %v", err)
		}
		saramaConfig.Version = parsedVersion
	}
	if cluster.SASL != nil {
		saramaConfig.Net.SASL.Enable = true
		saramaConfig.Net.SASL.User = cluster.SASL.Username
		saramaConfig.Net.SASL.Password = cluster.SASL.Password
	}
	if cluster.TLS != nil && cluster.SecurityProtocol != "SASL_SSL" {
		saramaConfig.Net.TLS.Enable = true
		tlsConfig := &tls.Config{
			InsecureSkipVerify: cluster.TLS.Insecure,
		}

		if cluster.TLS.Cafile != "" {
			caCert, err := ioutil.ReadFile(cluster.TLS.Cafile)
			if err != nil {
				return nil, fmt.Errorf("Unable to read Cafile :%v", err)
			}
			caCertPool := x509.NewCertPool()
			caCertPool.AppendCertsFromPEM(caCert)
			tlsConfig.RootCAs = caCertPool
		}

		if cluster.TLS.Clientfile != "" && cluster.TLS.Clientkeyfile != "" {
			clientCert, err := ioutil.ReadFile(cluster.TLS.Clientfile)
			if err != nil {
				return nil, fmt.Errorf("Unable to read Clientfile :%v", err)
			}
			clientKey, err := ioutil.ReadFile(cluster.TLS.Clientkeyfile)
			if err != nil {
				return nil, fmt.Errorf("Unable to read Clientkeyfile :%v", err)
			}

			cert, err := tls.X509KeyPair([]byte(clientCert), []byte(clientKey))
			if err != nil {
				return nil, fmt.Errorf("Unable to creatre KeyPair: %v", err)
			}
			tlsConfig.Certificates = []tls.Certificate{cert}

			tlsConfig.BuildNameToCertificate()
		}
		saramaConfig.Net.TLS.Config = tlsConfig
	}
	if cluster.SecurityProtocol == "SASL_SSL" {
		saramaConfig.Net.TLS.Enable = true
		if cluster.TLS != nil {
			tlsConfig := &tls.Config{
				InsecureSkipVerify: cluster.TLS.Insecure,
			}
			if cluster.TLS.Cafile != "" {
				caCert, err := ioutil.ReadFile(cluster.TLS.Cafile)
				if err != nil {
					return nil, err
				}
				caCertPool := x509.NewCertPool()
				caCertPool.AppendCertsFromPEM(caCert)
				tlsConfig.RootCAs = caCertPool
			}
			saramaConfig.Net.TLS.Config = tlsConfig

		} else {
			saramaConfig.Net.TLS.Config = &tls.Config{InsecureSkipVerify: false}
		}
		if cluster.SASL.Mechanism == "SCRAM-SHA-512" {
			saramaConfig.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &XDGSCRAMClient{HashGeneratorFcn: SHA512} }
			saramaConfig.Net.SASL.Mechanism = sarama.SASLMechanism(sarama.SASLTypeSCRAMSHA512)
		} else if cluster.SASL.Mechanism == "SCRAM-SHA-256" {
			saramaConfig.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &XDGSCRAMClient{HashGeneratorFcn: SHA256} }
			saramaConfig.Net.SASL.Mechanism = sarama.SASLMechanism(sarama.SASLTypeSCRAMSHA256)
		}
	}
	return saramaConfig, nil
}
