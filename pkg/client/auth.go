package client

import (
	"context"
	"net/http"
	"sync"
	"time"

	"github.com/birdayz/kaf/pkg/config"
	aws_config "github.com/aws/aws-sdk-go-v2/config"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/aws"
	"github.com/twmb/franz-go/pkg/sasl/oauth"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"
)

func oauthMechanism(cluster *config.Cluster) sasl.Mechanism {
	s := cluster.SASL

	// Static token: no refresh needed.
	if s.Token != "" {
		return oauth.Oauth(func(_ context.Context) (oauth.Auth, error) {
			return oauth.Auth{Token: s.Token}, nil
		})
	}

	// OAuth2 client credentials flow with token caching.
	tp := &tokenCache{
		cfg: &clientcredentials.Config{
			ClientID:     s.ClientID,
			ClientSecret: s.ClientSecret,
			TokenURL:     s.TokenURL,
			Scopes:       s.Scopes,
		},
		refreshBuffer: 20 * time.Second,
	}

	return oauth.Oauth(func(ctx context.Context) (oauth.Auth, error) {
		tok, err := tp.token(ctx)
		if err != nil {
			return oauth.Auth{}, err
		}
		return oauth.Auth{Token: tok}, nil
	})
}

// awsMSKMechanism uses franz-go's native AWS_MSK_IAM SASL mechanism with
// SigV4 signing. Credentials are loaded from the default AWS credential chain.
func awsMSKMechanism(cluster *config.Cluster) (sasl.Mechanism, error) {
	return aws.ManagedStreamingIAM(func(ctx context.Context) (aws.Auth, error) {
		cfg, err := aws_config.LoadDefaultConfig(ctx)
		if err != nil {
			return aws.Auth{}, err
		}
		creds, err := cfg.Credentials.Retrieve(ctx)
		if err != nil {
			return aws.Auth{}, err
		}
		return aws.Auth{
			AccessKey:    creds.AccessKeyID,
			SecretKey:    creds.SecretAccessKey,
			SessionToken: creds.SessionToken,
		}, nil
	}), nil
}

// tokenCache provides thread-safe caching of OAuth2 tokens with refresh.
type tokenCache struct {
	mu            sync.Mutex
	cfg           *clientcredentials.Config
	cachedToken   string
	replaceAt     time.Time
	refreshBuffer time.Duration
}

func (tc *tokenCache) token(ctx context.Context) (string, error) {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	if tc.cachedToken != "" && time.Now().Before(tc.replaceAt) {
		return tc.cachedToken, nil
	}

	httpClient := &http.Client{Timeout: 10 * time.Second}
	ctx = context.WithValue(ctx, oauth2.HTTPClient, httpClient)

	tok, err := tc.cfg.Token(ctx)
	if err != nil {
		return "", err
	}

	tc.cachedToken = tok.AccessToken
	tc.replaceAt = tok.Expiry.Add(-tc.refreshBuffer)
	return tc.cachedToken, nil
}
