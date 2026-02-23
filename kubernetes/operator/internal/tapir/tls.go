package tapir

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
)

// LoadTLSConfig creates a tls.Config suitable for mTLS client connections.
//
// The GetClientCertificate callback reads cert and key files from disk on every
// new TLS handshake, so cert-manager rotations take effect automatically without
// restarts. Since both AdminClient and ShardManagerClient open short-lived
// connections (one per API call), rotated certificates are picked up on the next
// call.
//
// RootCAs are loaded once at construction time. CA rotation is rare and handled
// by pod restart if needed.
func LoadTLSConfig(certFile, keyFile, caFile string) (*tls.Config, error) {
	caPEM, err := os.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("read CA file %s: %w", caFile, err)
	}
	caPool := x509.NewCertPool()
	if !caPool.AppendCertsFromPEM(caPEM) {
		return nil, fmt.Errorf("no valid CA certificates in %s", caFile)
	}

	// Verify the cert and key are loadable at construction time.
	if _, err := tls.LoadX509KeyPair(certFile, keyFile); err != nil {
		return nil, fmt.Errorf("load cert/key (%s, %s): %w", certFile, keyFile, err)
	}

	return &tls.Config{
		RootCAs:    caPool,
		MinVersion: tls.VersionTLS12,
		GetClientCertificate: func(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
			cert, err := tls.LoadX509KeyPair(certFile, keyFile)
			if err != nil {
				return nil, fmt.Errorf("reload cert/key (%s, %s): %w", certFile, keyFile, err)
			}
			return &cert, nil
		},
	}, nil
}
