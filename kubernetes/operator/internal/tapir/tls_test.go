package tapir

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// generateTestCerts creates a CA and leaf cert/key pair in dir, returning paths.
// The leaf cert has an IP SAN for 127.0.0.1.
func generateTestCerts(t *testing.T, dir string) (certFile, keyFile, caFile string) {
	t.Helper()

	// CA key and self-signed cert.
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	caTemplate := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "Test CA"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
	}
	caCertDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	caCert, err := x509.ParseCertificate(caCertDER)
	if err != nil {
		t.Fatal(err)
	}

	// Leaf key and cert signed by CA, with IP SAN 127.0.0.1.
	leafKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	leafTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "localhost"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		IPAddresses:  []net.IP{net.IPv4(127, 0, 0, 1)},
	}
	leafCertDER, err := x509.CreateCertificate(rand.Reader, leafTemplate, caCert, &leafKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}

	// Write PEM files.
	caFile = filepath.Join(dir, "ca.crt")
	certFile = filepath.Join(dir, "tls.crt")
	keyFile = filepath.Join(dir, "tls.key")

	writePEM(t, caFile, "CERTIFICATE", caCertDER)
	writePEM(t, certFile, "CERTIFICATE", leafCertDER)

	leafKeyDER, err := x509.MarshalECPrivateKey(leafKey)
	if err != nil {
		t.Fatal(err)
	}
	writePEM(t, keyFile, "EC PRIVATE KEY", leafKeyDER)

	return certFile, keyFile, caFile
}

func writePEM(t *testing.T, path, blockType string, data []byte) {
	t.Helper()
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	if err := pem.Encode(f, &pem.Block{Type: blockType, Bytes: data}); err != nil {
		t.Fatal(err)
	}
}

func TestLoadTLSConfig(t *testing.T) {
	dir := t.TempDir()
	certFile, keyFile, caFile := generateTestCerts(t, dir)

	cfg, err := LoadTLSConfig(certFile, keyFile, caFile)
	if err != nil {
		t.Fatalf("LoadTLSConfig() error: %v", err)
	}
	if cfg.RootCAs == nil {
		t.Fatal("expected non-nil RootCAs")
	}
	if cfg.MinVersion != tls.VersionTLS12 {
		t.Errorf("expected MinVersion TLS 1.2, got %d", cfg.MinVersion)
	}
	if cfg.GetClientCertificate == nil {
		t.Fatal("expected non-nil GetClientCertificate callback")
	}

	// Verify the callback returns a valid cert.
	cert, err := cfg.GetClientCertificate(nil)
	if err != nil {
		t.Fatalf("GetClientCertificate() error: %v", err)
	}
	if cert == nil || len(cert.Certificate) == 0 {
		t.Fatal("expected non-empty certificate from callback")
	}
}

func TestLoadTLSConfig_InvalidCA(t *testing.T) {
	dir := t.TempDir()
	certFile, keyFile, _ := generateTestCerts(t, dir)

	badCA := filepath.Join(dir, "bad-ca.crt")
	if err := os.WriteFile(badCA, []byte("not a pem"), 0644); err != nil {
		t.Fatal(err)
	}

	_, err := LoadTLSConfig(certFile, keyFile, badCA)
	if err == nil {
		t.Fatal("expected error for invalid CA")
	}
}

func TestLoadTLSConfig_MissingCert(t *testing.T) {
	dir := t.TempDir()
	_, _, caFile := generateTestCerts(t, dir)

	_, err := LoadTLSConfig(filepath.Join(dir, "nonexistent.crt"), filepath.Join(dir, "nonexistent.key"), caFile)
	if err == nil {
		t.Fatal("expected error for missing cert file")
	}
}

func TestLoadTLSConfig_CertReload(t *testing.T) {
	dir := t.TempDir()
	certFile, keyFile, caFile := generateTestCerts(t, dir)

	cfg, err := LoadTLSConfig(certFile, keyFile, caFile)
	if err != nil {
		t.Fatalf("LoadTLSConfig() error: %v", err)
	}

	// Get first cert.
	cert1, err := cfg.GetClientCertificate(nil)
	if err != nil {
		t.Fatalf("first GetClientCertificate() error: %v", err)
	}

	// Regenerate certs in the same paths (simulating cert-manager rotation).
	generateTestCerts(t, dir)

	// Get second cert — should be different (new serial/key).
	cert2, err := cfg.GetClientCertificate(nil)
	if err != nil {
		t.Fatalf("second GetClientCertificate() error: %v", err)
	}

	// The raw DER bytes should differ since we generated fresh keys.
	if len(cert1.Certificate) > 0 && len(cert2.Certificate) > 0 {
		if string(cert1.Certificate[0]) == string(cert2.Certificate[0]) {
			t.Error("expected different certificate after rotation")
		}
	}
}
