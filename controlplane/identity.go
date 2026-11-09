package controlplane

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
)

// DeviceIdentity is a device's Ed25519 keypair. The private key proves possession during the
// pairing exchange.
type DeviceIdentity struct {
	private ed25519.PrivateKey
}

// GenerateDeviceIdentity creates a new random device identity.
func GenerateDeviceIdentity() (*DeviceIdentity, error) {
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("controlplane: generate device key: %w", err)
	}
	return &DeviceIdentity{private: priv}, nil
}

// DeviceIdentityFromSeed derives an identity from a 32-byte Ed25519 seed (RFC 8032).
func DeviceIdentityFromSeed(seed []byte) (*DeviceIdentity, error) {
	if len(seed) != ed25519.SeedSize {
		return nil, fmt.Errorf("controlplane: device seed must be %d bytes", ed25519.SeedSize)
	}
	return &DeviceIdentity{private: ed25519.NewKeyFromSeed(seed)}, nil
}

// NewDeviceIdentity wraps an existing Ed25519 private key.
func NewDeviceIdentity(private ed25519.PrivateKey) (*DeviceIdentity, error) {
	if len(private) != ed25519.PrivateKeySize {
		return nil, errors.New("controlplane: invalid Ed25519 private key")
	}
	return &DeviceIdentity{private: private}, nil
}

// PublicKey returns the raw 32-byte public key.
func (d *DeviceIdentity) PublicKey() ed25519.PublicKey {
	return d.private.Public().(ed25519.PublicKey)
}

// PrivateKey returns the underlying private key.
func (d *DeviceIdentity) PrivateKey() ed25519.PrivateKey { return d.private }

// PublicKeyBase64 returns the base64url (unpadded) raw public key sent in StartPairing.
func (d *DeviceIdentity) PublicKeyBase64() string {
	return base64.RawURLEncoding.EncodeToString(d.PublicKey())
}

// Fingerprint returns the protocol key fingerprint ("SHA256:…").
func (d *DeviceIdentity) Fingerprint() string { return FingerprintRaw(d.PublicKey()) }

// Sign signs message and returns the base64url signature.
func (d *DeviceIdentity) Sign(message []byte) string {
	return base64.RawURLEncoding.EncodeToString(ed25519.Sign(d.private, message))
}

// SignPairingExchange signs the pairing-exchange message for the given code (either display or
// normalized form), pairing ID, and timestamp string (sent verbatim).
func (d *DeviceIdentity) SignPairingExchange(code, pairingID, timestamp string) (string, error) {
	normalized, ok := NormalizeUserCode(code)
	if !ok {
		return "", fmt.Errorf("controlplane: invalid pairing code %q", code)
	}
	return d.Sign([]byte(PairingExchangeMessage(normalized, pairingID, timestamp))), nil
}

const pemTypePrivateKey = "PRIVATE KEY"

// MarshalPEM encodes the private key as a PKCS#8 PEM block.
func (d *DeviceIdentity) MarshalPEM() ([]byte, error) {
	der, err := x509.MarshalPKCS8PrivateKey(d.private)
	if err != nil {
		return nil, err
	}
	return pem.EncodeToMemory(&pem.Block{Type: pemTypePrivateKey, Bytes: der}), nil
}

// ParseDeviceIdentityPEM parses a PKCS#8 PEM-encoded Ed25519 private key.
func ParseDeviceIdentityPEM(data []byte) (*DeviceIdentity, error) {
	block, _ := pem.Decode(data)
	if block == nil || block.Type != pemTypePrivateKey {
		return nil, errors.New("controlplane: device key file is not a PKCS#8 PEM private key")
	}
	key, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("controlplane: parse device key: %w", err)
	}
	priv, ok := key.(ed25519.PrivateKey)
	if !ok {
		return nil, errors.New("controlplane: device key is not Ed25519")
	}
	return &DeviceIdentity{private: priv}, nil
}

// Save writes the private key to path as PKCS#8 PEM with 0600 permissions, creating parent
// directories with 0700. The write is atomic (temp file + rename).
func (d *DeviceIdentity) Save(path string) error {
	data, err := d.MarshalPEM()
	if err != nil {
		return err
	}
	return writeFileAtomic(path, data, 0o600)
}

// LoadDeviceIdentity reads a private key written by Save.
func LoadDeviceIdentity(path string) (*DeviceIdentity, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return ParseDeviceIdentityPEM(data)
}

// LoadOrCreateDeviceIdentity loads the key at path, or generates and saves a new one if the file
// does not exist.
func LoadOrCreateDeviceIdentity(path string) (*DeviceIdentity, error) {
	id, err := LoadDeviceIdentity(path)
	if err == nil {
		return id, nil
	}
	if !errors.Is(err, fs.ErrNotExist) {
		return nil, err
	}
	id, err = GenerateDeviceIdentity()
	if err != nil {
		return nil, err
	}
	if err := id.Save(path); err != nil {
		return nil, err
	}
	return id, nil
}

// writeFileAtomic writes data to a temp file in the target directory with perm, then renames it
// over path.
func writeFileAtomic(path string, data []byte, perm os.FileMode) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer func() {
		if tmpName != "" {
			_ = os.Remove(tmpName)
		}
	}()
	if err := tmp.Chmod(perm); err != nil {
		tmp.Close()
		return err
	}
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpName, path); err != nil {
		return err
	}
	tmpName = ""
	return os.Chmod(path, perm)
}
