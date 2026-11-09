package controlplane

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
)

// DeviceState is what a paired device persists next to its private key.
type DeviceState struct {
	DeviceID         string `json:"deviceId"`
	DeviceCredential string `json:"deviceCredential"`
	OwnerUserID      string `json:"ownerUserId,omitempty"`
	Issuer           string `json:"issuer"`
	JWKSURI          string `json:"jwksUri,omitempty"`
	RevocationEpoch  int64  `json:"revocationEpoch"`
	// ControlPlaneURL is the base URL the device was paired against.
	ControlPlaneURL string `json:"controlPlaneUrl"`
}

// NewDeviceState builds the persisted state from a successful pairing exchange.
func NewDeviceState(controlPlaneURL string, result *PairingResult) *DeviceState {
	return &DeviceState{
		DeviceID:         result.DeviceID,
		DeviceCredential: result.DeviceCredential,
		OwnerUserID:      result.OwnerUserID,
		Issuer:           result.Issuer,
		JWKSURI:          result.JWKSURI,
		RevocationEpoch:  result.RevocationEpoch,
		ControlPlaneURL:  controlPlaneURL,
	}
}

// Validate checks that the fields needed to verify tokens and heartbeat are present.
func (s *DeviceState) Validate() error {
	switch {
	case s.DeviceID == "":
		return errors.New("controlplane: device state missing deviceId")
	case s.DeviceCredential == "":
		return errors.New("controlplane: device state missing deviceCredential")
	case s.Issuer == "":
		return errors.New("controlplane: device state missing issuer")
	case s.ControlPlaneURL == "":
		return errors.New("controlplane: device state missing controlPlaneUrl")
	}
	return nil
}

// LoadDeviceState reads and validates a state file written by Save.
func LoadDeviceState(path string) (*DeviceState, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var s DeviceState
	if err := json.Unmarshal(data, &s); err != nil {
		return nil, fmt.Errorf("controlplane: decode device state: %w", err)
	}
	if err := s.Validate(); err != nil {
		return nil, err
	}
	return &s, nil
}

// Save writes the state as indented JSON with 0600 permissions (atomic temp file + rename).
func (s *DeviceState) Save(path string) error {
	data, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return err
	}
	return writeFileAtomic(path, append(data, '\n'), 0o600)
}

// DeleteDeviceState removes the state file (for example after device_revoked). A missing file is
// not an error.
func DeleteDeviceState(path string) error {
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}
