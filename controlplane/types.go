package controlplane

import "time"

// Discovery is the document at GET /.well-known/t3code-control-plane.
type Discovery struct {
	ProtocolVersion             int      `json:"protocolVersion"`
	Issuer                      string   `json:"issuer"`
	JWKSURI                     string   `json:"jwksUri"`
	DevicePairingEndpoint       string   `json:"devicePairingEndpoint"`
	ClientAuthorizationEndpoint string   `json:"clientAuthorizationEndpoint"`
	ClientTokenEndpoint         string   `json:"clientTokenEndpoint"`
	DevicesEndpoint             string   `json:"devicesEndpoint"`
	ScopesSupported             []string `json:"scopesSupported"`
	ConnectTokenTTLSeconds      int      `json:"connectTokenTtlSeconds"`
	HeartbeatIntervalSeconds    int      `json:"heartbeatIntervalSeconds"`
}

// ClientInfo identifies an implementation ({"kind": "g0code", "version": "0.1.0"}).
type ClientInfo struct {
	Kind    string `json:"kind"`
	Version string `json:"version"`
}

// TailscaleInfo is a device's Tailscale address. At least one field must be set when pairing.
type TailscaleInfo struct {
	IP           string `json:"ip,omitempty"`
	MagicDNSName string `json:"magicDnsName,omitempty"`
}

// PairingRequest is the body of POST /v1/device-pairings.
type PairingRequest struct {
	PublicKey    string        `json:"publicKey"`
	Name         string        `json:"name,omitempty"`
	Hostname     string        `json:"hostname"`
	Platform     string        `json:"platform"`
	Arch         string        `json:"arch"`
	Client       ClientInfo    `json:"client"`
	Tailscale    TailscaleInfo `json:"tailscale"`
	Port         int           `json:"port"`
	Capabilities []string      `json:"capabilities"`
}

// PairingStart is the 201 response of POST /v1/device-pairings.
type PairingStart struct {
	PairingID               string    `json:"pairingId"`
	Code                    string    `json:"code"`
	VerificationURI         string    `json:"verificationUri"`
	VerificationURIComplete string    `json:"verificationUriComplete"`
	ExpiresAt               time.Time `json:"expiresAt"`
	Interval                int       `json:"interval"`
}

// PairingExchangeRequest is the body of POST /v1/device-pairings/:code/exchange.
type PairingExchangeRequest struct {
	Timestamp string `json:"timestamp"`
	Signature string `json:"signature"`
}

// PairingResult is the 200 response of a successful pairing exchange.
type PairingResult struct {
	DeviceID                 string `json:"deviceId"`
	DeviceCredential         string `json:"deviceCredential"`
	OwnerUserID              string `json:"ownerUserId"`
	Issuer                   string `json:"issuer"`
	JWKSURI                  string `json:"jwksUri"`
	RevocationEpoch          int64  `json:"revocationEpoch"`
	HeartbeatIntervalSeconds int    `json:"heartbeatIntervalSeconds"`
}

// PairingStatus is the response of GET /v1/device-pairings/:code.
type PairingStatus struct {
	PairingID string    `json:"pairingId"`
	Status    string    `json:"status"`
	ExpiresAt time.Time `json:"expiresAt"`
}

// HeartbeatRequest is the body of POST /v1/devices/:deviceId/heartbeat. All fields are optional;
// present fields replace stored values.
type HeartbeatRequest struct {
	Tailscale    *TailscaleInfo `json:"tailscale,omitempty"`
	Port         int            `json:"port,omitempty"`
	Client       *ClientInfo    `json:"client,omitempty"`
	Capabilities []string       `json:"capabilities,omitzero"`
}

// HeartbeatResponse is the 200 response of a heartbeat.
type HeartbeatResponse struct {
	DeviceID                 string    `json:"deviceId"`
	Status                   string    `json:"status"`
	RevocationEpoch          int64     `json:"revocationEpoch"`
	Issuer                   string    `json:"issuer"`
	JWKSURI                  string    `json:"jwksUri"`
	HeartbeatIntervalSeconds int       `json:"heartbeatIntervalSeconds"`
	ServerTime               time.Time `json:"serverTime"`
}

// ClientAuthorizationRequest is the body of POST /v1/client-authorizations. Scopes defaults to
// both scopes on the server when empty.
type ClientAuthorizationRequest struct {
	ClientName string     `json:"clientName"`
	Client     ClientInfo `json:"client"`
	Scopes     []Scope    `json:"scopes,omitempty"`
}

// ClientAuthorization is the 201 response of POST /v1/client-authorizations.
type ClientAuthorization struct {
	AuthorizationID         string    `json:"authorizationId"`
	DeviceCode              string    `json:"deviceCode"`
	UserCode                string    `json:"userCode"`
	VerificationURI         string    `json:"verificationUri"`
	VerificationURIComplete string    `json:"verificationUriComplete"`
	ExpiresAt               time.Time `json:"expiresAt"`
	Interval                int       `json:"interval"`
}

// ClientToken is the success response of POST /v1/client-authorizations/token.
type ClientToken struct {
	AccessToken string    `json:"accessToken"`
	TokenType   string    `json:"tokenType"`
	Scope       string    `json:"scope"`
	UserID      string    `json:"userId"`
	ExpiresAt   time.Time `json:"expiresAt"`
}

// User is the user object in GET /v1/me.
type User struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	Username  string `json:"username"`
	Email     string `json:"email"`
	AvatarURL string `json:"avatarUrl"`
}

// Me is the response of GET /v1/me.
type Me struct {
	User  User   `json:"user"`
	Scope string `json:"scope"`
}

// Device is a device entry in GET /v1/devices.
type Device struct {
	ID                   string        `json:"id"`
	Name                 string        `json:"name"`
	Hostname             string        `json:"hostname"`
	Platform             string        `json:"platform"`
	Arch                 string        `json:"arch"`
	Client               ClientInfo    `json:"client"`
	Tailscale            TailscaleInfo `json:"tailscale"`
	Port                 int           `json:"port"`
	Endpoint             string        `json:"endpoint,omitempty"`
	Capabilities         []string      `json:"capabilities"`
	PublicKeyFingerprint string        `json:"publicKeyFingerprint"`
	Status               string        `json:"status"`
	Online               bool          `json:"online"`
	RevocationEpoch      int64         `json:"revocationEpoch"`
	CreatedAt            time.Time     `json:"createdAt"`
	LastSeenAt           *time.Time    `json:"lastSeenAt,omitempty"`
}

// DeviceList is the response of GET /v1/devices.
type DeviceList struct {
	Devices []Device `json:"devices"`
}

// ConnectTokenRequest is the body of POST /v1/devices/:deviceId/connect-tokens. Scopes defaults
// to orchestration:read on the server when empty.
type ConnectTokenRequest struct {
	Scopes []Scope `json:"scopes,omitempty"`
}

// ConnectToken is the 201 response of POST /v1/devices/:deviceId/connect-tokens.
type ConnectToken struct {
	Token     string    `json:"token"`
	TokenType string    `json:"tokenType"`
	Scope     string    `json:"scope"`
	ExpiresIn int       `json:"expiresIn"`
	ExpiresAt time.Time `json:"expiresAt"`
	DeviceID  string    `json:"deviceId"`
	Endpoint  string    `json:"endpoint"`
}
