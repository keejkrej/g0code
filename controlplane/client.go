package controlplane

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// Protocol error codes returned by the control plane.
const (
	ErrCodeAuthorizationPending = "authorization_pending"
	ErrCodeSlowDown             = "slow_down"
	ErrCodeAccessDenied         = "access_denied"
	ErrCodeExpiredToken         = "expired_token"
	ErrCodeInvalidSignature     = "invalid_signature"
	ErrCodeNotFound             = "not_found"
	ErrCodeInvalidCredential    = "invalid_credential"
	ErrCodeDeviceRevoked        = "device_revoked"
	ErrCodeInvalidToken         = "invalid_token"
	ErrCodeInsufficientScope    = "insufficient_scope"
)

// Sentinel errors. An *APIError matches the sentinel for its Code via errors.Is.
var (
	ErrAuthorizationPending = errors.New("controlplane: authorization_pending")
	ErrSlowDown             = errors.New("controlplane: slow_down")
	ErrAccessDenied         = errors.New("controlplane: access_denied")
	ErrExpiredToken         = errors.New("controlplane: expired_token")
	ErrInvalidSignature     = errors.New("controlplane: invalid_signature")
	ErrNotFound             = errors.New("controlplane: not_found")
	ErrInvalidCredential    = errors.New("controlplane: invalid_credential")
	ErrDeviceRevoked        = errors.New("controlplane: device_revoked")
	ErrInvalidToken         = errors.New("controlplane: invalid_token")
	ErrInsufficientScope    = errors.New("controlplane: insufficient_scope")
)

var codeSentinels = map[string]error{
	ErrCodeAuthorizationPending: ErrAuthorizationPending,
	ErrCodeSlowDown:             ErrSlowDown,
	ErrCodeAccessDenied:         ErrAccessDenied,
	ErrCodeExpiredToken:         ErrExpiredToken,
	ErrCodeInvalidSignature:     ErrInvalidSignature,
	ErrCodeNotFound:             ErrNotFound,
	ErrCodeInvalidCredential:    ErrInvalidCredential,
	ErrCodeDeviceRevoked:        ErrDeviceRevoked,
	ErrCodeInvalidToken:         ErrInvalidToken,
	ErrCodeInsufficientScope:    ErrInsufficientScope,
}

// APIError is a non-2xx control-plane response. Code is the protocol "error" field (empty if the
// body was not a protocol error body); branch on Code, never on Message.
type APIError struct {
	Status  int
	Code    string
	Message string
}

func (e *APIError) Error() string {
	if e.Code == "" {
		return fmt.Sprintf("controlplane: HTTP %d: %s", e.Status, e.Message)
	}
	if e.Message == "" {
		return fmt.Sprintf("controlplane: HTTP %d %s", e.Status, e.Code)
	}
	return fmt.Sprintf("controlplane: HTTP %d %s: %s", e.Status, e.Code, e.Message)
}

// Is lets errors.Is match the sentinel error for e.Code (e.g. ErrDeviceRevoked).
func (e *APIError) Is(target error) bool {
	s, ok := codeSentinels[e.Code]
	return ok && s == target
}

func isDeviceRevoked(err error) bool { return errors.Is(err, ErrDeviceRevoked) }

// Client talks to a control plane. The zero value is not usable; set BaseURL.
type Client struct {
	// BaseURL is the deployment origin (the issuer), e.g. https://cp.example.com.
	BaseURL string
	// HTTPClient defaults to a client with a 30 s timeout.
	HTTPClient *http.Client
	// Discovery, when set (see Discover), supplies endpoint URLs; otherwise the default paths
	// under BaseURL are used.
	Discovery *Discovery
	// UserAgent is sent when non-empty.
	UserAgent string

	// test seams
	now   func() time.Time
	sleep func(ctx context.Context, d time.Duration) error
}

// NewClient returns a client for baseURL.
func NewClient(baseURL string, httpClient *http.Client) *Client {
	return &Client{BaseURL: baseURL, HTTPClient: httpClient}
}

func (c *Client) httpClient() *http.Client {
	if c.HTTPClient != nil {
		return c.HTTPClient
	}
	return &http.Client{Timeout: 30 * time.Second}
}

func (c *Client) clock() time.Time {
	if c.now != nil {
		return c.now()
	}
	return time.Now()
}

func (c *Client) wait(ctx context.Context, d time.Duration) error {
	if c.sleep != nil {
		return c.sleep(ctx, d)
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

func (c *Client) base() string { return strings.TrimRight(c.BaseURL, "/") }

func (c *Client) endpoint(fromDiscovery func(*Discovery) string, defaultPath string) string {
	if c.Discovery != nil {
		if u := fromDiscovery(c.Discovery); u != "" {
			return strings.TrimRight(u, "/")
		}
	}
	return c.base() + defaultPath
}

func (c *Client) pairingsURL() string {
	return c.endpoint(func(d *Discovery) string { return d.DevicePairingEndpoint }, "/v1/device-pairings")
}

func (c *Client) clientAuthURL() string {
	return c.endpoint(func(d *Discovery) string { return d.ClientAuthorizationEndpoint }, "/v1/client-authorizations")
}

func (c *Client) clientTokenURL() string {
	return c.endpoint(func(d *Discovery) string { return d.ClientTokenEndpoint }, "/v1/client-authorizations/token")
}

func (c *Client) devicesURL() string {
	return c.endpoint(func(d *Discovery) string { return d.DevicesEndpoint }, "/v1/devices")
}

// do sends a JSON request and decodes a 2xx JSON response into out (if non-nil). Non-2xx
// responses become *APIError.
func (c *Client) do(ctx context.Context, method, rawURL, bearer string, in, out any) error {
	var body io.Reader
	if in != nil {
		data, err := json.Marshal(in)
		if err != nil {
			return fmt.Errorf("controlplane: encode request: %w", err)
		}
		body = bytes.NewReader(data)
	}
	req, err := http.NewRequestWithContext(ctx, method, rawURL, body)
	if err != nil {
		return fmt.Errorf("controlplane: build request: %w", err)
	}
	req.Header.Set("Accept", "application/json")
	if in != nil {
		req.Header.Set("Content-Type", "application/json; charset=utf-8")
	}
	if bearer != "" {
		req.Header.Set("Authorization", "Bearer "+bearer)
	}
	if c.UserAgent != "" {
		req.Header.Set("User-Agent", c.UserAgent)
	}
	resp, err := c.httpClient().Do(req)
	if err != nil {
		return fmt.Errorf("controlplane: %s %s: %w", method, rawURL, err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return fmt.Errorf("controlplane: read response: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		apiErr := &APIError{Status: resp.StatusCode}
		var eb ErrorBody
		if json.Unmarshal(data, &eb) == nil && eb.Error != "" {
			apiErr.Code, apiErr.Message = eb.Error, eb.Message
		} else {
			apiErr.Message = strings.TrimSpace(string(data))
			if len(apiErr.Message) > 200 {
				apiErr.Message = apiErr.Message[:200]
			}
			if apiErr.Message == "" {
				apiErr.Message = http.StatusText(resp.StatusCode)
			}
		}
		return apiErr
	}
	if out == nil || len(bytes.TrimSpace(data)) == 0 {
		return nil
	}
	if err := json.Unmarshal(data, out); err != nil {
		return fmt.Errorf("controlplane: decode response: %w", err)
	}
	return nil
}

// Discover fetches the discovery document, stores it in c.Discovery, and returns it.
func (c *Client) Discover(ctx context.Context) (*Discovery, error) {
	var d Discovery
	if err := c.do(ctx, http.MethodGet, c.base()+WellKnownPath, "", nil, &d); err != nil {
		return nil, err
	}
	if d.ProtocolVersion != ProtocolVersion {
		return nil, fmt.Errorf("controlplane: unsupported protocol version %d", d.ProtocolVersion)
	}
	c.Discovery = &d
	return &d, nil
}

// ---------------------------------------------------------------------------------------------
// Device side
// ---------------------------------------------------------------------------------------------

// StartPairing begins device pairing (POST /v1/device-pairings). If req.PublicKey is empty and
// identity is non-nil, the identity's public key is used.
func (c *Client) StartPairing(ctx context.Context, identity *DeviceIdentity, req PairingRequest) (*PairingStart, error) {
	if req.PublicKey == "" && identity != nil {
		req.PublicKey = identity.PublicKeyBase64()
	}
	if req.Capabilities == nil {
		req.Capabilities = []string{}
	}
	var out PairingStart
	if err := c.do(ctx, http.MethodPost, c.pairingsURL(), "", req, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// PairingStatus returns GET /v1/device-pairings/:code.
func (c *Client) PairingStatus(ctx context.Context, code string) (*PairingStatus, error) {
	normalized, ok := NormalizeUserCode(code)
	if !ok {
		return nil, fmt.Errorf("controlplane: invalid pairing code %q", code)
	}
	var out PairingStatus
	if err := c.do(ctx, http.MethodGet, c.pairingsURL()+"/"+url.PathEscape(normalized), "", nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// ExchangePairing performs one exchange attempt, signing the protocol message with identity at
// the current time. Pending/slow-down/denied/expired outcomes are returned as *APIError (match
// with errors.Is against ErrAuthorizationPending etc.).
func (c *Client) ExchangePairing(ctx context.Context, identity *DeviceIdentity, code, pairingID string) (*PairingResult, error) {
	normalized, ok := NormalizeUserCode(code)
	if !ok {
		return nil, fmt.Errorf("controlplane: invalid pairing code %q", code)
	}
	timestamp := FormatTimestamp(c.clock())
	signature := identity.Sign([]byte(PairingExchangeMessage(normalized, pairingID, timestamp)))
	var out PairingResult
	u := c.pairingsURL() + "/" + url.PathEscape(normalized) + "/exchange"
	if err := c.do(ctx, http.MethodPost, u, "", PairingExchangeRequest{Timestamp: timestamp, Signature: signature}, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// pollDeviceGrant runs an RFC 8628-style polling loop: wait interval, attempt, keep going on
// authorization_pending, add 5 s on slow_down, and stop on anything else.
func pollDeviceGrant[T any](ctx context.Context, c *Client, intervalSeconds int, attempt func() (T, error)) (T, error) {
	var zero T
	interval := time.Duration(intervalSeconds) * time.Second
	if interval <= 0 {
		interval = PollIntervalSeconds * time.Second
	}
	for {
		if err := c.wait(ctx, interval); err != nil {
			return zero, err
		}
		result, err := attempt()
		switch {
		case err == nil:
			return result, nil
		case errors.Is(err, ErrAuthorizationPending):
			continue
		case errors.Is(err, ErrSlowDown):
			interval += SlowDownIncrementSeconds * time.Second
			continue
		default:
			return zero, err
		}
	}
}

// PollPairing polls the exchange endpoint every start.Interval seconds until the user approves
// (returns the credential), denies (ErrAccessDenied), or the code expires (ErrExpiredToken).
// slow_down adds 5 s to the interval. Cancel ctx to stop.
func (c *Client) PollPairing(ctx context.Context, identity *DeviceIdentity, start *PairingStart) (*PairingResult, error) {
	return pollDeviceGrant(ctx, c, start.Interval, func() (*PairingResult, error) {
		return c.ExchangePairing(ctx, identity, start.Code, start.PairingID)
	})
}

// Heartbeat reports liveness and current addresses. A 403 device_revoked response is returned as
// an error matching ErrDeviceRevoked; the device must then stop accepting all connect tokens,
// delete its credential, and require re-pairing.
func (c *Client) Heartbeat(ctx context.Context, deviceID, credential string, req HeartbeatRequest) (*HeartbeatResponse, error) {
	var out HeartbeatResponse
	u := c.devicesURL() + "/" + url.PathEscape(deviceID) + "/heartbeat"
	if err := c.do(ctx, http.MethodPost, u, credential, req, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// ---------------------------------------------------------------------------------------------
// GUI side
// ---------------------------------------------------------------------------------------------

// StartClientAuthorization begins the GUI device-code flow (POST /v1/client-authorizations).
func (c *Client) StartClientAuthorization(ctx context.Context, req ClientAuthorizationRequest) (*ClientAuthorization, error) {
	var out ClientAuthorization
	if err := c.do(ctx, http.MethodPost, c.clientAuthURL(), "", req, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// RequestClientToken performs one token request (POST /v1/client-authorizations/token).
func (c *Client) RequestClientToken(ctx context.Context, deviceCode string) (*ClientToken, error) {
	var out ClientToken
	if err := c.do(ctx, http.MethodPost, c.clientTokenURL(), "", map[string]string{"deviceCode": deviceCode}, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// PollClientToken polls the token endpoint every auth.Interval seconds until approval, denial
// (ErrAccessDenied), or expiry (ErrExpiredToken), honoring slow_down.
func (c *Client) PollClientToken(ctx context.Context, auth *ClientAuthorization) (*ClientToken, error) {
	return pollDeviceGrant(ctx, c, auth.Interval, func() (*ClientToken, error) {
		return c.RequestClientToken(ctx, auth.DeviceCode)
	})
}

// Me returns GET /v1/me for a client token.
func (c *Client) Me(ctx context.Context, clientToken string) (*Me, error) {
	var out Me
	if err := c.do(ctx, http.MethodGet, c.base()+"/v1/me", clientToken, nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// ListDevices returns GET /v1/devices.
func (c *Client) ListDevices(ctx context.Context, clientToken string) ([]Device, error) {
	var out DeviceList
	if err := c.do(ctx, http.MethodGet, c.devicesURL(), clientToken, nil, &out); err != nil {
		return nil, err
	}
	return out.Devices, nil
}

// MintConnectToken requests a connect token for deviceID. Empty scopes lets the server default to
// orchestration:read. A revoked device returns an error matching ErrDeviceRevoked (409).
func (c *Client) MintConnectToken(ctx context.Context, clientToken, deviceID string, scopes ...Scope) (*ConnectToken, error) {
	var out ConnectToken
	u := c.devicesURL() + "/" + url.PathEscape(deviceID) + "/connect-tokens"
	if err := c.do(ctx, http.MethodPost, u, clientToken, ConnectTokenRequest{Scopes: scopes}, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// RevokeDevice revokes a device (POST /v1/devices/:deviceId/revoke) and returns it.
func (c *Client) RevokeDevice(ctx context.Context, clientToken, deviceID string) (*Device, error) {
	var out Device
	u := c.devicesURL() + "/" + url.PathEscape(deviceID) + "/revoke"
	if err := c.do(ctx, http.MethodPost, u, clientToken, struct{}{}, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// RevokeClientToken revokes the presenting client token (POST /v1/client-tokens/revoke → 204).
func (c *Client) RevokeClientToken(ctx context.Context, clientToken string) error {
	return c.do(ctx, http.MethodPost, c.base()+"/v1/client-tokens/revoke", clientToken, nil, nil)
}
