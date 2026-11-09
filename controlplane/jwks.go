package controlplane

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"
)

// JWKSCache fetches and caches the control plane's JWKS from URI. Fetches (including refetches
// triggered by an unknown kid) happen at most once per MinRefetchInterval; the very first fetch is
// never delayed.
type JWKSCache struct {
	// URI is the jwksUri from discovery, pairing, or heartbeat.
	URI string
	// HTTPClient is used for fetches. Nil means a client with a 10 s timeout.
	HTTPClient *http.Client
	// MinRefetchInterval defaults to 60 s.
	MinRefetchInterval time.Duration

	now func() time.Time // test seam; nil means time.Now

	fetchMu   sync.Mutex // serializes fetches
	mu        sync.RWMutex
	keys      JWKS
	fetched   bool
	lastFetch time.Time
}

// NewJWKSCache returns a cache for uri.
func NewJWKSCache(uri string, client *http.Client) *JWKSCache {
	return &JWKSCache{URI: uri, HTTPClient: client}
}

// NewStaticJWKSCache returns a cache preloaded with keys. If URI is empty it never fetches.
func NewStaticJWKSCache(keys JWKS) *JWKSCache {
	c := &JWKSCache{}
	c.keys = keys
	c.fetched = true
	return c
}

func (c *JWKSCache) clock() time.Time {
	if c.now != nil {
		return c.now()
	}
	return time.Now()
}

func (c *JWKSCache) interval() time.Duration {
	if c.MinRefetchInterval > 0 {
		return c.MinRefetchInterval
	}
	return JWKSRefetchMinIntervalSec * time.Second
}

// Keys returns the current cached key set.
func (c *JWKSCache) Keys() JWKS {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return JWKS{Keys: append([]JWK(nil), c.keys.Keys...)}
}

// SetURI changes the JWKS location (for example after a heartbeat reports a new jwksUri).
func (c *JWKSCache) SetURI(uri string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.URI = uri
}

func (c *JWKSCache) uri() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.URI
}

// Ensure makes sure the key set has been loaded and, if kid is non-empty and unknown, refetches
// it when the rate limit allows. It returns the key set to verify against. Fetch errors are
// returned alongside the (possibly stale) cached keys, so callers may still verify.
func (c *JWKSCache) Ensure(ctx context.Context, kid string) (JWKS, error) {
	c.mu.RLock()
	fetched := c.fetched
	known := kid != "" && c.keys.Has(kid)
	c.mu.RUnlock()
	if fetched && (kid == "" || known) {
		return c.Keys(), nil
	}
	if c.uri() == "" {
		return c.Keys(), nil
	}

	c.fetchMu.Lock()
	defer c.fetchMu.Unlock()

	// Re-check under the fetch lock: another goroutine may have just refreshed.
	c.mu.RLock()
	fetched = c.fetched
	known = kid != "" && c.keys.Has(kid)
	last := c.lastFetch
	c.mu.RUnlock()
	if fetched && (kid == "" || known) {
		return c.Keys(), nil
	}
	if !last.IsZero() && c.clock().Sub(last) < c.interval() {
		return c.Keys(), nil
	}
	err := c.refresh(ctx)
	return c.Keys(), err
}

// Refresh fetches the JWKS now, ignoring the rate limit.
func (c *JWKSCache) Refresh(ctx context.Context) error {
	c.fetchMu.Lock()
	defer c.fetchMu.Unlock()
	return c.refresh(ctx)
}

func (c *JWKSCache) refresh(ctx context.Context) error {
	uri := c.uri()
	c.mu.Lock()
	c.lastFetch = c.clock()
	c.mu.Unlock()

	client := c.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 10 * time.Second}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, uri, nil)
	if err != nil {
		return fmt.Errorf("controlplane: jwks request: %w", err)
	}
	req.Header.Set("Accept", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("controlplane: fetch jwks: %w", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return fmt.Errorf("controlplane: read jwks: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("controlplane: fetch jwks: HTTP %d", resp.StatusCode)
	}
	var set JWKS
	if err := json.Unmarshal(body, &set); err != nil {
		return fmt.Errorf("controlplane: decode jwks: %w", err)
	}
	c.mu.Lock()
	c.keys = set
	c.fetched = true
	c.mu.Unlock()
	return nil
}
