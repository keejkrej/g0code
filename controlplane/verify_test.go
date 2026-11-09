package controlplane

import (
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"testing"
	"time"
)

// testSigner crafts connect tokens with arbitrary headers and raw JSON payloads.
type testSigner struct {
	priv ed25519.PrivateKey
	kid  string
	x    string
}

func newTestSigner(seedByte byte) *testSigner {
	seed := make([]byte, ed25519.SeedSize)
	for i := range seed {
		seed[i] = seedByte
	}
	priv := ed25519.NewKeyFromSeed(seed)
	x := base64.RawURLEncoding.EncodeToString(priv.Public().(ed25519.PublicKey))
	return &testSigner{priv: priv, kid: JWKThumbprint(x), x: x}
}

func (s *testSigner) jwk() JWK {
	return JWK{Kty: "OKP", Crv: "Ed25519", X: s.x, Kid: s.kid, Alg: "EdDSA", Use: "sig"}
}

func (s *testSigner) signRaw(header, payload string) string {
	enc := base64.RawURLEncoding
	signing := enc.EncodeToString([]byte(header)) + "." + enc.EncodeToString([]byte(payload))
	return signing + "." + enc.EncodeToString(ed25519.Sign(s.priv, []byte(signing)))
}

func (s *testSigner) sign(claims map[string]any) string {
	h, _ := json.Marshal(map[string]any{"alg": "EdDSA", "typ": "JWT", "kid": s.kid})
	p, _ := json.Marshal(claims)
	return s.signRaw(string(h), string(p))
}

const (
	tIssuer = "https://cp.test"
	tDevice = "dev_test"
	tNow    = int64(1_800_000_000)
)

func baseClaims(over map[string]any) map[string]any {
	c := map[string]any{
		"iss": tIssuer, "sub": "usr_test", "aud": tDevice,
		"iat": tNow, "nbf": tNow, "exp": tNow + 300,
		"jti": "ctk_test", "scope": "orchestration:read orchestration:write",
		"t3cp_epoch": 2, "t3cp_ver": 1,
	}
	for k, v := range over {
		if v == nil {
			delete(c, k)
		} else {
			c[k] = v
		}
	}
	return c
}

func testCtx(s *testSigner) VerificationContext {
	return VerificationContext{
		Issuer: tIssuer, DeviceID: tDevice, RevocationEpoch: 2,
		JWKS: JWKS{Keys: []JWK{s.jwk()}}, Now: time.Unix(tNow, 0),
	}
}

func expectCode(t *testing.T, err error, want VerifyErrorCode) {
	t.Helper()
	verr, ok := err.(*VerifyError)
	if !ok {
		t.Fatalf("got %v (%T), want %s", err, err, want)
	}
	if verr.Code != want {
		t.Fatalf("code = %s (%s), want %s", verr.Code, verr.Reason, want)
	}
}

func TestVerifyExtraCases(t *testing.T) {
	s := newTestSigner(7)
	h := func(m map[string]any) string { b, _ := json.Marshal(m); return string(b) }
	p := func(over map[string]any) string { return h(baseClaims(over)) }
	hdr := h(map[string]any{"alg": "EdDSA", "typ": "JWT", "kid": s.kid})

	cases := []struct {
		name     string
		token    string
		required Scope
		want     VerifyErrorCode // "" = valid
	}{
		{"valid", s.signRaw(hdr, p(nil)), ScopeWrite, ""},
		{"no scope needed, empty scope", s.signRaw(hdr, p(map[string]any{"scope": ""})), "", ""},
		{"integral float time accepted", s.signRaw(hdr, `{"iss":"https://cp.test","aud":"dev_test","iat":1800000000.0,"nbf":1800000000,"exp":1.8000003e9,"scope":"orchestration:read","t3cp_epoch":2,"t3cp_ver":1.0}`), ScopeRead, ""},
		{"fractional exp", s.signRaw(hdr, p(map[string]any{"exp": float64(tNow) + 300.5})), ScopeRead, CodeInvalidToken},
		{"string exp", s.signRaw(hdr, p(map[string]any{"exp": "1800000300"})), ScopeRead, CodeInvalidToken},
		{"missing nbf", s.signRaw(hdr, p(map[string]any{"nbf": nil})), ScopeRead, CodeInvalidToken},
		{"missing iat", s.signRaw(hdr, p(map[string]any{"iat": nil})), ScopeRead, CodeInvalidToken},
		{"huge exp", s.signRaw(hdr, p(map[string]any{"exp": 1e300})), ScopeRead, CodeInvalidToken},
		{"string epoch", s.signRaw(hdr, p(map[string]any{"t3cp_epoch": "2"})), ScopeRead, CodeInvalidToken},
		{"string ver", s.signRaw(hdr, p(map[string]any{"t3cp_ver": "1"})), ScopeRead, CodeInvalidToken},
		{"missing ver", s.signRaw(hdr, p(map[string]any{"t3cp_ver": nil})), ScopeRead, CodeInvalidToken},
		{"missing iss", s.signRaw(hdr, p(map[string]any{"iss": nil})), ScopeRead, CodeInvalidToken},
		{"missing aud", s.signRaw(hdr, p(map[string]any{"aud": nil})), ScopeRead, CodeWrongAudience},
		{"numeric aud", s.signRaw(hdr, p(map[string]any{"aud": 5})), ScopeRead, CodeWrongAudience},
		{"empty aud array", s.signRaw(hdr, p(map[string]any{"aud": []string{}})), ScopeRead, CodeWrongAudience},
		{"non-string scope", s.signRaw(hdr, p(map[string]any{"scope": []string{"orchestration:write"}})), ScopeRead, CodeInsufficientScope},
		{"unknown scope only", s.signRaw(hdr, p(map[string]any{"scope": "admin"})), ScopeRead, CodeInsufficientScope},
		{"double spaces in scope", s.signRaw(hdr, p(map[string]any{"scope": "  orchestration:write  "})), ScopeWrite, ""},
		{"null typ", s.signRaw(h(map[string]any{"alg": "EdDSA", "typ": nil, "kid": s.kid}), p(nil)), ScopeRead, CodeInvalidToken},
		{"numeric kid", s.signRaw(h(map[string]any{"alg": "EdDSA", "kid": 1}), p(nil)), ScopeRead, CodeInvalidToken},
		{"lowercase alg", s.signRaw(h(map[string]any{"alg": "eddsa", "kid": s.kid}), p(nil)), ScopeRead, CodeInvalidToken},
		{"header array", s.signRaw(`["EdDSA"]`, p(nil)), ScopeRead, CodeInvalidToken},
		{"header null", s.signRaw(`null`, p(nil)), ScopeRead, CodeInvalidToken},
		{"payload array", s.signRaw(hdr, `[1]`), ScopeRead, CodeInvalidToken},
		{"payload null", s.signRaw(hdr, `null`), ScopeRead, CodeInvalidToken},
		{"payload trailing garbage", s.signRaw(hdr, p(nil)+"x"), ScopeRead, CodeInvalidToken},
		{"padded signature", s.signRaw(hdr, p(nil)) + "==", ScopeRead, CodeInvalidToken},
		{"four segments", s.signRaw(hdr, p(nil)) + ".x", ScopeRead, CodeInvalidToken},
		{"empty", "", ScopeRead, CodeInvalidToken},
		{"wrong aud beats missing exp", s.signRaw(hdr, p(map[string]any{"aud": "dev_other", "exp": nil})), ScopeRead, CodeWrongAudience},
		{"stale epoch beats revoked", s.signRaw(hdr, p(map[string]any{"t3cp_epoch": 1})), ScopeRead, CodeStaleEpoch},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			claims, err := VerifyConnectToken(c.token, testCtx(s), c.required)
			if c.want == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if claims.Subject == "" && c.name == "valid" {
					t.Fatal("missing subject")
				}
				return
			}
			expectCode(t, err, c.want)
		})
	}
}

func TestVerifyRevokedAndWrongKeyInJWKS(t *testing.T) {
	s := newTestSigner(7)
	tok := s.sign(baseClaims(nil))
	ctx := testCtx(s)
	ctx.Revoked = true
	_, err := VerifyConnectToken(tok, ctx, ScopeRead)
	expectCode(t, err, CodeDeviceRevoked)

	// A JWKS entry with the right kid but a non-Ed25519 curve must not be used.
	ctx = testCtx(s)
	ctx.JWKS.Keys[0].Crv = "X25519"
	_, err = VerifyConnectToken(tok, ctx, ScopeRead)
	expectCode(t, err, CodeInvalidToken)

	// A JWKS x that is not 32 bytes is rejected rather than panicking.
	ctx = testCtx(s)
	ctx.JWKS.Keys[0].X = "AAAA"
	_, err = VerifyConnectToken(tok, ctx, ScopeRead)
	expectCode(t, err, CodeInvalidToken)
}

func TestVerifyErrorStatus(t *testing.T) {
	if (&VerifyError{Code: CodeInsufficientScope}).HTTPStatus() != 403 {
		t.Fatal("insufficient_scope should be 403")
	}
	for _, c := range []VerifyErrorCode{CodeInvalidToken, CodeTokenExpired, CodeWrongAudience, CodeStaleEpoch, CodeDeviceRevoked} {
		if (&VerifyError{Code: c}).HTTPStatus() != 401 {
			t.Fatalf("%s should be 401", c)
		}
	}
}

func TestRequiredScopeForMethod(t *testing.T) {
	for _, m := range []string{"initialize", "tools/list", "ping", "notifications/initialized", "notifications/cancelled"} {
		if _, needs := RequiredScopeForMethod(m, ""); needs {
			t.Errorf("%s should not need a scope", m)
		}
	}
	if s, needs := RequiredScopeForMethod("tools/call", "t3_thread_read"); !needs || s != ScopeRead {
		t.Errorf("t3_thread_read => %s,%v", s, needs)
	}
	if s, needs := RequiredScopeForMethod("tools/call", "t3_thread_send"); !needs || s != ScopeWrite {
		t.Errorf("t3_thread_send => %s,%v", s, needs)
	}
	if s, needs := RequiredScopeForMethod("tools/call", "brand_new_tool"); !needs || s != ScopeWrite {
		t.Errorf("unknown tool => %s,%v", s, needs)
	}
	if s, needs := RequiredScopeForMethod("shutdown", ""); !needs || s != ScopeWrite {
		t.Errorf("unknown method => %s,%v", s, needs)
	}
}

func TestNormalizeAndFormat(t *testing.T) {
	if _, ok := NormalizeUserCode("bcdf\u00a0ghjk"); !ok {
		t.Error("non-ASCII separators should be dropped")
	}
	if _, ok := NormalizeUserCode("BCDF-GHJ\u212a"); ok { // KELVIN SIGN must not fold to K
		t.Error("non-ASCII letters must not be accepted")
	}
	if FormatUserCode("ABC") != "ABC" {
		t.Error("short input should pass through")
	}
	if FormatTimestamp(time.Date(2026, 11, 9, 20, 0, 0, 123456789, time.FixedZone("x", 3600))) != "2026-11-09T19:00:00.123Z" {
		t.Error("timestamp format")
	}
}
