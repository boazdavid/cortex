package contextguru

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/rossoctl/context-guru/keepalive"
	"github.com/rossoctl/context-guru/keepalive/keepalivetest"
)

func TestCortexDispatcherMeetsTheContract(t *testing.T) {
	keepalivetest.RunDispatcherContract(t, func(t *testing.T, upstreamURL string) keepalivetest.Harness {
		u, err := url.Parse(upstreamURL)
		if err != nil {
			t.Fatal(err)
		}
		d := newCortexDispatcher(http.DefaultClient)
		return keepalivetest.Harness{D: d, Hold: func(k keepalive.Key, h http.Header) {
			d.hold(k, u.Scheme, u.Host, h)
		}}
	})
}

func TestPingUsageReadsBothDialects(t *testing.T) {
	anth := pingUsage([]byte(`{"usage":{"input_tokens":3,"cache_read_input_tokens":48000,` +
		`"cache_creation_input_tokens":0,"output_tokens":1}}`))
	if anth.FreshInput != 3 || anth.CacheRead != 48000 || anth.CacheWrite != 0 || anth.Output != 1 {
		t.Errorf("anthropic usage = %+v", anth)
	}
	// Responses: input_tokens INCLUDES the cached part; fresh is the difference.
	resp := pingUsage([]byte(`{"usage":{"input_tokens":48003,` +
		`"input_tokens_details":{"cached_tokens":48000},"output_tokens":16}}`))
	if resp.FreshInput != 3 || resp.CacheRead != 48000 || resp.Output != 16 {
		t.Errorf("responses usage = %+v", resp)
	}
}

func TestDispatchSetsTheSentinel(t *testing.T) {
	// A ping that transits this proxy must short-circuit OnRequest, not be recorded again.
	var got http.Header
	srv := newHeaderSink(t, &got)
	u, _ := url.Parse(srv.URL)
	d := newCortexDispatcher(http.DefaultClient)
	key := keepalive.Key{Tenant: "", Session: "s"}
	d.hold(key, u.Scheme, u.Host, http.Header{"Authorization": {"Bearer k"}})
	if _, _, err := d.Dispatch(t.Context(), keepalive.DispatchRequest{Key: key, Route: "/v1/messages",
		Body: []byte(`{}`), Headers: http.Header{}}); err != nil {
		t.Fatal(err)
	}
	if got.Get(sentinelHeader) == "" {
		t.Fatalf("ping carried no %s header", sentinelHeader)
	}
}

func newHeaderSink(t *testing.T, got *http.Header) *httptest.Server {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		*got = r.Header.Clone()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"usage":{"input_tokens":1,"output_tokens":1}}`))
	}))
	t.Cleanup(srv.Close)
	return srv
}

func TestPingClientBypassesTheProxy(t *testing.T) {
	c := newPingClient()
	tr, ok := c.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("ping client transport is %T, want *http.Transport", c.Transport)
	}
	if tr.Proxy != nil {
		t.Fatal("ping client routes through a proxy; a ping sent back into Cortex would be recorded as agent traffic")
	}
	if tr.IdleConnTimeout <= 0 {
		t.Fatal("ping client has no IdleConnTimeout; idle connections are never reclaimed")
	}
	if c.CheckRedirect == nil {
		t.Fatal("ping client has no CheckRedirect; it will follow a redirect to another host")
	}
	if err := c.CheckRedirect(nil, nil); !errors.Is(err, http.ErrUseLastResponse) {
		t.Fatalf("CheckRedirect = %v, want http.ErrUseLastResponse", err)
	}
}

// TestPingClientDoesNotFollowRedirects guards against the ping client sending a credential header
// (x-api-key / x-goog-api-key) to a second host: Go strips only Authorization/Cookie on a
// cross-host redirect, so following one would leak those headers to srv2.
func TestPingClientDoesNotFollowRedirects(t *testing.T) {
	var srv2Hit bool
	srv2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		srv2Hit = true
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv2.Close)

	srv1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, srv2.URL, http.StatusTemporaryRedirect)
	}))
	t.Cleanup(srv1.Close)

	u, _ := url.Parse(srv1.URL)
	d := newCortexDispatcher(newPingClient())
	key := keepalive.Key{Tenant: "", Session: "s"}
	d.hold(key, u.Scheme, u.Host, http.Header{"x-api-key": {"secret"}})
	_, status, err := d.Dispatch(t.Context(), keepalive.DispatchRequest{Key: key, Route: "/v1/messages",
		Body: []byte(`{}`), Headers: http.Header{}})
	if err != nil {
		t.Fatalf("Dispatch returned an error for a 307 response: %v", err)
	}
	if status != http.StatusTemporaryRedirect {
		t.Fatalf("status = %d, want %d (the ping client must not follow the redirect)", status, http.StatusTemporaryRedirect)
	}
	if srv2Hit {
		t.Fatal("the ping client followed the redirect to the second host; a credential header would leak there")
	}
}
