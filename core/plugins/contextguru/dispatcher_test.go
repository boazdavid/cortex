package contextguru

import (
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
	tr, ok := newPingClient().Transport.(*http.Transport)
	if !ok {
		t.Fatalf("ping client transport is %T, want *http.Transport", newPingClient().Transport)
	}
	if tr.Proxy != nil {
		t.Fatal("ping client routes through a proxy; a ping sent back into Cortex would be recorded as agent traffic")
	}
}
