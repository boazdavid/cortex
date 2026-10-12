package contextguru

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/rossoctl/context-guru/credmask"
	"github.com/rossoctl/context-guru/keepalive"
	"github.com/tidwall/gjson"
)

// authHeaderNames are the slots a provider credential arrives in. Held masked, per thread, only
// while the keeper tracks that thread (keepalive.Record.Hold), and zeroized on Release.
var authHeaderNames = []string{"Authorization", "x-api-key", "x-goog-api-key"}

var errNotHeld = errors.New("context-guru keep-alive: no upstream held for this session")

// maxPingResponse bounds how much of a ping's response is read. A ping is non-streaming with a
// tiny output budget, so its body is a few hundred bytes; the cap only guards a misbehaving
// upstream.
const maxPingResponse = 1 << 20

// cortexDispatcher is the plugin's keepalive.Dispatcher: transport and credential custody only.
// The credential is the one the request carried when it left Cortex's pipeline — after any
// token-exchange or credential-injection plugin ran — because OnFinish reads pctx.Headers then.
//
// Lock order: hold and Release run under the keeper's lock and take d.mu; Dispatch runs outside
// the keeper's lock and takes only d.mu. Nothing here calls back into the keeper.
type cortexDispatcher struct {
	client *http.Client
	mu     sync.Mutex
	held   map[keepalive.Key]heldTarget
}

type heldTarget struct {
	scheme, host string
	auth         []credmask.Header
}

func newCortexDispatcher(c *http.Client) *cortexDispatcher {
	return &cortexDispatcher{client: c, held: map[keepalive.Key]heldTarget{}}
}

// newPingClient is a standalone client that never routes through HTTPS_PROXY — a ping sent back
// into this proxy would be recorded as agent traffic. The sentinel header is the second guard.
//
// It never follows a redirect: Go's default client strips Authorization/Cookie on a cross-host
// redirect, but not x-api-key / x-goog-api-key, so a 307/308 could carry a credential header to a
// host the operator never intended it for. Matches core/tlsbridge/upstream.go's choice.
// IdleConnTimeout bounds how long an idle connection is kept around between pings.
func newPingClient() *http.Client {
	return &http.Client{
		Transport: &http.Transport{Proxy: nil, IdleConnTimeout: 90 * time.Second},
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}

// hold captures the upstream and the credential a finished request carried, masked. It replaces
// (and zeroizes) whatever was held for key before.
func (d *cortexDispatcher) hold(key keepalive.Key, scheme, host string, reqHeaders http.Header) {
	var auth []credmask.Header
	for _, name := range authHeaderNames {
		for _, v := range reqHeaders.Values(name) {
			b := []byte(v)
			credmask.Mask(b)
			auth = append(auth, credmask.Header{Name: name, Val: b})
		}
	}
	if scheme == "" {
		scheme = "https"
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if prev, ok := d.held[key]; ok {
		zeroAll(prev.auth)
	}
	d.held[key] = heldTarget{scheme: scheme, host: host, auth: auth}
}

// Close releases the ping client's idle connections. Called from Shutdown so a hot-reloaded
// instance's connections do not linger past it.
func (d *cortexDispatcher) Close() {
	d.client.CloseIdleConnections()
}

// Release runs under the keeper's lock: zeroize and forget, nothing else.
func (d *cortexDispatcher) Release(key keepalive.Key) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if h, ok := d.held[key]; ok {
		zeroAll(h.auth)
		delete(d.held, key)
	}
}

// Dispatch sends one ping on a private copy of the held credential, so a Release while the ping
// is on the wire zeroizes the held bytes without touching the ones being sent.
func (d *cortexDispatcher) Dispatch(ctx context.Context, req keepalive.DispatchRequest) (keepalive.Usage, int, error) {
	d.mu.Lock()
	h, ok := d.held[req.Key]
	var auth []credmask.Header
	if ok {
		auth = make([]credmask.Header, len(h.auth))
		for i, a := range h.auth {
			auth[i] = a.Clone()
		}
	}
	d.mu.Unlock()
	defer zeroAll(auth)
	if !ok || h.host == "" {
		return keepalive.Usage{}, 0, errNotHeld
	}
	// The body is copied: the keeper zeroizes req.Body once Dispatch returns.
	hreq, err := http.NewRequestWithContext(ctx, http.MethodPost,
		h.scheme+"://"+h.host+req.Route, bytes.NewReader(append([]byte(nil), req.Body...)))
	if err != nil {
		return keepalive.Usage{}, 0, err
	}
	for name, vs := range req.Headers {
		hreq.Header[name] = append([]string(nil), vs...)
	}
	hreq.Header.Set(sentinelHeader, "1")
	for _, a := range auth {
		credmask.Mask(a.Val)
		hreq.Header.Add(a.Name, string(a.Val))
		credmask.Zero(a.Val)
	}
	resp, err := d.client.Do(hreq)
	if err != nil {
		return keepalive.Usage{}, 0, err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxPingResponse))
	if err != nil {
		return keepalive.Usage{}, resp.StatusCode, err
	}
	return pingUsage(raw), resp.StatusCode, nil
}

func zeroAll(hs []credmask.Header) {
	for _, a := range hs {
		credmask.Zero(a.Val)
	}
}

// pingUsage reads a ping's non-streaming response (pings always send stream:false). Anthropic's
// input_tokens excludes the cached tiers; OpenAI Responses' input_tokens includes cached_tokens,
// so fresh is the difference — the same rule the engine's own proxy applies.
func pingUsage(raw []byte) keepalive.Usage {
	u := gjson.GetBytes(raw, "usage")
	if cached := u.Get("input_tokens_details.cached_tokens"); cached.Exists() {
		in, c := u.Get("input_tokens").Int(), cached.Int()
		return keepalive.Usage{FreshInput: max(in-c, 0), CacheRead: c, Output: u.Get("output_tokens").Int()}
	}
	return keepalive.Usage{
		FreshInput:   u.Get("input_tokens").Int(),
		CacheRead:    u.Get("cache_read_input_tokens").Int(),
		CacheWrite:   u.Get("cache_creation_input_tokens").Int(),
		CacheWrite1h: u.Get("cache_creation.ephemeral_1h_input_tokens").Int(),
		Output:       u.Get("output_tokens").Int(),
		StopReason:   gjson.GetBytes(raw, "stop_reason").String(),
	}
}
