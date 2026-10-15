package contextguru

import (
	"net/http"
	"testing"
	"time"

	bschemas "github.com/maximhq/bifrost/core/schemas"
	cgconfig "github.com/rossoctl/context-guru/config"
	"github.com/rossoctl/context-guru/dispatch"
	"github.com/rossoctl/cortex/core/pipeline"
)

func finishedPctx(status int, role pipeline.AgentRole) *pipeline.Context {
	p := inferencePctx("/v1/messages", []byte(`{"model":"claude-sonnet-5","messages":[]}`))
	p.StartedAt = time.Unix(1_700_000_000, 0)
	p.StatusCode = status
	p.Headers = http.Header{"Authorization": {"Bearer caller"}, "Anthropic-Version": {"2023-06-01"}}
	p.Extensions.Inference = &pipeline.InferenceExtension{Model: "claude-sonnet-5",
		InputTokens: 3, CacheReadTokens: 48_000, OutputTokens: 120,
		PresentKinds: 1 | 2 | 4 | 8, AgentRole: role}
	return p
}

// heldCount reads the dispatcher's holds under its lock.
func heldCount(p *ContextGuru) int {
	p.disp.mu.Lock()
	defer p.disp.mu.Unlock()
	return len(p.disp.held)
}

// keepAlivePlugin is a configured, started plugin with keep-alive on and the prefix floor out of
// the way; Shutdown runs at cleanup.
func keepAlivePlugin(t *testing.T) *ContextGuru {
	t.Helper()
	p := configured(t, `{"keep_alive": {"enabled": true, "min_prefix_tokens": 1}}`)
	if err := p.Init(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Shutdown(t.Context()) })
	return p
}

// finishTurns drives n finished main-thread requests through OnRequest and OnFinish.
func finishTurns(t *testing.T, p *ContextGuru, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		pctx := finishedPctx(200, pipeline.AgentRoleMain)
		invokeReq(p, pctx)
		p.OnFinish(t.Context(), pctx)
	}
}

func TestRecordForMapsAFinishedRequest(t *testing.T) {
	pol := keepAliveConfig{Enabled: true}.policy()
	r, ok := recordFor(finishedPctx(200, pipeline.AgentRoleMain), "s1", pol, func() dispatch.Ref { return "" }, logEmitter{})
	if !ok {
		t.Fatal("a finished main-thread request was not recorded")
	}
	if r.Key.Session != "s1" || r.Key.Thread != "" || r.Status != 200 || r.Provider != bschemas.Anthropic {
		t.Fatalf("record = %+v", r)
	}
	if r.Usage.CacheRead != 48_000 || r.Usage.FreshInput != 3 || r.Usage.Output != 120 || !r.UsageOK {
		t.Fatalf("usage = %+v ok=%v", r.Usage, r.UsageOK)
	}
	if r.Route != "/v1/messages" || r.Endpoint != "ollama" || r.Hold == nil {
		t.Fatalf("route/endpoint/hold = %q %q %v", r.Route, r.Endpoint, r.Hold != nil)
	}
	if !r.StartedAt.Equal(time.Unix(1_700_000_000, 0)) || len(r.Body) == 0 || r.Emitter == nil {
		t.Fatalf("startedAt/body/emitter = %v %d %v", r.StartedAt, len(r.Body), r.Emitter)
	}
}

func TestRecordForUsageNotOKWithoutCacheTiers(t *testing.T) {
	pctx := finishedPctx(200, pipeline.AgentRoleMain)
	pctx.Extensions.Inference.PresentKinds = 1 | 8 // input + output only
	r, ok := recordFor(pctx, "s1", keepAliveConfig{Enabled: true}.policy(), nil, logEmitter{})
	if !ok || r.UsageOK {
		t.Fatalf("ok=%v usageOK=%v; want recorded with UsageOK false", ok, r.UsageOK)
	}
}

func TestRecordForSkipsSubagentsAndMissingInputs(t *testing.T) {
	pol := keepAliveConfig{Enabled: true}.policy()
	if _, ok := recordFor(finishedPctx(200, pipeline.AgentRoleSubagent), "s1", pol, nil, logEmitter{}); ok {
		t.Error("a subagent request was recorded under the main thread's key")
	}
	if _, ok := recordFor(finishedPctx(200, pipeline.AgentRoleMain), "", pol, nil, logEmitter{}); ok {
		t.Error("a request with no session was recorded")
	}
	p := finishedPctx(200, pipeline.AgentRoleMain)
	p.Extensions.Inference = nil
	if _, ok := recordFor(p, "s1", pol, nil, logEmitter{}); ok {
		t.Error("a request with no inference parse was recorded")
	}
}

func TestPolicyDefaults(t *testing.T) {
	p := keepAliveConfig{Enabled: true}.policy()
	if !p.On() || p.Idle != time.Duration(cgconfig.DefaultKeepAliveIdle)*time.Second ||
		p.MaxPings != cgconfig.DefaultKeepAliveMaxPings ||
		p.MaxUSDPerPing != cgconfig.DefaultKeepAliveMaxUSDPerPing ||
		p.MinPrefixTokens != cgconfig.DefaultKeepAliveMinPrefix {
		t.Fatalf("policy = %+v", p)
	}
	if (keepAliveConfig{}).policy().On() {
		t.Fatal("keep-alive on with enabled: false")
	}
	o := keepAliveConfig{Enabled: true, IdleSeconds: 60, MaxPings: 5, MaxUSDPerPing: 0.1,
		MinPrefixTokens: 1000}.policy()
	if o.Idle != time.Minute || o.MaxPings != 5 || o.MaxUSDPerPing != 0.1 || o.MinPrefixTokens != 1000 {
		t.Fatalf("overrides not applied: %+v", o)
	}
}

func TestKeepAliveOffByDefault(t *testing.T) {
	p := configured(t, `{}`)
	if p.keeper != nil || p.disp != nil {
		t.Fatal("a keeper was built with no keep_alive block")
	}
	// The lifecycle hooks are no-ops with keep-alive off.
	if err := p.Init(t.Context()); err != nil {
		t.Fatal(err)
	}
	pctx := finishedPctx(200, pipeline.AgentRoleMain)
	invokeReq(p, pctx)
	p.OnFinish(t.Context(), pctx)
	if err := p.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestOnFinishHoldsTheCredentialOnlyForATrackedRequest(t *testing.T) {
	p := keepAlivePlugin(t)
	// Turn 0 is counted but never tracked (the engine's first-request gate).
	finishTurns(t, p, 2)
	if n := p.keeper.Stats().Live; n != 1 {
		t.Fatalf("live entries = %d, want 1", n)
	}
	if n := heldCount(p); n != 1 {
		t.Fatalf("held credentials = %d, want 1", n)
	}
	// A denied request on the same session retires nothing new and holds nothing.
	bad := finishedPctx(401, pipeline.AgentRoleMain)
	invokeReq(p, bad) // Arrive clears the tracked entry
	p.OnFinish(t.Context(), bad)
	if n := heldCount(p); n != 0 {
		t.Fatalf("held credentials after a 401 = %d, want 0", n)
	}
}

func TestSubagentRequestDoesNotClearTheMainThread(t *testing.T) {
	p := keepAlivePlugin(t)
	finishTurns(t, p, 2)
	sub := finishedPctx(200, pipeline.AgentRoleSubagent)
	invokeReq(p, sub)
	p.OnFinish(t.Context(), sub)
	if n := p.keeper.Stats().Live; n != 1 {
		t.Fatalf("live entries after a subagent request = %d, want the main thread's 1", n)
	}
	if n := heldCount(p); n != 1 {
		t.Fatalf("held credentials = %d, want 1", n)
	}
}

func TestSentinelRequestIsNeverRecorded(t *testing.T) {
	p := keepAlivePlugin(t)
	for i := 0; i < 2; i++ {
		pctx := finishedPctx(200, pipeline.AgentRoleMain)
		pctx.Headers.Set(sentinelHeader, "1")
		invokeReq(p, pctx)
		p.OnFinish(t.Context(), pctx)
	}
	if n := p.keeper.Stats().Live; n != 0 {
		t.Fatalf("a sentinel request was tracked: %d live", n)
	}
}

func TestShutdownReleasesHeldCredentials(t *testing.T) {
	p := configured(t, `{"keep_alive": {"enabled": true, "min_prefix_tokens": 1}}`)
	if err := p.Init(t.Context()); err != nil {
		t.Fatal(err)
	}
	finishTurns(t, p, 2)
	if n := heldCount(p); n != 1 {
		t.Fatalf("held credentials before Shutdown = %d, want 1", n)
	}
	if err := p.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
	if n := heldCount(p); n != 0 {
		t.Fatalf("held credentials after Shutdown = %d, want 0", n)
	}
}

func TestConfigSchemaListsKeepAlive(t *testing.T) {
	for _, f := range New().ConfigSchema() {
		if f.Name == "keep_alive" {
			return
		}
	}
	t.Fatal("ConfigSchema has no keep_alive entry")
}

// TestASkippedSubagentRequestDoesNotPauseTheMainThread covers a request that looks like ordinary
// traffic when OnRequest runs — Extensions.Inference is still nil, as it would be if this plugin
// runs before inference-parser's own OnRequest in the chain — so isSubagent is false there and
// Arrive opens an arrival on the session's (threadless) key. By OnFinish, inference-parser has
// since run and the request turns out to be a subagent's, which recordFor skips: no Record, and
// before this fix, no Depart either. That leaked arrival pins the main thread's OWN entry
// in-flight (same Key{Session: sid}, no Thread — #423's per-thread keying collapses to one key
// when nothing ever sets Thread), so the keeper's sweep would never ping it again.
func TestASkippedSubagentRequestDoesNotPauseTheMainThread(t *testing.T) {
	p := keepAlivePlugin(t)

	sub := inferencePctx("/v1/messages", []byte(`{"model":"claude-sonnet-5","messages":[]}`))
	sub.StartedAt = time.Unix(1_700_000_000, 0)
	sub.StatusCode = 200
	sub.Headers = http.Header{"Authorization": {"Bearer caller"}, "Anthropic-Version": {"2023-06-01"}}
	invokeReq(p, sub) // Extensions.Inference is nil: isSubagent is false, so Arrive fires here.
	sub.Extensions.Inference = &pipeline.InferenceExtension{Model: "claude-sonnet-5",
		InputTokens: 3, CacheReadTokens: 48_000, OutputTokens: 120,
		PresentKinds: 1 | 2 | 4 | 8, AgentRole: pipeline.AgentRoleSubagent}
	p.OnFinish(t.Context(), sub) // recordFor now sees the subagent role and skips.

	// The main thread's own ordinary turns, after the subagent request. Turn 0 is counted but
	// never tracked (the engine's first-request gate); turn 1 establishes the live entry.
	finishTurns(t, p, 2)
	if n := p.keeper.Stats().Live; n != 1 {
		t.Fatalf("live entries after the main thread's turns = %d, want 1", n)
	}

	if n := p.keeper.Sweep(time.Unix(1_700_000_000, 0).Add(281 * time.Second)); n != 1 {
		t.Fatalf("sweep sent %d pings, want 1 — the skipped subagent request's undeparted "+
			"arrival pinned the main thread's entry in-flight", n)
	}
}

// TestARequestWithNoInferenceParseDoesNotPauseTheMainThread is
// TestASkippedSubagentRequestDoesNotPauseTheMainThread's sibling for recordFor's other skip:
// Extensions.Inference stays nil all the way to OnFinish (no parser ever populated it, e.g. a
// gated path that isn't valid inference traffic). Arrive still opens on it in OnRequest, and
// without a Depart on this path too the leaked arrival would pin the main thread's entry the
// same way.
func TestARequestWithNoInferenceParseDoesNotPauseTheMainThread(t *testing.T) {
	p := keepAlivePlugin(t)

	noParse := inferencePctx("/v1/messages", []byte(`{"model":"claude-sonnet-5","messages":[]}`))
	noParse.StartedAt = time.Unix(1_700_000_000, 0)
	noParse.StatusCode = 200
	noParse.Headers = http.Header{"Authorization": {"Bearer caller"}, "Anthropic-Version": {"2023-06-01"}}
	invokeReq(p, noParse)            // Arrive fires: no Extensions.Inference, so isSubagent is false.
	p.OnFinish(t.Context(), noParse) // recordFor skips: still no Extensions.Inference.

	finishTurns(t, p, 2)
	if n := p.keeper.Stats().Live; n != 1 {
		t.Fatalf("live entries after the main thread's turns = %d, want 1", n)
	}

	if n := p.keeper.Sweep(time.Unix(1_700_000_000, 0).Add(281 * time.Second)); n != 1 {
		t.Fatalf("sweep sent %d pings, want 1 — the no-inference-parse request's undeparted "+
			"arrival pinned the main thread's entry in-flight", n)
	}
}

func TestRequestFinishingAfterShutdownHoldsNothing(t *testing.T) {
	p := configured(t, `{"keep_alive": {"enabled": true, "min_prefix_tokens": 1}}`)
	if err := p.Init(t.Context()); err != nil {
		t.Fatal(err)
	}
	finishTurns(t, p, 2)
	if n := heldCount(p); n != 1 {
		t.Fatalf("held credentials before Shutdown = %d, want 1", n)
	}
	if err := p.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
	// Requests still draining on the old pipeline after a hot-reload. Two of them, because
	// Keeper.Shutdown resets the turn counter and a lone turn 0 is never tracked anyway.
	finishTurns(t, p, 2)
	if n := heldCount(p); n != 0 {
		t.Fatalf("held credentials after a post-Shutdown finish = %d, want 0", n)
	}
	if n := p.keeper.Stats().Live; n != 0 {
		t.Fatalf("live entries after a post-Shutdown finish = %d, want 0", n)
	}
}
