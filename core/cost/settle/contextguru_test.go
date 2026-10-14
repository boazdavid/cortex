package settle

import (
	"math"
	"testing"

	"github.com/rossoctl/cortex/core/cost/event"
	"github.com/rossoctl/cortex/core/cost/pricing"
	"github.com/rossoctl/cortex/core/pipeline"
)

// cgPctx is a settled response carrying a context-guru event. cacheWrite is the response's own
// cache-write count.
func cgPctx(cacheWrite int, facts map[string]any) *pipeline.Context {
	return &pipeline.Context{
		Host: "gw.internal",
		Extensions: pipeline.Extensions{
			Inference: &pipeline.InferenceExtension{
				Model: "m", InputTokens: 500, CacheReadTokens: 40_000,
				CacheWriteTokens: cacheWrite, OutputTokens: 100,
			},
			Custom: map[string]any{"context-guru" + pipeline.PluginEventSuffix: facts},
		},
	}
}

func cgFacts(extra map[string]any) map[string]any {
	m := map[string]any{"bytesRemoved": 16_000, "bodyBytesAfter": 160_000,
		"tokensBefore": 10_000, "tokensAfter": 6_000}
	for k, v := range extra {
		m[k] = v
	}
	return m
}

func onlySaving(t *testing.T, got []event.Saving) event.Saving {
	t.Helper()
	if len(got) != 1 {
		t.Fatalf("Avoided returned %d savings, want 1: %+v", len(got), got)
	}
	return got[0]
}

func near(a, b float64) bool { return math.Abs(a-b) < 1e-9 }

// The engine's own token counts are the saving, priced at the input rate: the engine compacts the
// uncached tail, which without compaction would have been billed as fresh input.
func TestAvoided_ContextGuruPricesEngineTokensAtTheInputRate(t *testing.T) {
	s := onlySaving(t, Avoided(cgPctx(0, cgFacts(nil)), rates(t)))
	if s.Component != "context-guru" || s.TokensAvoided != 4_000 || !s.Estimated {
		t.Fatalf("saving = %+v, want context-guru, 4000 tokens, estimated", s)
	}
	if s.Tier != pricing.TierInput.String() || !near(s.USD, 4_000*7e-6) || s.PenaltyUSD != 0 {
		t.Errorf("tier/usd/penalty = %q %v %v, want input, %v, 0", s.Tier, s.USD, s.PenaltyUSD, 4_000*7e-6)
	}
}

// When this turn wrote the cache, the removed tail would have been written too, so it is priced
// at the cache-write rate.
func TestAvoided_ContextGuruUsesTheCacheWriteRateOnAWritingTurn(t *testing.T) {
	s := onlySaving(t, Avoided(cgPctx(2_000, cgFacts(nil)), rates(t)))
	if s.Tier != pricing.TierCacheWrite.String() || !near(s.USD, 4_000*11e-6) {
		t.Errorf("tier/usd = %q %v, want cache-write, %v", s.Tier, s.USD, 4_000*11e-6)
	}
}

// A rewrite inside the cached prefix made the provider write, at the cache-write rate, tokens it
// would otherwise have read. That difference is the penalty, priced from the response's own
// cache-write count.
func TestAvoided_ContextGuruChargesAPrefixRewrite(t *testing.T) {
	s := onlySaving(t, Avoided(cgPctx(30_000, cgFacts(map[string]any{"prefixRewritten": true})), rates(t)))
	if want := 30_000 * (11 - 3) * 1e-6; !near(s.PenaltyUSD, want) {
		t.Errorf("PenaltyUSD = %v, want %v", s.PenaltyUSD, want)
	}
	if !near(s.USD, 4_000*11e-6) {
		t.Errorf("USD = %v, want the gross saving %v", s.USD, 4_000*11e-6)
	}
	// The penalty is larger than the saving, so this turn saved nothing.
	if got := (event.Event{Avoided: []event.Saving{s}}).TotalAvoidedUSD(); got != 0 {
		t.Errorf("TotalAvoidedUSD = %v, want 0 for a turn whose penalty exceeds its saving", got)
	}
}

// Net of a smaller penalty, the total keeps the difference.
func TestTotalAvoidedNetsThePenalty(t *testing.T) {
	ev := event.Event{Avoided: []event.Saving{{USD: 0.5, PenaltyUSD: 0.2}, {USD: 0.1}}}
	if got := ev.TotalAvoidedUSD(); !near(got, 0.4) {
		t.Errorf("TotalAvoidedUSD = %v, want 0.4", got)
	}
}

// With no engine counts, the saving falls back to the byte ratio tool-prune uses.
func TestAvoided_ContextGuruFallsBackToTheByteRatio(t *testing.T) {
	pctx := cgPctx(0, map[string]any{"bytesRemoved": 16_000, "bodyBytesAfter": 160_000})
	s := onlySaving(t, Avoided(pctx, rates(t)))
	prompt := pricing.UsageFromInference(pctx.Extensions.Inference).PromptTotal()
	if want := pricing.EstimateTokensFromBytes(16_000, prompt, 160_000); s.TokensAvoided != want {
		t.Errorf("TokensAvoided = %d, want the byte-ratio %d", s.TokensAvoided, want)
	}
}

// An observed (not applied) saving stays projected, so no total counts it.
func TestAvoided_ContextGuruCarriesProjected(t *testing.T) {
	s := onlySaving(t, Avoided(cgPctx(0, cgFacts(map[string]any{"projected": true})), rates(t)))
	if !s.Projected {
		t.Error("projected was lost")
	}
}

// Each component's event is read with its own facts type: a tool-prune event that happens to carry
// context-guru's field names is still priced by tool-prune's byte rule, never the engine counts.
func TestAvoided_ToolPruneDoesNotReadCompactionFacts(t *testing.T) {
	pctx := cgPctx(0, nil)
	pctx.Extensions.Custom = map[string]any{"tool-prune" + pipeline.PluginEventSuffix: cgFacts(map[string]any{"prefixRewritten": true})}
	pctx.Extensions.Inference.CacheWriteTokens = 30_000
	s := onlySaving(t, Avoided(pctx, rates(t)))
	prompt := pricing.UsageFromInference(pctx.Extensions.Inference).PromptTotal()
	if want := pricing.EstimateTokensFromBytes(16_000, prompt, 160_000); s.TokensAvoided != want {
		t.Errorf("TokensAvoided = %d, want tool-prune's byte-ratio %d", s.TokensAvoided, want)
	}
	if s.PenaltyUSD != 0 {
		t.Errorf("PenaltyUSD = %v, want 0: tool-prune cannot cause a prefix rewrite", s.PenaltyUSD)
	}
}
