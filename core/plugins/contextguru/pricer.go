package contextguru

import (
	"context"

	"github.com/rossoctl/context-guru/keepalive"
	"github.com/rossoctl/cortex/core/cost/pricing"
)

// resolverPricer prices keep-alive pings from Cortex's own rate table, so the keeper's pre-flight
// ceiling and the cost the plugin logs are the same number (spec §9.1). r is read on every call:
// SetPricingResolver may run after Configure, and a reload hands a new instance a new resolver.
//
// Input, CacheRead and Output must be set: an unset rate on any of them is unknown, not free, and
// pricing it as zero would leave the ceiling unenforced for that model. CacheWrite is the
// exception — many tables (OpenAI's included) carry no separate cache-write rate, so an unset
// CacheWrite falls back to the Input rate rather than making the whole ping unpriced.
type resolverPricer struct{ r func() pricing.Resolver }

func (p resolverPricer) Price(_ context.Context, endpoint, model string, promptTokens int64) (keepalive.Price, bool) {
	res := p.r()
	if res == nil || model == "" {
		return keepalive.Price{}, false
	}
	rates, _ := res.Resolve(endpoint, model, int(promptTokens))
	var out keepalive.Price
	for _, t := range []struct {
		tier pricing.Tier
		dst  *float64
	}{
		{pricing.TierInput, &out.Input},
		{pricing.TierCacheRead, &out.CacheRead}, {pricing.TierOutput, &out.Output},
	} {
		v, ok := rates.For(t.tier)
		if !ok {
			return keepalive.Price{}, false
		}
		*t.dst = v
	}
	if v, ok := rates.For(pricing.TierCacheWrite); ok {
		out.CacheWrite = v
	} else {
		out.CacheWrite = out.Input
	}
	return out, true
}
