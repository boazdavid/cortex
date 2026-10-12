package contextguru

import (
	"context"
	"testing"

	"github.com/rossoctl/cortex/core/cost/pricing"
)

type fixedResolver struct {
	rates  pricing.Rates
	gotEP  string
	gotTok int
}

func (f *fixedResolver) Resolve(ep, _ string, promptTotal int) (pricing.Rates, pricing.Provenance) {
	f.gotEP, f.gotTok = ep, promptTotal
	return f.rates, pricing.ProvNone
}
func (f *fixedResolver) CurrencyFor(string, string) string { return "USD" }

func ratesOf(in, cw, cr, out float64) pricing.Rates {
	var r pricing.Rates
	for t, v := range map[pricing.Tier]float64{pricing.TierInput: in, pricing.TierCacheWrite: cw,
		pricing.TierCacheRead: cr, pricing.TierOutput: out} {
		r.Base[t], r.Set[t] = v, true
	}
	return r
}

func TestResolverPricerMapsTiersAndPassesEndpointAndPrefix(t *testing.T) {
	f := &fixedResolver{rates: ratesOf(3e-6, 3.75e-6, 0.3e-6, 15e-6)}
	p := resolverPricer{r: func() pricing.Resolver { return f }}
	price, ok := p.Price(context.Background(), "api.anthropic.com", "claude-sonnet-5", 48_000)
	if !ok {
		t.Fatal("priced model reported unpriced")
	}
	if price.Input != 3e-6 || price.CacheWrite != 3.75e-6 || price.CacheRead != 0.3e-6 || price.Output != 15e-6 {
		t.Fatalf("price = %+v", price)
	}
	if f.gotEP != "api.anthropic.com" || f.gotTok != 48_000 {
		t.Fatalf("Resolve got (%q, %d)", f.gotEP, f.gotTok)
	}
}

func TestResolverPricerUnsetTierIsUnpriced(t *testing.T) {
	r := ratesOf(3e-6, 3.75e-6, 0.3e-6, 15e-6)
	r.Set[pricing.TierCacheRead] = false
	p := resolverPricer{r: func() pricing.Resolver { return &fixedResolver{rates: r} }}
	if _, ok := p.Price(context.Background(), "h", "m", 1); ok {
		t.Fatal("a rate table without cache-read priced a ping")
	}
}

func TestResolverPricerUnsetCacheWriteFallsBackToInput(t *testing.T) {
	r := ratesOf(3e-6, 3.75e-6, 0.3e-6, 15e-6)
	r.Set[pricing.TierCacheWrite] = false
	p := resolverPricer{r: func() pricing.Resolver { return &fixedResolver{rates: r} }}
	price, ok := p.Price(context.Background(), "h", "m", 1)
	if !ok {
		t.Fatal("a rate table with only cache-write unset left the ping unpriced")
	}
	if price.CacheWrite != price.Input {
		t.Fatalf("CacheWrite = %v, want it to fall back to Input = %v", price.CacheWrite, price.Input)
	}
}

func TestResolverPricerUnsetOutputIsUnpriced(t *testing.T) {
	r := ratesOf(3e-6, 3.75e-6, 0.3e-6, 15e-6)
	r.Set[pricing.TierOutput] = false
	p := resolverPricer{r: func() pricing.Resolver { return &fixedResolver{rates: r} }}
	if _, ok := p.Price(context.Background(), "h", "m", 1); ok {
		t.Fatal("a rate table without output priced a ping")
	}
}

func TestResolverPricerWithNoResolver(t *testing.T) {
	p := resolverPricer{r: func() pricing.Resolver { return nil }}
	if _, ok := p.Price(context.Background(), "h", "m", 1); ok {
		t.Fatal("nil resolver priced a ping")
	}
}
