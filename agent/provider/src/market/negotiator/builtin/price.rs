use crate::market::negotiator::factory::PriceNegotiatorConfig;
use crate::market::negotiator::{NegotiationResult, NegotiatorComponent, ProposalView};
use actix::Addr;

pub struct PriceNego {
    market: Addr<crate::market::provider_market::ProviderMarket>,
    history: std::collections::VecDeque<f64>,
    last_update: std::time::Instant,
    min_price: f64,
}

static PRICE_PROPERTY: &str = "/golem/com/pricing/model/linear/coeffs";
static THREADS_PROPERTY: &str = "/golem/inf/cpu/threads";

impl PriceNego {
    pub fn new(
        market: Addr<crate::market::provider_market::ProviderMarket>,
        config: &PriceNegotiatorConfig,
    ) -> anyhow::Result<Self> {
        Ok(PriceNego {
            market,
            history: std::collections::VecDeque::new(),
            last_update: std::time::Instant::now() - std::time::Duration::from_secs(15 * 60), // Allow immediate update
            min_price: config.min_price,
        })
    }
}

impl NegotiatorComponent for PriceNego {
    fn negotiate_step(
        &mut self,
        demand: &ProposalView,
        mut offer: ProposalView,
    ) -> anyhow::Result<NegotiationResult> {
        if let (Ok(demand_prices), Ok(offer_prices)) = (
            demand.pointer_typed::<Vec<f64>>(PRICE_PROPERTY),
            offer.pointer_typed::<Vec<f64>>(PRICE_PROPERTY),
        ) {
            // Dynamic Pricing Logic
            let threads: i64 = demand.pointer_typed::<i64>(THREADS_PROPERTY).unwrap_or(1);
            let threads_f64 = std::cmp::max(threads, 1) as f64;

            // Assume coeffs are [start, cpu, env] as typical.
            if demand_prices.len() >= 3 && offer_prices.len() >= 3 {
                let demand_cpu = demand_prices[1];
                let demand_env = demand_prices[2];
                let offer_cpu = offer_prices[1];
                let offer_env = offer_prices[2];

                let p_normalized_demand = demand_cpu + (demand_env / threads_f64);
                let p_normalized_offer = offer_cpu + (offer_env / threads_f64);

                let weight = std::cmp::min(threads, 128) as usize; // Cap weight to avoid memory explosion

                for _ in 0..weight {
                    self.history.push_back(p_normalized_demand);
                }

                while self.history.len() > 50 {
                    self.history.pop_front();
                }

                if self.history.len() >= 10 {
                    // Require at least 10 data points
                    let mut sorted = self.history.clone().into_iter().collect::<Vec<f64>>();
                    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
                    let median = sorted[sorted.len() / 2];

                    if self.last_update.elapsed().as_secs() >= 15 * 60 {
                        let mut scalar: Option<f64> = None;

                        if median > p_normalized_offer * 1.05 {
                            // Step price 50% towards median
                            scalar = Some(1.0 + (median / p_normalized_offer - 1.0) / 2.0);
                        } else if median < p_normalized_offer * 0.95 {
                            // Step price 50% towards median
                            let mut test_scalar = 1.0 - (1.0 - median / p_normalized_offer) / 2.0;

                            // Enforce Floor
                            let p_floor = self.min_price;
                            // 1-thread baseline
                            if (offer_cpu * test_scalar * 1.0) + (offer_env * test_scalar)
                                >= p_floor
                            {
                                scalar = Some(test_scalar);
                            } else {
                                // Clamp to floor
                                test_scalar = p_floor / (offer_cpu * 1.0 + offer_env);
                                // Ensure we only step down if even clamped scalar is a decrease
                                if test_scalar < 1.0 {
                                    scalar = Some(test_scalar);
                                }
                            }
                        }

                        if let Some(s) = scalar {
                            self.last_update = std::time::Instant::now();

                            // Since ProviderAgent handles UpdatePricing, we need to send it to ProviderAgent.
                            // However, ProviderAgent spawned ProviderMarket, not the other way around.
                            // But actually, we can add a simple channel or just rely on actix System Registry if we registered it.
                            // Another way: ProviderAgent can be reached via `System::current().registry().get::<ProviderAgent>()` ONLY IF it's a SystemService.
                            // A quick hack since we can't easily change the whole architecture:
                            // The easiest way is to add UpdatePricing to `ProviderMarket` and have it forward to `ProviderAgent` (we'd need `ProviderMarket` to have `Addr<ProviderAgent>`),
                            // or better, `ProviderMarket` can handle it directly! Let's just modify the macro to send it to the ProviderMarket, and we will implement UpdatePricing for ProviderMarket.
                            // BUT WAIT, we already implemented `UpdatePricing` on `ProviderAgent`.
                            // So let's send it to `ProviderMarket`, and `ProviderMarket` will handle it by... we didn't add it to ProviderMarket yet.
                            // Let's implement `UpdatePricing` on `ProviderMarket` instead, because `ProviderMarket` owns `subscriptions` and `config`, but wait, PresetManager is in `ProviderAgent`.

                            // Let's use actix::Arbiter::current().spawn to emit a global event, or just implement a global channel.
                            // Let's just use `lazy_static` to hold a global channel sender, or add `Addr<ProviderAgent>` to `ProviderMarket`.

                            // Actually, I can just use a channel!
                            self.market.do_send(
                                crate::market::provider_market::MarketUpdatePricing { scalar: s },
                            );
                        }
                    }
                }
            }

            if demand_prices == offer_prices {
                return Ok(NegotiationResult::Ready { offer });
            }
            if demand_prices.len() != offer_prices.len() {
                return Ok(NegotiationResult::Reject {
                    message: "invalid price vector".to_string(),
                    is_final: false,
                });
            }
            if demand_prices
                .iter()
                .zip(&offer_prices)
                .all(|(dp, op)| dp >= op)
            {
                if let Some(p) = offer.pointer_mut(PRICE_PROPERTY) {
                    *p = demand.pointer(PRICE_PROPERTY).unwrap().clone();
                }
                Ok(NegotiationResult::Negotiating { offer })
            } else {
                Ok(NegotiationResult::Reject {
                    message: format!("{:?} < {:?}", demand_prices, offer_prices),
                    is_final: true,
                })
            }
        } else {
            Ok(NegotiationResult::Ready { offer })
        }
    }
}
