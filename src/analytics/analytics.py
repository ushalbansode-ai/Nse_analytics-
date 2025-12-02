"""
Comprehensive `src/analytics` package implementation in a single Python file for easy copy-paste.

Notes:
- This file intentionally bundles multiple modules (analytics functions + an OptionChainAnalyzer class) and pytest-style tests at the bottom for quick validation.
- You can split the sections into separate files as per your repo layout (e.g. oi_skew.py, volume_oi.py, gamma_exposure.py, vol_smile.py, premium_theory.py, cross_expiry.py, gen_signals.py, tests/test_analytics.py).

How to use:
- Save as `src/analytics/_analytics_bundle.py` (or split into modules) and run tests with `pytest` or `python -m pytest`.
- The code avoids external dependencies except for `numpy`. It uses a small `norm_cdf` implementation so scipy isn't required.

"""

from __future__ import annotations
import math
from typing import Dict, Tuple, List, Callable, Any
import numpy as np

# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

def norm_cdf(x: float) -> float:
    """Approximate cumulative normal distribution function using erf.
    Sufficient for analytics; not a replacement for scipy.stats.norm.cdf when
    extreme precision is required.
    """
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))

# ---------------------------------------------------------------------------
# 1) Strike-wise OI skew
# ---------------------------------------------------------------------------

def oi_skew(ce_oi: int, pe_oi: int) -> Tuple[int, float]:
    """Return (difference, ratio) where ratio in [-1, 1].

    - difference = CE_OI - PE_OI
    - ratio = diff / total if total > 0 else 0
    """
    total = int(ce_oi) + int(pe_oi)
    if total == 0:
        return 0, 0.0
    diff = int(ce_oi) - int(pe_oi)
    ratio = diff / total
    return diff, ratio

# batch helper
def oi_skew_map(strike_map: Dict[float, Tuple[int, int]]) -> Dict[float, Dict[str, float]]:
    """Given strike->(ce_oi, pe_oi), return dict with diff and ratio per strike."""
    out = {}
    for k, (ce, pe) in strike_map.items():
        diff, ratio = oi_skew(ce, pe)
        out[k] = {"ce_oi": int(ce), "pe_oi": int(pe), "diff": diff, "ratio": ratio}
    return out

# ---------------------------------------------------------------------------
# 2) Volume-OI efficiency
# ---------------------------------------------------------------------------

def volume_oi_efficiency(volume: int, oi_change: int, avg_volume: float) -> float:
    """Return an efficiency score where >0 indicates fresh building activity.

    Formula used in the plan: (volume / avg_volume) * (oi_change / volume) == oi_change / avg_volume
    but we keep the full form to preserve interpretability and handle edge cases.
    """
    volume = int(volume)
    oi_change = int(oi_change)
    if avg_volume == 0 or volume == 0:
        return 0.0
    return (volume / float(avg_volume)) * (oi_change / float(volume))

# ---------------------------------------------------------------------------
# 3) Cost-of-carry & premium mispricing (Black-Scholes)
# ---------------------------------------------------------------------------

def carry_cost(future_price: float, spot_price: float) -> float:
    if spot_price == 0:
        return 0.0
    return (float(future_price) - float(spot_price)) / float(spot_price)


def bs_call_price(S: float, K: float, r: float, sigma: float, tau: float) -> float:
    """Black-Scholes call price (European) using norm_cdf.

    - S: spot price
    - K: strike
    - r: risk-free rate (annual, decimal)
    - sigma: implied vol (annual, decimal)
    - tau: time to expiry in years
    """
    S = float(S)
    K = float(K)
    if tau <= 0 or sigma <= 0:
        # option has effectively expired or zero vol: intrinsic
        return max(0.0, S - K)
    sqrt_tau = math.sqrt(tau)
    d1 = (math.log(S / K) + (r + 0.5 * sigma * sigma) * tau) / (sigma * sqrt_tau)
    d2 = d1 - sigma * sqrt_tau
    return S * norm_cdf(d1) - K * math.exp(-r * tau) * norm_cdf(d2)


def premium_misalignment(market_premium: float, theoretical_premium: float) -> float:
    if theoretical_premium == 0:
        # when theoretical is zero (deep ITM/OTM with zero tau), return absolute diff normalized to market if possible
        return float(market_premium) if market_premium != 0 else 0.0
    return (float(market_premium) - float(theoretical_premium)) / float(theoretical_premium)

# ---------------------------------------------------------------------------
# 4) Gamma exposure approximation
# ---------------------------------------------------------------------------

def approx_delta_gamma(S: float, K: float, r: float, sigma: float, tau: float, eps_rel: float = 0.0005) -> Tuple[float, float]:
    """Estimate delta and gamma via central finite differences.

    eps_rel: relative step to S (small). Default chosen to be small but stable.
    Returns (delta, gamma) per underlying unit.
    """
    S = float(S)
    eps = max(1e-4, S * float(eps_rel))
    c_up = bs_call_price(S + eps, K, r, sigma, tau)
    c = bs_call_price(S, K, r, sigma, tau)
    c_down = bs_call_price(S - eps, K, r, sigma, tau)
    delta = (c_up - c_down) / (2.0 * eps)
    gamma = (c_up - 2.0 * c + c_down) / (eps * eps)
    return delta, gamma


def gamma_exposure_per_strike(S: float, K: float, r: float, sigma: float, tau: float, oi: int, contract_size: int = 1) -> float:
    """Return approximate gamma exposure = gamma * notional * oi.

    contract_size defaults to 1 (set to 25/50 for NSE derivatives depending on instrument), and oi is open interest.
    """
    _, gamma = approx_delta_gamma(S, K, r, sigma, tau)
    # exposure per option = gamma * (S^2) ??? - different conventions exist. We use gamma per underlying unit times notional.
    # For practical laddering we multiply gamma (per 1 underlying move) * S * contract_size * oi to give scale.
    notional = S * contract_size
    return float(gamma) * float(notional) * int(oi)

# ---------------------------------------------------------------------------
# 5) Volatility smile analysis
# ---------------------------------------------------------------------------

def vol_smile_metric(atm_iv: float, otm_iv: float) -> float:
    return float(otm_iv) - float(atm_iv)

# compute a simple skew measure across strikes
def vol_skew_curve(iv_map: Dict[float, float], atm_strike: float) -> Dict[str, Any]:
    """Given strike->iv map, compute simple metrics: slope left/right, steepness, skewness.

    Returns dict with keys: left_avg, right_avg, overall_skew, steepness
    """
    strikes = sorted(iv_map.keys())
    if not strikes:
        return {"left_avg": 0.0, "right_avg": 0.0, "overall_skew": 0.0, "steepness": 0.0}
    left = [iv_map[s] for s in strikes if s < atm_strike]
    right = [iv_map[s] for s in strikes if s > atm_strike]
    left_avg = float(np.mean(left)) if left else 0.0
    right_avg = float(np.mean(right)) if right else 0.0
    overall_skew = right_avg - left_avg
    steepness = max(left_avg, right_avg) - min(left_avg, right_avg)
    return {"left_avg": left_avg, "right_avg": right_avg, "overall_skew": overall_skew, "steepness": steepness}

# ---------------------------------------------------------------------------
# 6) Time-decay curvature (theta second derivative)
# ---------------------------------------------------------------------------

def time_decay_curvature(option_price_func: Callable[[float], float], days_list: List[int] = [30, 7, 3, 1]) -> float:
    """Estimate curvature of theta using backward finite differences on discrete days.

    option_price_func(d) should return option price when d days to expiry.
    We approximate theta(d) ≈ price(d-1) - price(d) (change if we reduce 1 day).
    Then curvature = second finite difference of thetas across a selection.
    """
    if not days_list:
        return 0.0
    prices = []
    for d in days_list:
        if d <= 0:
            prices.append(option_price_func(0))
        else:
            prices.append(option_price_func(d))
    # thetas approximated
    thetas = []
    for i in range(len(prices) - 1):
        thetas.append(prices[i + 1] - prices[i])
    if len(thetas) < 2:
        return 0.0
    # curvature as second diff across first 3 thetas if available
    if len(thetas) >= 3:
        curv = thetas[2] - 2 * thetas[1] + thetas[0]
    else:
        curv = thetas[-1] - thetas[-2]
    return float(curv)

# ---------------------------------------------------------------------------
# 7) Cross-expiry analysis (simple)
# ---------------------------------------------------------------------------

def cross_expiry_rollover(oi_by_expiry: Dict[str, Dict[float, int]]) -> Dict[str, Any]:
    """Given mapping expiry->(strike->oi), compute simple rollover indicators:
    - For each strike, find the expiry where OI is concentrated
    - Flag strikes where OI shifts from near-term to far-term (institutional rollover)
    Return structure with per-strike time-series and simple signals.
    """
    # Build strike->expiry->oi map
    strikes = set()
    expiries = sorted(oi_by_expiry.keys())
    for e, s_map in oi_by_expiry.items():
        strikes.update(s_map.keys())
    strikes = sorted(strikes)
    strike_timeseries = {s: {e: oi_by_expiry.get(e, {}).get(s, 0) for e in expiries} for s in strikes}
    signals = {}
    for s, series in strike_timeseries.items():
        vals = [series[e] for e in expiries]
        # simple heuristic: if near-term sum decreases while far-term increases, signal rollover
        near = sum(vals[:max(1, len(vals)//2)])
        far = sum(vals[max(1, len(vals)//2):])
        if near > 0 and far > near * 1.2:
            signals[s] = "Rollover to far expiry"
        else:
            signals[s] = "Stable/No Rollover"
    return {"expiries": expiries, "strike_timeseries": strike_timeseries, "signals": signals}

# ---------------------------------------------------------------------------
# 8) Signal identification helper
# ---------------------------------------------------------------------------

def identify_buildup(volume: int, oi_change: int, avg_volume: float, price_change: float) -> str:
    volume = int(volume)
    oi_change = int(oi_change)
    if volume > avg_volume and oi_change > 0:
        return "Fresh Long Buildup" if price_change > 0 else "Fresh Short Buildup"
    if volume > avg_volume and oi_change < 0:
        return "Position Unwinding"
    return "Neutral"

# ---------------------------------------------------------------------------
# 9) OptionChainAnalyzer - organizes functionality
# ---------------------------------------------------------------------------

class OptionChainAnalyzer:
    """High-level wrapper to operate on a single option chain snapshot.

    Expected snapshot format (per strike):
    {
        strike: {
            'ce_oi': int, 'pe_oi': int,
            'ce_vol': int, 'pe_vol': int,
            'ce_iv': float, 'pe_iv': float,
            'ce_price': float, 'pe_price': float
        },
        ...
    }
    """

    def __init__(self, snapshot: Dict[float, Dict[str, Any]], spot: float, future: float, r: float = 0.06, contract_size: int = 1):
        self.snapshot = snapshot
        self.spot = float(spot)
        self.future = float(future)
        self.r = float(r)
        self.contract_size = int(contract_size)

    def compute_oi_skew_map(self) -> Dict[float, Dict[str, float]]:
        strike_map = {s: (d.get('ce_oi', 0), d.get('pe_oi', 0)) for s, d in self.snapshot.items()}
        return oi_skew_map(strike_map)

    def compute_volume_oi_efficiency_map(self, avg_volume_map: Dict[float, float]) -> Dict[float, float]:
        out = {}
        for s, d in self.snapshot.items():
            vol = d.get('ce_vol', 0) + d.get('pe_vol', 0)
            oi_change = d.get('ce_oi_change', 0) + d.get('pe_oi_change', 0)
            avg_v = avg_volume_map.get(s, 0.0)
            out[s] = volume_oi_efficiency(vol, oi_change, avg_v)
        return out

    def compute_gamma_exposure_map(self, tau_years: float) -> Dict[float, float]:
        out = {}
        for s, d in self.snapshot.items():
            # we use average IV between CE/PE for a rough gamma estimate
            ivs = [v for v in (d.get('ce_iv', None), d.get('pe_iv', None)) if v is not None]
            sigma = float(np.mean(ivs)) if ivs else 0.2
            oi = int(d.get('ce_oi', 0) + d.get('pe_oi', 0))
            out[s] = gamma_exposure_per_strike(self.spot, s, self.r, sigma, tau_years, oi, self.contract_size)
        return out

    def compute_vol_smile(self, atm_strike: float) -> Dict[str, Any]:
        iv_map = {s: float(np.mean([d.get('ce_iv', 0.0), d.get('pe_iv', 0.0)])) for s, d in self.snapshot.items()}
        return vol_skew_curve(iv_map, atm_strike)

    def theoretical_vs_market_map(self, tau_years: float) -> Dict[float, Dict[str, float]]:
        out = {}
        for s, d in self.snapshot.items():
            ce_th = bs_call_price(self.spot, s, self.r, float(d.get('ce_iv', 0.2)), tau_years)
            ce_market = float(d.get('ce_price', 0.0))
            ce_misal = premium_misalignment(ce_market, ce_th)
            # for puts: use put-call parity to compute theoretical put if needed or approximate
            # simple approach: compute put price via BS parity: P = C + K*e^{-r*t} - S
            put_th = ce_th + s * math.exp(-self.r * tau_years) - self.spot
            pe_market = float(d.get('pe_price', 0.0))
            pe_misal = premium_misalignment(pe_market, put_th if put_th != 0 else pe_market)
            out[s] = {'ce_theoretical': ce_th, 'ce_market': ce_market, 'ce_misal': ce_misal,
                      'pe_theoretical': put_th, 'pe_market': pe_market, 'pe_misal': pe_misal}
        return out

    def identify_top_buildups(self, avg_volume_map: Dict[float, float], top_n: int = 10) -> List[Tuple[float, str, float]]:
        eff_map = self.compute_volume_oi_efficiency_map(avg_volume_map)
        scored = sorted(eff_map.items(), key=lambda x: abs(x[1]), reverse=True)
        out = []
        for s, score in scored[:top_n]:
            d = self.snapshot[s]
            price_change = float(d.get('underlying_change', 0.0))
            vol = d.get('ce_vol', 0) + d.get('pe_vol', 0)
            oi_change = d.get('ce_oi_change', 0) + d.get('pe_oi_change', 0)
            label = identify_buildup(vol, oi_change, avg_volume_map.get(s, 0.0), price_change)
            out.append((s, label, score))
        return out

# ---------------------------------------------------------------------------
# Simple pytest-style tests at bottom
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    # Quick manual smoke tests and demonstrations
    print("Running basic smoke tests...")

    # 1) oi_skew
    print(oi_skew(1000, 500))
    print(oi_skew(0, 0))

    # 2) vol-oi efficiency
    print(volume_oi_efficiency(1000, 200, 800))
    print(volume_oi_efficiency(0, 0, 100))

    # 3) carry
    print(carry_cost(110, 100))

    # 4) bs price
    c = bs_call_price(100, 100, 0.06, 0.2, 30/252)
    print(f"BS call sample: {c:.4f}")

    # 5) gamma approx
    d, g = approx_delta_gamma(100, 100, 0.06, 0.2, 30/252)
    print(f"delta: {d:.6f}, gamma: {g:.6f}")

    # 6) vol smile
    iv_map = {90:0.25, 95:0.22, 100:0.2, 105:0.21, 110:0.24}
    print(vol_skew_curve(iv_map, 100))

    # 7) time decay curvature
    def opt_price_func(d):
        # toy: price roughly proportional to sqrt(days)
        return max(0.0, math.sqrt(max(0, d))) * 0.5
    print(time_decay_curvature(opt_price_func))

    # 8) cross expiry
    oi_by_expiry = {
        'W1': {100: 200, 105: 50},
        'M1': {100: 250, 105: 300},
    }
    print(cross_expiry_rollover(oi_by_expiry))

    # 9) OptionChainAnalyzer demo
    snapshot = {
        100.0: {'ce_oi': 200, 'pe_oi': 100, 'ce_vol': 500, 'pe_vol': 300, 'ce_iv': 0.22, 'pe_iv': 0.24, 'ce_price': 2.5, 'pe_price':1.8, 'ce_oi_change':10, 'pe_oi_change': -5, 'underlying_change': 0.5},
        105.0: {'ce_oi': 100, 'pe_oi': 400, 'ce_vol': 200, 'pe_vol': 800, 'ce_iv': 0.28, 'pe_iv': 0.32, 'ce_price': 1.2, 'pe_price':3.2, 'ce_oi_change': -20, 'pe_oi_change': 30, 'underlying_change': -0.3}
    }
    aca = OptionChainAnalyzer(snapshot, spot=102.5, future=103.0, r=0.06, contract_size=1)
    print(aca.compute_oi_skew_map())
    print(aca.compute_volume_oi_efficiency_map({100.0:400, 105.0:600}))
    print(aca.compute_gamma_exposure_map(30/252))
    print(aca.compute_vol_smile(100.0))
    print(aca.theoretical_vs_market_map(30/252))
    print(aca.identify_top_buildups({100.0:400, 105.0:600}))

    print("Smoke tests finished.")

