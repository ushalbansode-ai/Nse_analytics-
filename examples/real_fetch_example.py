from nse_chain.fetcher import fetch_snapshot
from nse_chain.analytics import compute_oi_differences, compute_oi_magnets_and_gaps

symbol = "NIFTY"      # or BANKNIFTY

df = fetch_snapshot(symbol)
print("\n=== RAW SNAPSHOT ===")
print(df.head())

# Derive analytics
derived = compute_oi_differences(df)
print("\n=== DERIVED ANALYTICS ===")
print(derived.head())

# Spot: use nearest ATM strike LTP average
spot = derived["strike"].iloc[len(derived)//2]

magnets, gaps = compute_oi_magnets_and_gaps(derived, spot)
print("\n=== TOP OI MAGNETS ===")
print(magnets.head())

print("\n=== OI GAPS ===")
print(gaps.head())
