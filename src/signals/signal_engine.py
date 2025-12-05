import pandas as pd
import numpy as np

def classify_buildup(price_change, oi_change):
    if price_change > 0 and oi_change > 0:
        return "LONG_BUILDUP"
    if price_change < 0 and oi_change > 0:
        return "SHORT_BUILDUP"
    if price_change > 0 and oi_change < 0:
        return "SHORT_COVERING"
    if price_change < 0 and oi_change < 0:
        return "LONG_UNWINDING"
    return "NEUTRAL"


def detect_signal_row(row, underlying_spot):
    signal = None
    reasons = []
    score = 0  # ← multi-factor weight

    # --- Safety ---
    if pd.isna(row["price_change_CE"]) or pd.isna(row["price_change_PE"]):
        return None, "", 0

    # --- 1. Build-ups ---
    ce_bu = classify_buildup(row["price_change_CE"], row["oi_change_CE"])
    pe_bu = classify_buildup(row["price_change_PE"], row["oi_change_PE"])

    # --- 2. OI-diff flip ---
    oi_flip = (
        (row["oi_diff_prev"] > 0 and row["oi_diff"] < 0) or
        (row["oi_diff_prev"] < 0 and row["oi_diff"] > 0)
    )

    # --- 3. IV Skew ---
    iv_skew = row["iv_pe"] - row["iv_ce"]

    # --- 4. Price relation to strike ---
    above_strike = underlying_spot > row["strike"]
    below_strike = underlying_spot < row["strike"]

    # ----------------------------
    #         CALL BUY
    # ----------------------------

    if above_strike:
        # Strong confirmation: CE SC/LB and PE unwinding
        if ce_bu in ["SHORT_COVERING", "LONG_BUILDUP"] and \
           pe_bu in ["LONG_UNWINDING", "SHORT_BUILDUP"]:

            signal = "CALL_BUY"

            reasons.append(f"CE {ce_bu}, PE {pe_bu}")
            score += 2

            if oi_flip:
                reasons.append("OI difference flipped toward CE")
                score += 1

            if iv_skew < 0:
                reasons.append("IV skew favourable for calls (CE IV rising)")
                score += 1

    # ----------------------------
    #         PUT BUY
    # ----------------------------

    if below_strike:
        if pe_bu in ["SHORT_COVERING", "LONG_BUILDUP"] and \
           ce_bu in ["LONG_UNWINDING", "SHORT_BUILDUP"]:

            signal = "PUT_BUY"

            reasons.append(f"PE {pe_bu}, CE {ce_bu}")
            score += 2

            if oi_flip:
                reasons.append("OI difference flipped toward PE")
                score += 1

            if iv_skew > 0:
                reasons.append("IV skew favourable for puts (PE IV rising)")
                score += 1

    # Final reason string
    reason_text = "; ".join(reasons) if reasons else ""

    return signal, reason_text, score
  
