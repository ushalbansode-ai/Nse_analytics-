# --------------------------------------
# BANKNIFTY OPTION ANALYTICS (OPTION 2)
# --------------------------------------

def analyze_banknifty_option2(option_chain, underlying):
    """
    BankNifty Option Strategy #2:
    - Bullish CE Strength
    - Bearish PE weakness
    - Strike near underlying (ATM)
    - CE > PE LTP Filter
    """

    try:
        records = option_chain.get("records", {}).get("data", [])

        if not records:
            return {"error": "No option chain data found"}

        # Find ATM strike
        strikes = [row["strikePrice"] for row in records]
        atm_strike = min(strikes, key=lambda x: abs(x - underlying))

        atm_row = next(
            (r for r in records if r["strikePrice"] == atm_strike),
            None
        )

        if not atm_row:
            return {"error": "ATM strike not found"}

        ce = atm_row.get("CE", {})
        pe = atm_row.get("PE", {})

        ce_ltp = ce.get("lastPrice", 0)
        pe_ltp = pe.get("lastPrice", 0)
        ce_oi_chg = ce.get("changeinOpenInterest", 0)
        pe_oi_chg = pe.get("changeinOpenInterest", 0)

        signal = "NEUTRAL"

        if ce_ltp > pe_ltp and ce_oi_chg > 0:
            signal = "BULLISH"

        if pe_ltp > ce_ltp and pe_oi_chg > 0:
            signal = "BEARISH"

        return {
            "atm_strike": atm_strike,
            "ce_ltp": ce_ltp,
            "pe_ltp": pe_ltp,
            "ce_oi_chg": ce_oi_chg,
            "pe_oi_chg": pe_oi_chg,
            "signal": signal
        }

    except Exception as e:
        return {"error": str(e)}
      
