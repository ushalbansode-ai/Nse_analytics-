def compute_premium_discount(ce_ltp, pe_ltp, underlying):
    try:
        premium = ce_ltp - pe_ltp
        discount = underlying - (ce_ltp + pe_ltp)
        return {
            "premium": round(premium, 2),
            "discount": round(discount, 2)
        }
    except Exception as e:
        return {"error": str(e)}
      
