import pandas as pd
import numpy as np

def compute_gamma_exposure(df: pd.DataFrame) -> pd.Series:
    """
    GE = Σ(OI * Gamma * Price)
    Basic placeholder formula.
    """
    return df["OI"] * df["Gamma"] * df["LTP"]
  
