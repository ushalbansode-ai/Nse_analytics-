import pandas as pd
from .utils import zscore

def detect_volume_spike(df: pd.DataFrame) -> pd.Series:
    """
    Detects abnormal volume spikes vs historical mean.
    """
    vol = df["Volume"]
    return zscore(vol)
  
