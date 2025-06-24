"""
Utility functions for data processing and hashing.
"""

import pandas as pd
import hashlib


def fast_md5_hash(df: pd.DataFrame, hashed_columns: list[str]) -> pd.Series:
    """Vectorized MD5 hash from concatenated string of selected columns."""
    
    # Lowercase column names for uniformity (optional, if source is inconsistent)
    df_renamed = df.rename(columns={col: col.lower() for col in hashed_columns})
    
    # Fill NaNs and convert to string
    str_cols = df_renamed[[col.lower() for col in hashed_columns]].fillna('').astype(str)
    
    # Concatenate all fields into a single string per row
    concat_series = str_cols.agg(''.join, axis=1)
    
    # Hash each row's string using MD5
    return concat_series.map(lambda x: hashlib.md5(x.encode('utf-8')).hexdigest())
