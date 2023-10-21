from hashlib import sha1
from typing import List

import pandas as pd


def generate_hash_id(df: pd.DataFrame, keys: List[str | float | int]) -> pd.Series:
    df_keys = df.loc[:, keys].astype(str).fillna("NULL")
    hash_id = df_keys.apply(lambda x: sha1("-".join(x).encode()).hexdigest(), axis=1)
    return hash_id
