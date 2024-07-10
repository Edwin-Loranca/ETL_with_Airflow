import pandas as pd

def read_from_clean_stage(clean_path: str) -> None:
    return pd.read_csv(clean_path, index_col="Rank")