import pandas as pd

def load_to_consumption_stage(df: pd.DataFrame, consumption_path: str) -> None:
    return df.to_csv(consumption_path)