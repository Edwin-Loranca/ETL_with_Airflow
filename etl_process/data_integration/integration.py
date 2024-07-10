import pandas as pd


def integrate_data(source_path: str, raw_path: str) -> None:
    df = pd.read_csv(source_path, index_col="Rank")
    return df.to_csv(raw_path)

    