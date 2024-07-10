import pandas as pd


#FUNCION PARA LEER DESDE RAW

def read_data_from_raw_stage(raw_path: str) -> pd.DataFrame:
    return pd.read_csv(raw_path, index_col="Rank")
