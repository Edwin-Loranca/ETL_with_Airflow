import pandas as pd 

#FUNCION PARA CARGAR A CLEAN

def to_clean_stage(df:pd.DataFrame, raw_data: str) -> None:
    return df.to_csv(raw_data)