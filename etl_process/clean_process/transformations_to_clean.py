import pandas as pd
from datetime import datetime as dt


# FUNCIONES PARA REALIZAR TRANSFORMACIONES

def clean_extra_spaces(df: pd.DataFrame) -> pd.DataFrame:

    df["Platform"] = df["Platform"].str.strip()
    return df

def replace_null_values(df: pd.DataFrame) -> pd.DataFrame:

    df["Year"] = df["Year"].fillna(9999).astype(int)
    return df

def del_duplicates(df: pd.DataFrame) -> pd.DataFrame:

    df = df.drop_duplicates()
    return df

def create_load_date_column(df: pd.DataFrame) -> pd.DataFrame:

    df["load_date"] = str(dt.today().date())
    return df
 
