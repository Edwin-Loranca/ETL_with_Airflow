import pandas as pd


def sales_by_publisher(df: pd.DataFrame) -> pd.DataFrame:
    return df.groupby('Publisher').agg({
        'NA_Sales': 'sum',
        'EU_Sales': 'sum',
        'JP_Sales': 'sum',
        'Other_Sales': 'sum',
        'Global_Sales': 'sum'
    })