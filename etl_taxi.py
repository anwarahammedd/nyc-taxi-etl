import pandas as pd

PATH = r"A:\de\projects\ny-taxi-etl\data\yellow_tripdata_2025-01.parquet"

df = pd.read_parquet(PATH)
print(df.head())
print(df.shape)
