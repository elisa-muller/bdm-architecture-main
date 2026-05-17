import pandas as pd

df = pd.read_parquet("part-00000-97dd44c1-e6f7-4b7f-b687-dac9b88e220d-c000.snappy.parquet")
df.to_csv("audio5.csv", index=False)