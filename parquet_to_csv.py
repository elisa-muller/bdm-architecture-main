import pandas as pd

df = pd.read_parquet("part-00000-5d6a023a-b5e9-497d-8ed4-89fd6286f355-c000.snappy.parquet")
df.to_csv("audio3.csv", index=False)