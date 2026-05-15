import pandas as pd

df = pd.read_parquet("part-00000-68e2532c-d41d-45ff-ac11-93b10b59cc85-c000.snappy.parquet")
df.to_csv("audio.csv", index=False)