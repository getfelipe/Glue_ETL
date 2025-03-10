import pandas as pd


df_orders = pd.read_parquet("files/orders/file.parquet")
df_registers = pd.read_parquet("files/registers/file.parquet")


df_orders.to_csv("files/orders/file.csv")
df_orders.to_csv("files/registers/file.csv")