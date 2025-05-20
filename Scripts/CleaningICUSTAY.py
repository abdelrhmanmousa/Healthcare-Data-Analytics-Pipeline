import pyarrow.parquet as pq
import pyarrow as pa
import pandas as pd

df = pd.read_csv('ICUSTAYS.csv')

print(df.isna().sum())

df.info()

df['intime'] = pd.to_datetime(df['intime']).dt.strftime('%Y-%m-%d %H:%M:%S')
df['outtime'] = pd.to_datetime(df['outtime']).dt.strftime('%Y-%m-%d %H:%M:%S')

table = pa.Table.from_pandas(df)
pq.write_table(table, 'ICUSTAYS.parquet')

print(table.schema)