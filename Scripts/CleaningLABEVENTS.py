import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa

df = pd.read_csv('LABEVENTS.csv')
print(df.head())

print(df.info())

print(df.isna().sum())

df['charttime'] = pd.to_datetime(df['charttime'],errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S')
df['hadm_id'].fillna(-1, inplace=True)
df['hadm_id'] =df['hadm_id'].astype('int64')

df['charttime'].fillna('9999-12-31 00:00:00', inplace=True)
df['flag'].fillna('Normal', inplace=True)
df['valueuom'].fillna('Unknown', inplace=True)

df['value'].fillna('Unknown', inplace=True)
df['valuenum'].fillna(-1, inplace=True)

table = pa.Table.from_pandas(df)
pq.write_table(table, 'LABEVENTS.parquet')

parquet_schema = pq.read_table('LABEVENTS.parquet').schema
print([(field.name, field.type) for field in parquet_schema])