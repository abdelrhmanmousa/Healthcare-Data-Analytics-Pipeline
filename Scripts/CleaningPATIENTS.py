import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa

df = pd.read_csv('PATIENTS.csv')
print(df.head())

#casting columns data to string data to can deal with HIVE
df['dob'] = pd.to_datetime(df['dob']).dt.strftime('%Y-%m-%d %H:%M:%S')
df['dod'] = pd.to_datetime(df['dod']).dt.strftime('%Y-%m-%d %H:%M:%S')
df['dod_hosp'] = pd.to_datetime(df['dod_hosp']).dt.strftime('%Y-%m-%d %H:%M:%S')
df['dod_ssn'] = pd.to_datetime(df['dod_ssn']).dt.strftime('%Y-%m-%d %H:%M:%S')

print(df.isna().sum())

#Impute with sentinel datetime
df['dod_hosp'].fillna('9999-12-31 00:00:00', inplace=True)
df['dod_ssn'].fillna('9999-12-31 00:00:00', inplace=True)

#save to parquet file
table = pa.Table.from_pandas(df)
pq.write_table(table, 'PATIENTS.parquet')