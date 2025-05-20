import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa

# Load the CSV file
csv_file = 'CALLOUT.csv'  
df = pd.read_csv(csv_file)

print(df.info())

print(df.isna().sum())

#casting data types  >> createtime          | updatetime          | acknowledgetime     | outcometime         | firstreservationtime | currentreservationtime 

df['createtime'] = pd.to_datetime(df['createtime'],errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S')
df['updatetime'] = pd.to_datetime(df['updatetime'],errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S') 
df['outcometime'] = pd.to_datetime(df['outcometime'],errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S')
df['acknowledgetime'] = pd.to_datetime(df['acknowledgetime'],errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S')
df['firstreservationtime'] = pd.to_datetime(df['firstreservationtime'],errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S')
#df['currentreservationtime'] = pd.to_datetime(df['currentreservationtime']).dt.strftime('%Y-%m-%d %H:%M:%S') 
#df['discharge_wardid'] = df['discharge_wardid'].astype('Int64')
#df['request_tele'] = df['request_tele'].astype('boolean')

## handling missing values
df.drop(columns=['submit_careunit'], inplace=True)
df.drop(columns=['currentreservationtime'], inplace=True)
df['discharge_wardid'].fillna(-1,inplace=True)
df['firstreservationtime'].fillna('9999-12-31 00:00:00', inplace=True)
df['acknowledgetime'].fillna('9999-12-31 00:00:00', inplace=True)

table = pa.Table.from_pandas(df)
pq.write_table(table, 'CALLOUT.parquet')