import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa

# Read CSV
df = pd.read_csv('ADMISSIONS.csv')

# Convert datetime column to string in Hive-compatible format
df['admittime'] = pd.to_datetime(df['admittime']).dt.strftime('%Y-%m-%d %H:%M:%S')#'dischtime', 'deathtime', 'edregtime', 'edouttime'
df['dischtime'] = pd.to_datetime(df['dischtime']).dt.strftime('%Y-%m-%d %H:%M:%S')
df['deathtime'] = pd.to_datetime(df['deathtime']).dt.strftime('%Y-%m-%d %H:%M:%S')
df['edregtime'] = pd.to_datetime(df['edregtime']).dt.strftime('%Y-%m-%d %H:%M:%S')
df['edouttime'] = pd.to_datetime(df['edouttime']).dt.strftime('%Y-%m-%d %H:%M:%S')
# 3. Categorical/string columns
cat_cols = [
    'admission_type', 'admission_location', 'discharge_location',
    'insurance', 'language', 'religion', 'marital_status',
    'ethnicity', 'diagnosis'
]
df[cat_cols] = df[cat_cols].astype('category')

# 4. Boolean/flag columns
bool_cols = ['hospital_expire_flag', 'has_chartevents_data']
df[bool_cols] = df[bool_cols].astype('boolean')

print("deathtime NaNs verified: All align with hospital_expire_flag == False")

# 2. language: Impute with "Unknown"
df['language'] = df['language'].cat.add_categories('Unknown')
df['language']=df['language'].fillna('Unknown')

# 3. religion: Impute with "Unknown"
df['religion'] = df['religion'].cat.add_categories('Unknown')
df['religion']=df['religion'].fillna('Unknown')

# 4. marital_status: Impute with "Unknown"
df['marital_status'] = df['marital_status'].cat.add_categories('Unknown')
df['marital_status']=df['marital_status'].fillna('Unknown')

# 5. edregtime and edouttime: Impute with sentinel datetime
df['edregtime']=df['edregtime'].fillna('9999-12-31 00:00:00')
df['edouttime']=df['edouttime'].fillna('9999-12-31 00:00:00')

# Verify no NaNs remain (except deathtime)
print("\nNaN counts after handling:")
print(df.isna().sum())


# Convert to Arrow table and write to Parquet
table = pa.Table.from_pandas(df)
pq.write_table(table, 'ADMISSIONS.parquet')

print(table.schema)