CREATE EXTERNAL TABLE admissions (
    row_id BIGINT,
    subject_id BIGINT,
    hadm_id BIGINT,
    admittime STRING,
    dischtime STRING,
    deathtime STRING,
    admission_type STRING,
    admission_location STRING,
    discharge_location STRING,
    insurance STRING,
    language STRING,
    religion STRING,
    marital_status STRING,
    ethnicity STRING,
    edregtime STRING,
    edouttime STRING,
    diagnosis STRING,
    hospital_expire_flag BOOLEAN,
    has_chartevents_data BOOLEAN
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/user/hadoop/mimic/ADMISSIONS/';
CREATE EXTERNAL TABLE callout (
    row_id BIGINT,
    subject_id BIGINT,
    hadm_id BIGINT,
    submit_wardid BIGINT,
    curr_wardid BIGINT,
    curr_careunit STRING,
    callout_wardid BIGINT,
    callout_service STRING,
    request_tele BIGINT,
    request_resp BIGINT,
    request_cdiff BIGINT,
    request_mrsa BIGINT,
    request_vre BIGINT,
    callout_status STRING,
    callout_outcome STRING,
    discharge_wardid DOUBLE,
    acknowledge_status STRING,
    createtime STRING,
    updatetime STRING,
    acknowledgetime STRING,
    outcometime STRING,
    firstreservationtime STRING
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/user/hadoop/mimic/CALLOUT/';

CREATE EXTERNAL TABLE d_icd_diagnoses (
    row_id BIGINT,
    icd9_code STRING,
    short_title STRING,
    long_title STRING
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/user/hadoop/mimic/D_ICD_DIAGNOSES/';

CREATE EXTERNAL TABLE diagnoses_icd (
    row_id BIGINT,
    subject_id BIGINT,
    hadm_id BIGINT,
    seq_num INT,
    icd9_code STRING
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/user/hadoop/mimic/DIAGNOSES_ICD';

CREATE EXTERNAL TABLE icustays (
    row_id BIGINT,
    subject_id BIGINT,
    hadm_id BIGINT,
    icustay_id BIGINT,
    dbsource STRING,
    first_careunit STRING,
    last_careunit STRING,
    first_wardid BIGINT,
    last_wardid BIGINT,
    intime STRING,
    outtime STRING,
    los DOUBLE
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/user/hadoop/mimic/ICUSTAYS/';

CREATE EXTERNAL TABLE labevents (
    row_id BIGINT,
    subject_id BIGINT,
    hadm_id BIGINT,
    itemid BIGINT,
    charttime STRING,
    value STRING,
    valuenum DOUBLE,
    valueuom STRING,
    flag STRING
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/user/hadoop/mimic/LABEVENTS/';

CREATE EXTERNAL TABLE patients (
    row_id BIGINT,
    subject_id BIGINT,
    gender STRING,
    dob STRING,
    dod STRING,
    dod_hosp STRING,
    dod_ssn STRING,
    expire_flag BIGINT
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/user/hadoop/mimic/PATIENTS/';
