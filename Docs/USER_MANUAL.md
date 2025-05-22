# Healthcare Data Analytics Pipeline User Manual

![Docker](https://img.shields.io/badge/Docker-2496ED?logo=docker&logoColor=white)
![Hadoop](https://img.shields.io/badge/Hadoop-66CCFF?logo=apache-hadoop&logoColor=black)
![Hive](https://img.shields.io/badge/Hive-FDEE21?logo=apache-hive&logoColor=black)
![Python](https://img.shields.io/badge/Python-3776AB?logo=python&logoColor=white)

This repository ([abdelrhmanmousa/Healthcare-Data-Analytics-Pipeline](https://github.com/abdelrhmanmousa/Healthcare-Data-Analytics-Pipeline)) contains a pipeline for processing the MIMIC-III Clinical Database (demo version) using Docker, Hadoop, Hive, and MapReduce.

## Table of Contents
- [Overview](#overview)
- [Purpose](#purpose)
- [Prerequisites](#prerequisites)
- [Setup](#setup)
- [Running the Pipeline](#running-the-pipeline)
  - [Step 1: Clean Data with Pandas](#step-1-clean-data-with-pandas)
  - [Step 2: Copy Parquet Files to HDFS](#step-2-copy-parquet-files-to-hdfs)
  - [Step 3: Run MapReduce Job for Average Age](#step-3-run-mapreduce-job-for-average-age)
  - [Step 4: Set Up Hive Tables](#step-4-set-up-hive-tables)
  - [Step 5: Run Hive Queries](#step-5-run-hive-queries)
- [Expected Output](#expected-output)
- [Troubleshooting](#troubleshooting)
- [Notes](#notes)

## Overview
This user manual provides step-by-step instructions to run the **Healthcare Data Analytics Pipeline**, which processes the MIMIC-III Clinical Database (demo version) to analyze healthcare data. The pipeline uses Docker containers with Hadoop, Hive, and MapReduce to ingest, store, and analyze data from seven CSV files: `ADMISSIONS.csv`, `CALLOUT.csv`, `ICUSTAYS.csv`, `LABEVENTS.csv`, `PATIENTS.csv`, `DIAGNOSES_ICD.csv`, and `D_ICD_DIAGNOSES.csv`. It performs tasks like calculating average patient age via MapReduce and analyzing admission trends via Hive queries.

### Purpose
The pipeline:
- Cleans and converts MIMIC-III CSV files to Parquet format.
- Stores data in Hadoop Distributed File System (HDFS).
- Runs MapReduce jobs to compute metrics (e.g., average patient age).
- Creates Hive external tables for querying.
- Performs analytical queries (e.g., admission counts by date).

### Prerequisites
- **Docker**: Installed with Docker Compose ([download](https://www.docker.com/products/docker-desktop/)).
- **Python 3.6+**: For data cleaning with Pandas.
- **Java**: For compiling and running MapReduce jobs.
- **Dependencies**:
  ```bash
  pip install pandas pyarrow
  ```
- **Dataset**: MIMIC-III Clinical Database (demo version) from [PhysioNet](https://physionet.org/content/mimiciii-demo/1.4/).
- **Environment**: System with sufficient resources to run Docker containers.
- **Tools**: VS Code (or similar) for editing Python/Java scripts.

## Setup
1. **Install Docker**:
   - Download and install Docker Desktop from [docker.com](https://www.docker.com/products/docker-desktop/).
   - Ensure Docker Compose is included.

2. **Clone the Repository**:
   ```bash
   git clone https://github.com/abdelrhmanmousa/Healthcare-Data-Analytics-Pipeline
   cd Healthcare-Data-Analytics-Pipeline
   ```

3. **Start Docker Containers**:
   - Run Docker Compose to set up Hadoop, Hive, and Spark containers:
     ```bash
     docker-compose up -d
     ```
   - Verify containers are running:
     ```bash
     docker ps
     ```
   - Note the `namenode` and `hive-server` container names (e.g., `healthcare-data-analytics-pipeline_namenode_1`, `healthcare-data-analytics-pipeline_hive-server_1`).

4. **Download MIMIC-III Dataset**:
   - Register at [PhysioNet](https://physionet.org/content/mimiciii-demo/1.4/) and download the demo dataset.
   - Extract the following files to a local directory (e.g., `data/mimic-iii/`):
     - `ADMISSIONS.csv`
     - `CALLOUT.csv`
     - `ICUSTAYS.csv`
     - `LABEVENTS.csv`
     - `PATIENTS.csv`
     - `DIAGNOSES_ICD.csv`
     - `D_ICD_DIAGNOSES.csv`

5. **Install Python Dependencies**:
   ```bash
   pip install pandas pyarrow
   ```

## Running the Pipeline
### Step 1: Clean Data with Pandas
1. Open VS Code and create a Python script (e.g., `src/clean_data.py`) to clean the MIMIC-III CSV files.
2. Example cleaning script:
   ```python
   import pandas as pd
   import os

   input_dir = "data/mimic-iii"
   output_dir = "data/mimic-iii/cleaned"
   os.makedirs(output_dir, exist_ok=True)

   files = ["ADMISSIONS.csv", "CALLOUT.csv", "ICUSTAYS.csv", "LABEVENTS.csv",
            "PATIENTS.csv", "DIAGNOSES_ICD.csv", "D_ICD_DIAGNOSES.csv"]

   for file in files:
       df = pd.read_csv(f"{input_dir}/{file}")
       # Example cleaning: Remove missing values, convert dates, etc.
       df = df.dropna()
       # Save as Parquet
       df.to_parquet(f"{output_dir}/{file.replace('.csv', '.parquet')}")
       print(f"Cleaned and saved {file} as Parquet")
   ```
3. Run the script:
   ```bash
   python src/clean_data.py
   ```
4. **Output**: Parquet files in `data/mimic-iii/cleaned/`.

### Step 2: Copy Parquet Files to HDFS
1. Copy Parquet files to the `namenode` container:
   ```bash
   docker cp data/mimic-iii/cleaned/ADMISSIONS.parquet healthcare-data-analytics-pipeline_namenode_1:/hadoop/dfs/name/
   docker cp data/mimic-iii/cleaned/CALLOUT.parquet healthcare-data-analytics-pipeline_namenode_1:/hadoop/dfs/name/
   docker cp data/mimic-iii/cleaned/ICUSTAYS.parquet healthcare-data-analytics-pipeline_namenode_1:/hadoop/dfs/name/
   docker cp data/mimic-iii/cleaned/LABEVENTS.parquet healthcare-data-analytics-pipeline_namenode_1:/hadoop/dfs/name/
   docker cp data/mimic-iii/cleaned/PATIENTS.parquet healthcare-data-analytics-pipeline_namenode_1:/hadoop/dfs/name/
   docker cp data/mimic-iii/cleaned/DIAGNOSES_ICD.parquet healthcare-data-analytics-pipeline_namenode_1:/hadoop/dfs/name/
   docker cp data/mimic-iii/cleaned/D_ICD_DIAGNOSES.parquet healthcare-data-analytics-pipeline_namenode_1:/hadoop/dfs/name/
   ```
2. Access the `namenode` container:
   ```bash
   docker exec -it healthcare-data-analytics-pipeline_namenode_1 bash
   ```
3. Upload Parquet files to HDFS:
   ```bash
   hdfs dfs -mkdir -p /user/hadoop/mimic
   hdfs dfs -put /hadoop/dfs/name/*.parquet /user/hadoop/mimic/
   ```
4. Verify files in HDFS:
   ```bash
   hdfs dfs -ls /user/hadoop/mimic
   ```

### Step 3: Run MapReduce Job for Average Age
1. Create MapReduce Java files in VS Code under `src/`:
   - `PatientAgeMapper.java`
   - `AgeReducer.java`
   - `AgeDriver.java`
2. Example `PatientAgeMapper.java`:
   ```java
   import org.apache.hadoop.io.IntWritable;
   import org.apache.hadoop.io.LongWritable;
   import org.apache.hadoop.io.Text;
   import org.apache.hadoop.mapreduce.Mapper;
   import java.io.IOException;
   import java.time.LocalDate;
   import java.time.Period;

   public class PatientAgeMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
       public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
           String[] fields = value.toString().split(",");
           String dob = fields[3]; // DOB in PATIENTS.csv (format: YYYY-MM-DD)
           try {
               LocalDate birthDate = LocalDate.parse(dob);
               LocalDate currentDate = LocalDate.now();
               int age = Period.between(birthDate, currentDate).getYears();
               context.write(new Text("age"), new IntWritable(age));
           } catch (Exception e) {
               // Skip invalid dates
           }
       }
   }
   ```
3. Example `AgeReducer.java`:
   ```java
   import org.apache.hadoop.io.IntWritable;
   import org.apache.hadoop.io.Text;
   import org.apache.hadoop.mapreduce.Reducer;
   import java.io.IOException;

   public class AgeReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
       public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
           int sum = 0;
           int count = 0;
           for (IntWritable value : values) {
               sum += value.get();
               count++;
           }
           int average = count > 0 ? sum / count : 0;
           context.write(key, new IntWritable(average));
       }
   }
   ```
4. Example `AgeDriver.java`:
   ```java
   import org.apache.hadoop.conf.Configuration;
   import org.apache.hadoop.fs.Path;
   import org.apache.hadoop.io.IntWritable;
   import org.apache.hadoop.io.Text;
   import org.apache.hadoop.mapreduce.Job;
   import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
   import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

   public class AgeDriver {
       public static void main(String[] args) throws Exception {
           Configuration conf = new Configuration();
           Job job = Job.getInstance(conf, "Patient Age Average");
           job.setJarByClass(AgeDriver.class);
           job.setMapperClass(PatientAgeMapper.class);
           job.setReducerClass(AgeReducer.class);
           job.setOutputKeyClass(Text.class);
           job.setOutputValueClass(IntWritable.class);
           FileInputFormat.addInputPath(job, new Path(args[0]));
           FileOutputFormat.setOutputPath(job, new Path(args[1]));
           System.exit(job.waitForCompletion(true) ? 0 : 1);
       }
   }
   ```
5. Copy Java files to the `namenode` container:
   ```bash
   docker cp src/PatientAgeMapper.java healthcare-data-analytics-pipeline_namenode_1:/hadoop/dfs/name/
   docker cp src/AgeReducer.java healthcare-data-analytics-pipeline_namenode_1:/hadoop/dfs/name/
   docker cp src/AgeDriver.java healthcare-data-analytics-pipeline_namenode_1:/hadoop/dfs/name/
   ```
6. Copy `cleaned_PATIENTS.parquet` to HDFS:
   ```bash
   docker exec -it healthcare-data-analytics-pipeline_namenode_1 bash
   hdfs dfs -put /hadoop/dfs/name/cleaned_PATIENTS.parquet /user/hadoop/mimic/
   ```
7. Compile and create the JAR:
   ```bash
   docker exec -it healthcare-data-analytics-pipeline_namenode_1 bash
   cd /hadoop/dfs/name
   javac -classpath $(hadoop classpath) -d . PatientAgeMapper.java AgeReducer.java AgeDriver.java
   jar -cvf average-age-job.jar *.class
   ```
8. Run the MapReduce job:
   ```bash
   hadoop jar average-age-job.jar AgeDriver /user/hadoop/mimic/cleaned_PATIENTS.parquet /user/hadoop/patients_avg_age_output
   ```
9. Check results:
   ```bash
   hdfs dfs -ls /user/hadoop/patients_avg_age_output
   hdfs dfs -cat /user/hadoop/patients_avg_age_output/part-r-00000
   ```

### Step 4: Set Up Hive Tables
1. Access the `hive-server` container:
   ```bash
   docker exec -it healthcare-data-analytics-pipeline_hive-server_1 bash
   ```
2. Start Hive CLI:
   ```bash
   hive
   ```
3. Create external tables for Parquet files (example for `d_icd_diagnoses`):
   ```sql
   CREATE EXTERNAL TABLE d_icd_diagnoses (
       row_id BIGINT,
       icd9_code STRING,
       short_title STRING,
       long_title STRING
   )
   STORED AS PARQUET
   LOCATION 'hdfs://namenode:9000/user/hadoop/mimic/D_ICD_DIAGNOSES/';
   ```
4. Create tables for other files (adjust schemas as needed):
   - Example for `admissions`:
     ```sql
     CREATE EXTERNAL TABLE admissions (
         row_id BIGINT,
         subject_id BIGINT,
         hadm_id BIGINT,
         admittime TIMESTAMP,
         dischtime TIMESTAMP,
         admission_type STRING,
         admission_location STRING,
         discharge_location STRING,
         insurance STRING,
         language STRING,
         religion STRING,
         marital_status STRING,
         ethnicity STRING,
         diagnosis STRING
     )
     STORED AS PARQUET
     LOCATION 'hdfs://namenode:9000/user/hadoop/mimic/ADMISSIONS/';
     ```
   - Example for `patients`:
     ```sql
     CREATE EXTERNAL TABLE patients (
         row_id BIGINT,
         subject_id BIGINT,
         gender STRING,
         dob TIMESTAMP,
         dod TIMESTAMP,
         dod_hosp TIMESTAMP,
         dod_ssn TIMESTAMP,
         expire_flag INT
     )
     STORED AS PARQUET
     LOCATION 'hdfs://namenode:9000/user/hadoop/mimic/PATIENTS/';
     ```

### Step 5: Run Hive Queries
1. In the Hive CLI, run analytical queries:
   ```sql
   SELECT
       DATE_FORMAT(admittime, 'yyyy-MM-dd') AS admission_date,
       COUNT(*) AS admission_count
   FROM admissions
   GROUP BY DATE_FORMAT(admittime, 'yyyy-MM-dd')
   ORDER BY admission_date;
   ```
2. Save results (optional):
   ```sql
   INSERT OVERWRITE DIRECTORY 'hdfs://namenode:9000/user/hadoop/mimic/results/admission_counts'
   STORED AS PARQUET
   SELECT
       DATE_FORMAT(admittime, 'yyyy-MM-dd') AS admission_date,
       COUNT(*) AS admission_count
   FROM admissions
   GROUP BY DATE_FORMAT(admittime, 'yyyy-MM-dd')
   ORDER BY admission_date;
   ```
3. View results:
   ```bash
   hdfs dfs -cat /user/hadoop/mimic/results/admission_counts/*
   ```

## Expected Output
- **Parquet Files**: Cleaned data in `data/mimic-iii/cleaned/` and HDFS (`/user/hadoop/mimic/`).
- **MapReduce**: Average patient age in `/user/hadoop/patients_avg_age_output/part-r-00000`.
- **Hive**: Tables in `default` database (e.g., `admissions`, `d_icd_diagnoses`, `patients`).
- **Query Results**: Admission counts or other metrics in HDFS or console output.

## Troubleshooting
- **Docker Issues**:
  - Ensure containers are running: `docker ps`.
  - Check logs: `docker logs healthcare-data-analytics-pipeline_namenode_1`.
- **HDFS Errors**:
  - Verify HDFS paths: `hdfs dfs -ls /user/hadoop/mimic`.
  - Ensure Namenode is active: `docker exec -it healthcare-data-analytics-pipeline_namenode_1 hdfs dfsadmin -report`.
- **Hive Errors**:
  - Confirm Hive metastore is running: `docker exec -it healthcare-data-analytics-pipeline_hive-server_1 hive --service metastore`.
  - Validate table schemas match Parquet files.
- **MapReduce Errors**:
  - Check Java compilation: Ensure `hadoop classpath` is correct.
  - Verify input file: `hdfs dfs -ls /user/hadoop/mimic/cleaned_PATIENTS.parquet`.
- **Data Issues**:
  - Ensure CSV files are valid and Parquet conversion succeeded.
  - Check for missing values or schema mismatches.

## Notes
- Customize `clean_data.py` for specific cleaning (e.g., handling dates, missing values).
- Update Hive table schemas to match your Parquet files.
- Contact your system administrator for Docker or Hadoop issues.
- Add new Hive queries or MapReduce jobs for additional analyses.
