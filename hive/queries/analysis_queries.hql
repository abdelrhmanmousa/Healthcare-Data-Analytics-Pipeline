-- Insight 1: Emergency Department Usage
-- Analyze ED stay duration for admissions with ED data:

SELECT
    DATE_FORMAT(admittime, 'yyyy-MM-dd') AS admission_date,
    COUNT(*) AS admission_count
FROM admissions
GROUP BY DATE_FORMAT(admittime, 'yyyy-MM-dd')
ORDER BY admission_date;

-- Insight 2: Common Diagnoses
-- Identify the most frequent diagnoses:

SELECT
    diagnosis,
    COUNT(*) AS diagnosis_count
FROM admissions
GROUP BY diagnosis
ORDER BY diagnosis_count DESC
LIMIT 5;

-- Insight 3: Mortality Analysis
-- Examine mortality rates by admission type or location:

SELECT
    admission_type,
    COUNT(*) AS total_admissions,
    SUM(CASE WHEN hospital_expire_flag THEN 1 ELSE 0 END) AS deaths,
    (SUM(CASE WHEN hospital_expire_flag THEN 1 ELSE 0 END) * 100.0 / COUNT(*)) AS mortality_rate
FROM admissions
GROUP BY admission_type
ORDER BY mortality_rate DESC;

-- Insight 4: Length of Stay
-- Calculate the length of stay (in hours) for each admission

SELECT
    hadm_id,
    admission_type,
    (UNIX_TIMESTAMP(dischtime) - UNIX_TIMESTAMP(admittime)) / 3600 AS stay_hours
FROM admissions
WHERE dischtime IS NOT NULL
ORDER BY stay_hours DESC
LIMIT 10;

-- Insight 5: Time to Acknowledge Callouts
-- Calculate average time from callout creation to acknowledgment:

SELECT
    callout_service,
    AVG(UNIX_TIMESTAMP(acknowledgetime) - UNIX_TIMESTAMP(createtime)) / 3600 AS avg_acknowledge_hours
FROM callout
WHERE acknowledgetime IS NOT NULL
GROUP BY callout_service
ORDER BY avg_acknowledge_hours DESC;

-- Insight 6: Callout Outcomes by Admission Type
-- Join with admissions to analyze outcomes:

SELECT
    a.admission_type,
    c.callout_outcome,
    COUNT(*) AS callout_count
FROM admissions a
JOIN callout c ON a.hadm_id = c.hadm_id
GROUP BY a.admission_type, c.callout_outcome
ORDER BY callout_count DESC;

-- Insight 7: Callouts and Mortality
-- Analyze if callouts correlate with hospital deaths:

SELECT
    c.callout_service,
    COUNT(*) AS total_callouts,
    SUM(CASE WHEN a.hospital_expire_flag THEN 1 ELSE 0 END) AS deaths,
    (SUM(CASE WHEN a.hospital_expire_flag THEN 1 ELSE 0 END) * 100.0 / COUNT(*)) AS mortality_rate
FROM callout c
JOIN admissions a ON c.hadm_id = a.hadm_id
GROUP BY c.callout_service
ORDER BY mortality_rate DESC;



-- Insight 8: Most Common Diagnoses
-- Join diagnoses_icd with d_icd_diagnoses:

SELECT
    d.icd9_code,
    d.short_title,
    COUNT(*) AS diagnosis_count
FROM diagnoses_icd di
JOIN d_icd_diagnoses d ON di.icd9_code = d.icd9_code
GROUP BY d.icd9_code, d.short_title
ORDER BY diagnosis_count DESC
LIMIT 5;

-- Insight 9: Diagnoses Associated with Callouts
-- Join diagnoses_icd, callout, and admissions

SELECT
    d.short_title,
    c.callout_service,
    COUNT(*) AS callout_count
FROM diagnoses_icd di
JOIN d_icd_diagnoses d ON di.icd9_code = d.icd9_code
JOIN callout c ON di.hadm_id = c.hadm_id
GROUP BY d.short_title, c.callout_service
ORDER BY callout_count DESC
LIMIT 5;

-- Insight 10: Diagnoses in ICU Stays
-- Join with diagnoses_icd and d_icd_diagnoses:

SELECT
    d.short_title,
    COUNT(*) AS diagnosis_count,
    AVG(i.los) AS avg_los_days
FROM icustays i
JOIN diagnoses_icd di ON i.hadm_id = di.hadm_id
JOIN d_icd_diagnoses d ON di.icd9_code = d.icd9_code
GROUP BY d.short_title
ORDER BY diagnosis_count DESC
LIMIT 5;

-- Insight 11: Time to Callout in ICU
-- Time from ICU admission to callout:

SELECT
    i.first_careunit,
    AVG(UNIX_TIMESTAMP(c.createtime) - UNIX_TIMESTAMP(i.intime)) / 3600 AS avg_hours_to_callout
FROM icustays i
JOIN callout c ON i.hadm_id = c.hadm_id
WHERE c.createtime IS NOT NULL
GROUP BY i.first_careunit
ORDER BY avg_hours_to_callout DESC;

-- Insight 12: Abnormal Lab Results
-- Count abnormal results by care unit (join with icustays):

SELECT
    i.first_careunit,
    COUNT(*) AS total_tests,
    SUM(CASE WHEN l.flag = 'abnormal' THEN 1 ELSE 0 END) AS abnormal_count,
    (SUM(CASE WHEN l.flag = 'abnormal' THEN 1 ELSE 0 END) * 100.0 / COUNT(*)) AS abnormal_rate
FROM labevents l
JOIN icustays i ON l.hadm_id = i.hadm_id
GROUP BY i.first_careunit
ORDER BY abnormal_rate DESC;

-- Insight 13: Lab Results by Diagnosis
-- Join with diagnoses_icd and d_icd_diagnoses:

SELECT
    d.short_title,
    l.itemid,
    AVG(l.valuenum) AS avg_value,
    COUNT(*) AS test_count
FROM labevents l
JOIN diagnoses_icd di ON l.hadm_id = di.hadm_id
JOIN d_icd_diagnoses d ON di.icd9_code = d.icd9_code
WHERE l.valuenum IS NOT NULL
GROUP BY d.short_title, l.itemid
ORDER BY test_count DESC
LIMIT 5;

-- Lab Results by Gender
-- Analyze lab test results:

SELECT
    p.gender,
    l.itemid,
    AVG(l.valuenum) AS avg_value,
    COUNT(*) AS test_count
FROM patients p
JOIN admissions a ON p.subject_id = a.subject_id
JOIN labevents l ON a.hadm_id = l.hadm_id
WHERE l.valuenum IS NOT NULL
GROUP BY p.gender, l.itemid
ORDER BY test_count DESC
LIMIT 5;

-- ▪Average length of stay per diagnosis

SELECT
    d.short_title AS diagnosis,
    COUNT(*) AS diagnosis_count,
    AVG(i.los) AS avg_icu_los_days
FROM icustays i
JOIN diagnoses_icd di ON i.hadm_id = di.hadm_id
JOIN d_icd_diagnoses d ON di.icd9_code = d.icd9_code
JOIN admissions a ON i.hadm_id = a.hadm_id
WHERE i.los IS NOT NULL
GROUP BY d.short_title
ORDER BY avg_icu_los_days DESC
LIMIT 5;




-- ▪Distribution of ICU readmissions.

WITH icu_counts AS (
    SELECT
        i.hadm_id,
        i.subject_id,
        COUNT(DISTINCT i.icustay_id) AS icu_stay_count
    FROM icustays i
    GROUP BY i.hadm_id, i.subject_id
    HAVING COUNT(DISTINCT i.icustay_id) > 1
)
SELECT
    icu_stay_count AS number_of_icu_stays,
    COUNT(*) AS number_of_admissions,
    COUNT(DISTINCT subject_id) AS number_of_patients
FROM icu_counts
GROUP BY icu_stay_count
ORDER BY icu_stay_count;

-- ▪Mortality rates by demographic groups.

SELECT 
    p.gender,
    a.ethnicity,
    COUNT(*) AS total_admissions,
    SUM(CASE WHEN a.hospital_expire_flag THEN 1 ELSE 0 END) AS deaths,
    ROUND(100 * SUM(CASE WHEN a.hospital_expire_flag THEN 1 ELSE 0 END) / COUNT(*), 2) AS mortality_rate
FROM admissions a
JOIN patients p ON a.subject_id = p.subject_id
GROUP BY p.gender, a.ethnicity
ORDER BY mortality_rate DESC;