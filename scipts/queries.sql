-- 1. Bed Occupancy Rate by Ward
SELECT
    w.ward_name,
    COUNT(*) AS total_bed_assignments,
    SUM(CASE WHEN b.bed_status = 'Occupied' THEN 1 ELSE 0 END) AS currently_occupied,
    ROUND(SUM(CASE WHEN b.bed_status = 'Occupied' THEN 1 ELSE 0 END) * 100.0 / w.ward_capacity, 1) AS occupancy_rate_pct
FROM hospital_analytics.03_gold.fact_bed_utilisation b
JOIN hospital_analytics.03_gold.dim_ward w ON b.ward_sk = w.ward_sk
GROUP BY w.ward_name, w.ward_capacity
ORDER BY occupancy_rate_pct DESC;


-- 2. Admissions Trend Over Time (monthly)
SELECT
    d.year,
    d.month,
    d.month_name,
    COUNT(*) AS admission_count,
    SUM(CASE WHEN a.icu_flag = 'Y' THEN 1 ELSE 0 END) AS icu_admissions
FROM hospital_analytics.03_gold.fact_admissions a
JOIN hospital_analytics.03_gold.dim_date d ON a.admission_date_sk = d.date_sk
GROUP BY d.year, d.month, d.month_name
ORDER BY d.year, d.month;


-- 3. Length of Stay Distribution
SELECT
    los_category,
    COUNT(*) AS admission_count,
    ROUND(AVG(length_of_stay_days), 1) AS avg_los_days
FROM hospital_analytics.03_gold.fact_admissions
GROUP BY los_category
ORDER BY
    CASE los_category
        WHEN 'Same Day' THEN 1
        WHEN 'Short Stay (2-3 days)' THEN 2
        WHEN 'Medium Stay (4-7 days)' THEN 3
        WHEN 'Long Stay (8-14 days)' THEN 4
        ELSE 5
    END;


-- 4. Readmission Rate by Facility
SELECT
    f.facility_name,
    COUNT(*) AS total_admissions,
    SUM(CASE WHEN a.is_readmission_validated = 'Y' THEN 1 ELSE 0 END) AS readmissions,
    ROUND(SUM(CASE WHEN a.is_readmission_validated = 'Y' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS readmission_rate_pct
FROM hospital_analytics.03_gold.fact_admissions a
JOIN hospital_analytics.03_gold.dim_facility f ON a.facility_sk = f.facility_sk
GROUP BY f.facility_name
ORDER BY readmission_rate_pct DESC;


-- 5. Resource Utilisation by Category
SELECT
    resource_category,
    COUNT(*) AS total_assignments,
    SUM(CASE WHEN resource_status = 'Assigned' THEN 1 ELSE 0 END) AS currently_assigned,
    ROUND(SUM(CASE WHEN resource_status = 'Assigned' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS in_use_rate_pct
FROM hospital_analytics.03_gold.fact_resource_allocation
GROUP BY resource_category
ORDER BY in_use_rate_pct DESC;


-- 6. Population Health: High-Risk Comorbidity Burden by Demographic
SELECT
    demographic,
    age_band,
    COUNT(*) AS patient_count,
    SUM(CASE WHEN high_risk_comorbidity_flag = 'Y' THEN 1 ELSE 0 END) AS high_risk_count,
    ROUND(SUM(CASE WHEN high_risk_comorbidity_flag = 'Y' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS high_risk_pct
FROM hospital_analytics.03_gold.fact_patient_demographics
GROUP BY demographic, age_band
ORDER BY high_risk_pct DESC;


-- 7. Funding Mix (Public vs Private vs RAF vs Uninsured)
SELECT
    funding_category,
    COUNT(*) AS patient_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) AS pct_of_total
FROM hospital_analytics.03_gold.fact_patient_demographics
GROUP BY funding_category
ORDER BY patient_count DESC;


-- 8. Weekend vs Weekday Admission Load
SELECT
    is_weekend_admission,
    COUNT(*) AS admission_count,
    ROUND(AVG(length_of_stay_days), 1) AS avg_los
FROM hospital_analytics.03_gold.fact_admissions
GROUP BY is_weekend_admission;
