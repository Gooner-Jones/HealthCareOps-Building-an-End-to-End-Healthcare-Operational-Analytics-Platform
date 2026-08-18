# HealthCareOps: An End-to-End Healthcare Operational Analytics Platform
This project presents a complete, end-to-end data platform built for a simulated South African hospital network. It solves critical operational and population-health questions by processing synthetic Electronic Medical Record (EMR) data through a full Medallion architecture, culminating in a rich BI layer and a natural-language query interface.

The platform was developed on Databricks Free Edition and uses Delta Lake, Unity Catalog, and Power BI to provide a unified, governed, and insightful view of hospital operations.

# 🚀 Key Features
Full Medallion Architecture (Bronze → Silver → Gold): A governed data pipeline built on Unity Catalog and Delta Lake, implementing a robust dimensional model with deterministic, root-caused key design (md5() surrogate keys) validated with left-anti joins to ensure data integrity.

Realistic Data Lifecycles: A daily incremental pipeline simulates the dynamic nature of a hospital. It handles genuine record lifecycles, including SCD Type 2 for patient data and CDC-style update-in-place for admissions, beds, and resources.

# Dual BI Layer:

A Databricks SQL Dashboard for native, high-performance KPI monitoring.

A Power BI Report (Fabric Experience) with four dedicated report pages, validated dimension relationships, and sophisticated DAX measures for deep analytical insight.

Natural-Language Querying: A Databricks Genie Agent allows users to ask business questions in plain English, grounded with explicit schema, business logic, and calculation instructions for accurate, trustworthy results.

# 🔧 Technology Stack
Layer	Tool
Compute & Storage	Databricks Free Edition
Storage Format	Delta Lake
Governance / Catalog	Unity Catalog
Orchestration	Databricks Workflows
Native BI	Databricks SQL
BI & Reporting	Power BI (Fabric Experience)
Data Generation	Python, PySpark, Faker
Natural-Language Querying	Databricks Genie (Genie Agent)
# 🎯 Business Problems Addressed
Bed Capacity & Utilisation: Identify wards running near or over capacity, understand bed-type distribution, and analyse average bed length of stay.

Length-of-Stay & Readmission Patterns: Pinpoint facilities and diagnosis groups driving extended stays and repeat admissions to improve care pathways.

Resource Utilisation: Analyse the efficiency of staff, equipment, and consumable allocation across facilities, wards, and resource categories.

Population Health Equity: Explore comorbidity burden (with a focus on HIV/TB/diabetes co-infection, a critical South African health priority) and funding-source disparities (public vs. private) across demographic groups.

# 💎 Gold Layer: Dimensional Model Design
The Gold layer is built on a star schema to enable fast, flexible reporting. A key design principle is the use of deterministic surrogate keys (md5(natural_key_name)) to ensure that fact and dimension joins are provably reliable.

Star Schema Overview


<img width="4108" height="904" alt="deepseek_mermaid_20260818_1f67ba" src="https://github.com/user-attachments/assets/f58a4c5a-7034-423d-843b-6fe8a8b406b5" />










Fact Tables & Key Metrics
Fact Table	Key Metrics
fact_admissions	Admissions volume & trend, ICU admission rate, readmission rate by facility, LOS distribution, weekend vs. weekday load, admission type mix.
fact_bed_utilisation	Bed occupancy rate by ward/facility, bed status breakdown (Available/Occupied), bed type distribution, bed-level LOS.
fact_resource_allocation	Resource utilisation rate by category/ward/facility, top resource types by assignment volume, assignment duration profile.
fact_patient_demographics	High-risk comorbidity rate by demographic/age band, funding mix (public/private/RAF/uninsured), population age/language/marital distribution.
# 📊 Power BI Report: Admissions Overview
This page delivers a high-level view of patient flow, highlighting trends and resource demands.

[https://images/admissions_overview.jpg](https://github.com/Gooner-Jones/HealthCareOps-Building-an-End-to-End-Healthcare-Operational-Analytics-Platform/blob/main/images/admissions_overview.jpg)

Key Metrics (Card Visuals):

Total Admissions: 1,264

Readmission Rate: 83.7% (A significant indicator of care quality and/or patient complexity)

Weekend Admission %: 27.6%

Avg Length of Stay: 7.6 Days

Key Visuals & Insights:

Admissions Trend: A monthly bar chart shows total admissions peaking in August at over 200. The ICU admissions trend line follows a similar pattern, peaking at approximately 25 admissions in August.

Admissions by Ward: The General Medical ward handles the highest volume (~500 admissions), followed by Outpatient (~320) and Maternity (~175).

Diagnosis Group Analysis: A horizontal bar chart reveals the top diagnoses. Infectious Disease (314 cases) and Cardiovascular (160) are the leading reasons for admission, together accounting for nearly 40% of all cases.

Admission Type: Emergency admissions dominate at nearly 75% of all cases, indicating high acuity and demand on emergency services.

Length of Stay Distribution: Medium-stay admissions (4-7 days) are the most common at 496 cases, followed by short-stay (2-3 days) at 273 cases.

Readmission Rate by Facility: The readmission rate is consistently high across most facilities, hovering between 80% and 87%, indicating a systemic issue or a high-risk patient population across the network.

# 📊 Power BI Report: Bed Utilisation Overview
This page provides visibility into bed capacity and occupancy patterns across the hospital network.

Key Metrics (Card Visuals):

Total Bed Assignments: 35K

Currently Occupied: 12.3K

Avg Bed LOS: 10.35 Days

Bed Occupancy Rate: 448% (Note: This metric requires grouping by ward to be meaningful, as a hospital-wide average can exceed 100% when summing multiple wards.)

Key Visuals & Insights:

Bed Occupancy Rate by Ward: A horizontal bar chart shows that ICU, Cardiology, and Isolation/Infectious wards are operating at the highest occupancy rates, all exceeding 600%. This highlights critical capacity constraints in specialised care units.

Bed Assignment Distribution: A donut chart shows that 64.98% of beds are occupied, while 35.02% are available.

Average Bed LOS by Ward: Outpatient wards have the highest average bed LOS at 11.30 days, followed by Paediatrics at 10.78 days.

Total Bed Assignments by Bed Type: General Ward Bed accounts for the majority of assignments at approximately 12K, followed by High Care Bed at roughly 10K.

Monthly Bed Occupancy Rate: The occupancy rate shows a steady upward trend from February (around 80%) to a peak in August (nearly 300%), indicating increasing pressure on bed capacity over time.

# 📊 Power BI Report: Resource Allocation
This page provides visibility into how efficiently staff, equipment, and consumables are deployed across the network.

[https://images/resource_allocation.jpg](https://github.com/Gooner-Jones/HealthCareOps-Building-an-End-to-End-Healthcare-Operational-Analytics-Platform/blob/main/images/resource_allocation.jpg)

Key Metrics (Card Visuals):

Total Resource Assignments: 200K

Resource Utilisation Rate: 50.0% (Indicates an equal balance of availability and assignment)

Avg Assignment Duration: 547.5 Days

Resources In Use: 100K

Key Visuals & Insights:

Resource Utilisation by Category: Pharmacy & Consumables (approx. 55%) and Staff (approx. 50%) show the highest utilisation rates, while Support & Logistics (approx. 35%) shows the lowest.

Resource Utilisation by Facility: Utilisation rates are relatively consistent across facilities, ranging from approximately 45% to 55%.

Resource Breakdown by Type: A bar chart shows total assignments by resource type, with Midwife (approx. 8.5K), Paramedic (approx. 8K), and Occupational Therapist leading in assignment volume.

Assignment Duration: An overwhelming 93.9% of assignments are classified as "Long-term" (30+ days), indicating a stable staff and resource pool.

Resource Status: A pie chart shows a 50/50 split between Available and Assigned resources, consistent with the 50% utilisation rate.

# 📊 Power BI Report: Population Health
This page focuses on the demographic and health equity aspects of the patient population, providing insights into resource planning and community health needs.

[https://images/population_health.jpg](https://github.com/Gooner-Jones/HealthCareOps-Building-an-End-to-End-Healthcare-Operational-Analytics-Platform/blob/main/images/population_health.jpg)

Key Metrics (Card Visuals):

Total Patients: 35K

Average Patient Age: 47.6 Years

Public Funded Patients %: 44.9%

High-Risk Comorbidity Rate: 20.5% (defined by co-occurring HIV/AIDS, Tuberculosis, and Type 2 Diabetes Mellitus)

Key Visuals & Insights:

High-Risk Comorbidity Rate by Demographics:

Age Band: The risk is highest in the 70+ age group (over 50%) and lowest in the 0-10 age group (under 5%).

Marital Status: The rate is relatively consistent across all marital statuses, hovering around 15-20%.

Public Funded Patients by Facility: Kalafong Provincial Tertiary Hospital has the highest proportion of publicly funded patients (approximately 65%), while private facilities like Mediclinic Kloof and Little Company of Mary Hospital have the lowest (approximately 30-35%).

Patient Marital Status: The majority of patients are Married (approx. 6K), followed by Single (approx. 5K).

Patient Age Distribution: The patient population is relatively evenly distributed across age bands, with a slight concentration in the 50-59 and 60-69 age groups.

# 🤖 Natural-Language Querying with Databricks Genie
A Databricks Genie Agent was created to democratise data access, allowing non-technical stakeholders to ask complex operational questions in plain English. The Agent is grounded with explicit instructions that define:

Schema Orientation: Correctly joining dimensions to fact tables and understanding which relationships don't exist (e.g., demographic fact has no date or ward relationship).

Calculation Rules: How to correctly calculate KPIs like the bed occupancy rate, which is only meaningful when grouped by a specific ward.

Business Definitions: What constitutes a "high-risk comorbidity" (e.g., HIV/AIDS & TB co-occurrence) and the meaning of funding categories.

## Example Questions the Genie Agent Can Answer:

"What is the bed occupancy rate for the ICU ward?"

"Show me the readmission rate by facility."

"What is the average length of stay for patients with cardiovascular diagnoses?"

"How many patients are publicly funded?"

"What are the top 5 resource types by assignment volume?"

### Note: The Genie Agent is explicitly instructed to state that all data is synthetic, ensuring the generated insights are not misrepresented as real-world health statistics.

[https://Genie_Diagnosis_Groups.jpg](https://github.com/Gooner-Jones/HealthCareOps-Building-an-End-to-End-Healthcare-Operational-Analytics-Platform/blob/main/Genie_Diagnosis_Groups.pdf)

# 🏗️ Architecture and Orchestration
The entire pipeline is orchestrated through Databricks Workflows.

One-time Backfill Job: An 18-task job to populate the initial dataset.

daily_incremental_pipeline: A recurring daily job that simulates ongoing hospital activity. Each source follows a CDC-style lifecycle pattern, updating records in place (e.g., an admission gets discharged) rather than just appending new data.

# 🧠 Key Design Principles
Fix Data at the Source: Data quality and modeling defects are fixed at the Bronze (source) layer, ensuring downstream layers are built on a solid foundation.

Deterministic Keys: All dimension keys are derived deterministically (md5()), making joins provably reliable and preventing left-anti join failures.

Validate Everything: Every dimension and fact table is validated with an actual query (left-anti joins) to confirm data integrity before moving to the next step.

Realistic Growth: The daily incremental pipeline ensures the dataset is genuinely dynamic and grows over time, mimicking a real production environment.
