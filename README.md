# HealthCareOps: Building an End-to-End Healthcare Operational Analytics Platform

**A full Medallion-architecture data platform for a simulated South African hospital network, built end-to-end on Databricks Free Edition — from raw synthetic EMR data through a governed star schema to two BI layers and a natural-language query agent.**

- 🏗️ **Bronze → Silver → Gold** on Unity Catalog + Delta Lake, with a root-caused dimensional key design (deterministic `md5()` surrogate keys, validated with left-anti joins — not name-based workarounds)
- 🔄 **Daily incremental pipeline** with genuine record lifecycles — SCD Type 2 for patients, CDC-style update-in-place for admissions, beds, and resources
- 📊 **Two BI layers** — a native Databricks SQL dashboard and a Power BI report (Fabric Experience) with dedicated DAX measures across four report pages
- 🤖 **Natural-language querying** via Databricks Genie, grounded with explicit schema and business-logic instructions

## Project Scenario

This project simulates a South African (Gauteng/Tshwane) multi-facility hospital network — seven facilities spanning academic/tertiary, public district, and private hospitals — that needs a unified analytics platform to answer operational and population-health questions across patient care, bed capacity, staffing/equipment allocation, and admissions patterns.

The platform is built end-to-end on Databricks Free Edition, using a full Medallion (Bronze/Silver/Gold) architecture, Unity Catalog governance, and Delta Lake as the storage format throughout — with a daily incremental pipeline that keeps the dataset genuinely growing over time rather than remaining a static one-time load.

## Technology Stack

| Layer | Tool |
|---|---|
| Compute & Storage | Databricks Free Edition |
| Storage Format | Delta Lake |
| Governance / Catalog | Unity Catalog |
| Orchestration | Databricks Workflows |
| Native BI / Dashboards | Databricks SQL |
| Data Generation | Python, PySpark, Faker |
| BI & Reporting | Power BI (Fabric Experience) |
| Natural-Language Querying | Databricks Genie (Genie Agent) |

The reporting layer is published as a Power BI report within the Fabric Experience. Natural-language querying over the Gold layer is handled by a Databricks Genie Agent — Fabric Data Agents were evaluated first but ruled out, since they require paid Fabric capacity (F2+) which isn't available on a Fabric trial; Genie achieves the same goal natively within Databricks Free Edition at no additional cost.

## Key Business Problems

- **Bed capacity visibility** — which wards are running near or over capacity, and when.
- **Length-of-stay and readmission patterns** — which facilities and diagnoses drive extended stays and repeat admissions.
- **Resource utilisation** — how efficiently staff, equipment, and consumables are allocated across facilities and wards.
- **Population health equity** — comorbidity burden (particularly HIV/TB/diabetes co-infection, a genuine SA public health priority) across demographic groups, and funding-source disparities (public vs. private vs. RAF vs. uninsured).
- **Operational reporting latency** — without a well-designed dimensional model, dashboards and reports risk becoming unreliable as new data arrives, undermining trust in downstream reporting.
  
## Key Objections (design challenges addressed)

- **Ward and diagnosis dimension joins needed to be provably reliable.** A common pitfall in dimensional modeling is a dimension table using independently assigned codes that don't match the natural-key values held in fact-level source data. This project avoided that pitfall from the start by deriving every dimension's surrogate key deterministically from its natural key (see *Gold Layer Design* below), and validating every join with an actual left-anti join query rather than assuming correctness.
- **Doctor and facility references needed to be genuine entities, not incidental fields.** Building a meaningful `dim_doctor` required a real, fixed roster (specialty, HPCSA number, experience) rather than treating doctor IDs as disposable per-record labels. Similarly, `resource_allocation` needed an explicit facility relationship to support facility-level rollups.
- **Partitioning choices needed to reflect Delta Lake's actual strengths.** Low-cardinality partition columns (e.g. `demographic`, `admission_year`/`admission_month`) were deliberately evaluated and rejected in favour of Delta's native data skipping, which avoids the small-file overhead that static partitioning introduces at this scale.
- **The dataset needed to grow, not sit static.** A daily incremental pipeline was designed with genuine record lifecycles — SCD Type 2 for patient updates, and CDC-style update-in-place logic for admissions, bed allocation, and resource assignment — rather than a single one-time load.

## Source Systems Overview

Four synthetic Bronze source generators, designed to mimic realistic South African hospital EMR data:

| Source | Description |
|---|---|
| `patient_info` | Patient demographics, SA ID numbers, diagnoses, funding/payment details, facility assignment |
| `admission_discharge` | Admission and discharge events, referral sources, LOS, readmission tracking |
| `resource_allocation` | Equipment, staff, and consumable assignments across 44 resource types in 6 categories |
| `bed_allocation` | Bed-level occupancy, derived from and kept consistent with admission/discharge events |

---

## Notebooks

### Bronze Layer Ingestion

All four Bronze notebooks are structured around a **shared setup cell** (catalog/schema creation, reference data, helper functions) followed by independent per-table generator cells, keeping each table's generation logic self-contained and re-runnable on its own.

**Two data gaps were identified and fixed at the source, not papered over downstream:**
1. `resource_allocation` initially had no `facility_name` field — retrofitted into the Bronze generator (weighted random assignment across the seven facilities) so facility-level rollups are possible.
2. Doctor IDs (`admitting_doctor_id`, `discharge_doctor_id`) were initially generated randomly per-record with no fixed roster, making a genuine doctor dimension impossible. Fixed by building a 200-doctor roster (specialty, HPCSA number, years of experience) as `dim_doctor` in Gold *first*, then having `admission_discharge` Bronze sample from that roster — an intentional Bronze-depends-on-Gold reference-data relationship, since the roster is genuinely master data rather than a raw ingestion event.

A real bug was also caught and fixed during development: a trailing comma in `resource_id = str(uuid.uuid4()),` was silently turning `resource_id` into a one-element tuple instead of a string.

### Silver Layer Transformations

Each Silver notebook follows a consistent pattern: trim/standardise text fields, derive categorical/banding columns useful for Gold aggregation, apply a cascading data-quality flag (`FAIL` → quarantine, `WARN`/`PASS` → pass through), and split output into `quarantine` and `silver` schemas.

- **`patient_info`** — age banding, SA ID number validation, funding category grouping, HIV/TB/diabetes comorbidity and high-risk flags, SCD Type 2 scaffolding (`effective_start_date`, `effective_end_date`, `is_current`)
- **`admission_discharge`** — LOS categorisation, weekend-admission flag, combined admission/discharge datetime, readmission validation; an inverted `admission_status` logic bug was caught and fixed during testing
- **`resource_allocation`** — added a `resource_category` column (Beds & Accommodation / Medical Equipment / Staff / Theatre & Surgical / Pharmacy & Consumables / Support & Logistics) derived from `resource_type`, plus assignment duration and duration category
- **`bed_allocation`** — re-derives `length_of_stay_days` and `bed_status` from dates rather than trusting Bronze's versions, since Silver is where data quality is actually re-validated, not assumed

A deliberate, project-wide decision: **low-cardinality partitioning** (`demographic`, `admission_year`/`admission_month`) was evaluated and dropped in favour of Delta Lake's native data skipping, which is more effective at this scale and avoids small-file overhead.

### Gold Layer Design

#### The Core Design Principle: Deterministic Surrogate Keys

Every Gold dimension's surrogate key is derived as **`md5(natural_key_name)`** — computed identically on both the dimension and every fact table that references it. Because both sides derive the key the same way from the same source value, they cannot drift apart. This was validated at every step with left-anti joins between each fact/Silver source and its corresponding dimension, confirming zero unmatched keys throughout the build.

#### Star Schema Overview

**`fact_admissions`**
```
                          dim_date
                             │
dim_facility ──────── fact_admissions ──────── dim_ward
                             │                (admitting_ward_sk,
dim_doctor ──────────────────┤                 discharge_ward_sk)
    (admitting_doctor_sk,    │
     discharge_doctor_sk)    │
                       dim_diagnosis
```

**`fact_bed_utilisation`**
```
              dim_date
                 │
dim_ward ── fact_bed_utilisation ── dim_facility
```
*(`facility_name` is pulled in via a join back to `admission_discharge` Silver on `admission_id`, since `bed_allocation` doesn't carry it natively.)*

**`fact_resource_allocation`**
```
              dim_date
                 │
dim_ward ── fact_resource_allocation ── dim_facility
```

**`fact_patient_demographics`**
```
dim_diagnosis (primary + secondary) ── fact_patient_demographics ── dim_facility
                                               │
                                          dim_payment
```
*(No ward or date dimension — this fact table is patient-grain, not event-grain.)*

#### Key Metrics Each Fact Table Enables

| Fact Table | Key Metrics |
|---|---|
| `fact_admissions` | Admissions volume & trend, ICU admission rate, readmission rate by facility, LOS distribution, weekend vs. weekday load, admission type mix |
| `fact_bed_utilisation` | Bed occupancy rate by ward/facility, bed status breakdown, bed-level LOS, bed type distribution |
| `fact_resource_allocation` | Resource utilisation rate by category/ward/facility, top resource types by assignment volume, assignment duration profile |
| `fact_patient_demographics` | High-risk comorbidity rate by demographic/age band, funding mix (public/private/RAF/uninsured), population age/language/marital distribution |

#### Build Order

1. `dim_doctor` (must precede `admission_discharge` Bronze, which depends on the roster)
2. `dim_ward`, `dim_diagnosis`, `dim_facility`, `dim_payment`, `dim_date`
3. `fact_admissions`
4. `fact_bed_utilisation`
5. `fact_resource_allocation`
6. `fact_patient_demographics`

Every dimension and fact table was validated immediately after creation via left-anti join checks against its Silver source(s), confirming zero unmatched foreign keys before moving to the next build step.


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

## 🤖 Natural-Language Querying with Databricks Genie

### Why Genie Instead of Fabric Data Agents

Fabric Data Agents were the original plan for natural-language querying, but two constraints ruled them out: they only support Fabric-native data sources (no direct external database access — external data must be mirrored into Fabric first), and they require paid F2+ Fabric capacity, which isn't available on a Fabric trial license. Since the platform already runs natively on Databricks, **Databricks Genie** — specifically a **Genie Agent**, part of the Genie family of AI/BI natural-language experiences — provides the same capability without either constraint, and is included in Databricks Free Edition at no additional cost.

### Setup

A Genie Agent was created and pointed directly at the Gold layer (`hospital_analytics.03_gold`) — all six dimensions and four fact tables. Rather than relying on schema alone, the agent was given explicit written instructions covering the things Genie can't infer from column names and relationships:

- **Schema orientation** — which dimensions join to which fact tables, and critically, which joins *don't* exist (e.g. `fact_patient_demographics` has no `dim_ward` or `dim_date` relationship, despite being tempting to assume otherwise)
- **Calculation rules** — most importantly, that bed occupancy rate is only meaningful when grouped by ward (occupied beds ÷ that ward's `ward_capacity`), since a rate without a ward grouping would silently produce a meaningless number
- **Business definitions** — what `funding_category` groupings mean, what counts as a "high-risk comorbidity" (specifically HIV/AIDS, Tuberculosis, and Type 2 Diabetes Mellitus co-occurrence — a genuine SA public health priority, not a general severity indicator), and which fact table applies to which class of question (event-level questions → `fact_admissions`; patient-level questions → `fact_patient_demographics`)
- **A disclosure that all data is synthetic**, so the agent doesn't present generated figures as real-world South African health statistics

A set of example questions was also seeded to ground the agent's behaviour, covering occupancy, readmission, funding mix, resource utilisation, and demographic breakdowns — giving it concrete reference points across every fact table rather than only the most obvious ones.

### Validation Approach

Rather than trusting natural-language answers at face value, testing specifically targeted the trickiest instruction first — the ward-grouped occupancy rate calculation — since it requires a join plus a division rather than a simple aggregate, making it the best early signal of whether the agent was genuinely applying instructions rather than pattern-matching on table names. Genie's generated SQL was also inspected directly for questions involving the public/private facility classification, to confirm it was querying `facility_type` (the correct column per the instructions) rather than the more general `sector` field.

🤖 Genie chat output: [https://Genie_Diagnosis_Groups.jpg](https://github.com/Gooner-Jones/HealthCareOps-Building-an-End-to-End-Healthcare-Operational-Analytics-Platform/blob/main/Genie_Diagnosis_Groups.pdf)

### Note: The Genie Agent is explicitly instructed to state that all data is synthetic, ensuring the generated insights are not misrepresented as real-world health statistics.

## Job Orchestration

Two separate Databricks Workflows jobs:

### One-time backfill job
18 tasks with explicit dependency chains, covering the full Bronze → Silver → Gold build described above, run once to populate the initial dataset.

### `daily_incremental_pipeline`
A recurring daily job simulating ongoing hospital data flow — each source follows a **CDC-style lifecycle pattern** appropriate to how real EMR records behave (created, then updated in place as events occur — e.g. an admission gets discharged, a resource gets released), rather than pure append-only Bronze:

```
1. incremental_bronze_patient_info
   └─ 2. incremental_silver_patient_info_scd2
        └─ 3. incremental_bronze_admission_discharge   (needs current patient pool)
             ├─ 4. incremental_silver_admission_discharge
             │    └─ 5. incremental_bronze_bed_allocation   (reads today's changed admissions)
             │         └─ 6. incremental_silver_bed_allocation
             └─ (resource_allocation only needs patient_info, not admission chain)

7. incremental_bronze_resource_allocation   (depends on step 1, not step 3)
   └─ 8. incremental_silver_resource_allocation

9.  refresh_fact_patient_demographics    ← depends on 2
10. refresh_fact_admissions              ← depends on 4
11. refresh_fact_bed_utilisation         ← depends on 6, 4  (needs both — joins bed_allocation to admission_discharge for facility_name)
12. refresh_fact_resource_allocation     ← depends on 8
```

Steps 3 and 7 run in parallel, as do their downstream branches — both only depend on `patient_info` Bronze completing, not on each other.

**Notable fixes made during incremental build-out:**
- A marital-status correction in `patient_info` incremental Silver used a plain Python `random.choices()` call — evaluated once per batch instead of once per row — inside a Spark expression. Fixed using a stored `rand()` column, keeping the randomness genuinely per-row.
- Introducing `province`/`province_name` fields via `ALTER TABLE` was treated as deliberate schema evolution — legacy patient records are left `NULL` rather than backfilled, since this reflects genuinely new data capture starting at a point in time, not a modeling defect.
- `fact_patient_demographics` required an `is_current == "Y"` filter added before the incremental job could safely run repeatedly, to prevent SCD Type 2 historical/expired patient versions from producing duplicate rows in Gold.
- `admission_discharge` and `bed_allocation` Silver use plain **upsert** (`MERGE ... whenMatchedUpdateAll() ... whenNotMatchedInsertAll()`), not SCD Type 2 — appropriate since these tables were never designed with historical versioning, unlike `patient_info`.

---

## Solution Architecture

- **Compute & Storage:** Databricks Free Edition, Delta Lake throughout
- **Governance:** Unity Catalog, with schemas numerically prefixed (`01_bronze`, `02_silver`, `03_gold`) for correct catalog-browser ordering
- **Orchestration:** Databricks Workflows — one-time backfill job plus a daily incremental job with CDC-style lifecycle updates
- **BI Layer:**
  - Databricks SQL dashboard — 8 native KPI queries covering bed occupancy, admissions trend, LOS distribution, readmission rate, resource utilisation, comorbidity burden, funding mix, and weekend/weekday load
  - Power BI (Fabric Experience) — connected via the Databricks connector; four report pages (Admissions, Bed Utilisation, Resource Allocation, Population Health) with dedicated DAX measure sets, validated `*:1` fact-to-dimension relationships, and `dim_date` marked as a proper Date table for time intelligence; published within the Fabric Experience
  - Databricks Genie (Genie Agent) — natural-language querying directly over the Gold layer, grounded with explicit schema, calculation, and business-definition instructions
- **Key architectural principle carried through the whole build:** fix data-quality and modeling defects at their source layer, derive dimensional keys deterministically rather than assigning them independently, and treat every dimension/fact join as something to be validated with an actual query — not assumed correct.

# About Me
Hi, I'm Neo Jones — a Microsoft Certified Fabric Analytics Engineer Associate with a growing passion for data engineering. I thrive on solving real-world problems using SQL, and I'm especially excited about the capabilities of Microsoft Fabric and Databricks. From building data pipelines to exploring insights in the Lakehouse, I enjoy every part of working with data inside this evolving ecosystem.

This portfolio reflects my journey into the tech field, showcasing my hands-on projects, SQL skills, and curiosity-driven learning. Outside of work and code, I’m a proud single dad and a lifelong Arsenal supporter (yes, even through the tough seasons! But we are CHAMPIONS!!).

Thanks for checking out my work — let’s build something great with data.
