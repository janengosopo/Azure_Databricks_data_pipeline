# Azure-Databricks Data Pipeline

## What problem does it solve?

**Healthpoint** is a hypothetical healthcare group whose data is spread across multiple systems: patient, doctor and hospital records in PostgreSQL, and insurance and lab test data in Azure Storage.
This makes it difficult to get a complete view of patients, hospitals and lab results.

I built an automated data pipeline using Azure Data Factory and Databricks to bring this data together, clean it and turn it into analytics-ready data.
The resulting data products support use cases such as:
* Patient overview: Patient profiles, hospital visits, insurance coverage and abnormal lab results.
* Hospital performance: Daily patient volumes, lab tests and doctor counts.
* Lab trends: Monthly trends in abnormal and critical results by test type.

**Healthpoint** is a hypothetical healthcare group with data spreads over different places: a database (Neon PostgreSQL) with hospitals, doctors and patients, and files (CSV in Azure storage) with insurance providers and lab test results. Looking at this data together is hard because it is stored in different systems and formats.

This project brings all the data into one place every day, cleans it, and turns it into ready-to-use data products, for example:

* a full profile of every patient (age, hospital, insurance, how many lab tests, how many were abnormal or critical),
* a daily summary per hospital (how many tests, patients and doctors, and how this changes day to day),
* a monthly trend per lab test type (is the rate of abnormal results going up or down?).

## Technology stack

| Area | Technology |
| --- | --- |
| Orchestration | Azure Data Factory |
| Processing | Azure Databricks (PySpark, Spark SQL) |
| Storage | Azure Data Lake Storage Gen2 |
| Sources | Neon PostgreSQL (cloud database), CSV files in ADLS |
| Metadata and logging | Azure SQL Database |
| Secrets | Azure Key Vault |
| Alerts | Azure Logic App (e-mail) |
| Version control | GitHub (one repo for Databricks, one for ADF) |

## Architecture overview

![CareSync healthcare pipeline architecture](Architecture%20diagram.jpg)

## Design decisions

- **Metadata-driven:** One reusable pipeline handles multiple tables, with loading rules managed through a control table.
- **Incremental loading:** Watermarks track changes, so only new or updated data is processed.
- **Flexible loading:** Supports FULL and MERGE (upsert) strategies depending on the data.
- **Reliable processing:** Safe reruns prevent duplicates, empty loads are skipped, and older records cannot overwrite newer lab results.
- **Monitoring and alerting:** Audit logs track pipeline status and record counts. Email alerts report each run.
- **Secure access:** Azure Key Vault stores database credentials and Databricks tokens.
- **Data quality:** Separate test schemas allow testing without affecting production data.
- **Version control:** Feature branches, pull requests and ADF Git integration support controlled development and deployment.

## Load strategy

| Source | Table | Load type | Only new data copied? |
| --- | --- | --- | --- |
| Neon PostgreSQL | hospitals | FULL | No, full copy each run |
| Neon PostgreSQL | doctors | FULL | No, full copy each run |
| Neon PostgreSQL | patients | MERGE on `patient_id` | Yes, rows changed since last run (`updated_at`) |
| ADLS CSV | insurance_providers | FULL | No, full copy each run |
| ADLS CSV | lab_results | MERGE on `lab_result_id` | Yes, files changed since last run |

The source data was generated with SQL scripts (starting data of 50 hospitals, ~500 doctors, ~5,000 patients), followed by incremental change scripts with new and updated records to test the incremental and upsert logic.

## How the pipeline runs (orchestration in ADF)

| Step | ADF pipeline | What happens |
| --- | --- | --- |
| 1 | `TGR_healthcare` | Daily schedule trigger (05:00) starts the master pipeline |
| 2 | `PL_master` | Runs the two source loads in parallel, then gold, then the e-mail |
| 3 | `PL_source_to_silver_main` | Reads the list of active tables from the control table and loops over them in parallel |
| 4 | `PL_source_to_silver_inner` | Per table: copy new data to landing, run `landing_to_bronze`, run `bronze_to_silver`, update watermark and audit log |
| 5 | `PL_silver_to_gold` | Runs one gold notebook per report |
| 6 | `PL_send_email` | Reads the audit log for the run and sends a Success/Failed e-mail via Logic App |

Three control tables in Azure SQL drive the pipeline. They are created and filled with their starting settings using SQL scripts:

| Control table | What it holds |
| --- | --- |
| `table_config` | One row per object: 5 source tables (stage `SOURCE_TO_SILVER`) and 3 gold reports (stage `SILVER_TO_GOLD`), with load type, watermark column, merge keys and an on/off switch (`is_active`) |
| `watermark` | Last loaded value per incremental table (patients, lab_results); starts at 1900-01-01 so the first run loads the full history |
| `audit_log` | One row per table, per stage, per run: status (in progress / success / failed), rows written, start and end time |

Adding a new table or report means adding a row to `table_config`; no pipeline change is needed.

## Databricks notebooks

| Notebook | Layer | Purpose |
| --- | --- | --- |
| `landing_to_bronze` | Bronze | Reads the raw files (CSV or Parquet) for one batch and appends them to bronze with a batch time stamp |
| `Silver/bronze_to_silver_upsert` | Silver | Writes the batch to silver: full replace for master data (hospitals, doctors, insurance providers), upsert for patients and lab results. Safe to rerun |
| `Bronze/Prepare calendar table` | Silver | Date dimension (1940-2030) with built-in data quality tests |
| `Gold/NB_patient_360` | Gold | One row per patient: hospital, insurance and lab test summary |
| `Gold/NB_hospital_daily_summary` | Gold | One row per hospital per day: test volumes, abnormal/critical rates, day-over-day trend |
| `Gold/NB_lab_test_trends` | Gold | One row per test type per month: abnormal/critical rates, month-over-month trend |

Gold tables were designed from a source-to-target mapping spreadsheet (`Gold/sttm_silver_to_gold.xlsx`).

