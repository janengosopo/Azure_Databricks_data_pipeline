# Healthcare Data Pipeline - Project Documentation

Status: development (`env = dev`). Last reviewed: 3 November 2026.

This document explains, end to end, what the healthcare data pipeline does and how it is built. It has two parts:

* **Part 1 - Project in plain words**: for readers without a technical background.
* **Part 2 - Technical details**: for engineers and technical reviewers.

Items we could not confirm from the code are marked **[NOT DOCUMENTED]**. Known problems are marked **[OPEN ISSUE]**.

---

## Part 1 - The project in plain words

### What problem does it solve?

**CareSync Health Network** is a healthcare group with data spread over different places: a database (Neon PostgreSQL) with hospitals, doctors and patients, and files (CSV in Azure storage) with insurance providers and lab test results. Looking at this data together is hard because it is stored in different systems and formats.

This project brings all the data into one place every day, cleans it, and turns it into ready-to-use reports, for example:

* a full profile of every patient (age, hospital, insurance, how many lab tests, how many were abnormal or critical),
* a daily summary per hospital (how many tests, patients and doctors, and how this changes day to day),
* a monthly trend per lab test type (is the rate of abnormal results going up or down?).

### How does it work? (the "medallion" idea)

The data moves through layers, like water through filters. Each layer is cleaner than the one before:

| Layer | Simple meaning | What happens here |
| --- | --- | --- |
| Source | Where the data is born | Neon PostgreSQL database (`neondb`, schema `public`): tables `hospitals`, `doctors`, `patients`; and CSV files in Azure storage: `insurance_providers`, `lab_results` |
| Landing | The "mailbox" | Raw copies of the data are dropped here, one folder per run |
| Bronze | The "archive" | Every delivery is kept as it arrived, with a time stamp. Nothing is deleted |
| Silver | The "clean library" | One up-to-date, clean version of each record |
| Gold | The "reports shelf" | Business-ready tables that answer real questions |

### Who does what?

* **Azure Data Factory (ADF)** is the "conductor". It starts the pipeline every morning, copies data from the sources, and tells Databricks what to do next.
* **Databricks** is the "workshop". It reads the raw data, cleans it and builds the reports.
* **Azure SQL Database** is the "control room". It holds the list of tables to load, how each one should be loaded, and a logbook of every run.
* **Azure Key Vault** is the "safe". It stores passwords and access tokens so they are not written in the code.
* **Azure Logic App** is the "messenger". It sends an e-mail after each run saying whether everything worked.
* **GitHub** holds all code, with version history and reviewed changes (pull requests).

### A day in the life of the pipeline

1. Every day at 05:00 (Finland/Eastern European time) the pipeline starts automatically.
2. It checks the control room for the list of tables to load.
3. For each table it copies only the new or changed data from the source into the landing area.
4. Databricks stores that delivery in the archive (bronze).
5. Databricks updates the clean library (silver) so it holds the newest version of each record.
6. When all tables are done, Databricks rebuilds the three reports (gold).
7. Every step is written into the logbook (success or failure, and how many records).
8. An e-mail is sent with the overall result (Success or Failed) and a status table.

### What has been achieved

* Data from 2 different source types (database and files) is loaded automatically every day.
* 5 business tables are loaded: hospitals, doctors, patients, insurance providers, lab results.
* Only new data is copied each day (incremental loading), which saves time and cost.
* The pipeline is **configuration-driven**: adding a new table means adding a row to a settings table, not writing new code.
* Reruns are made safe so data is not counted twice (work in progress for lab results, see Part 2).
* 3 gold reports are built from a written mapping specification.
* Monitoring: a run logbook and an automatic e-mail report.
* Code is version controlled in GitHub, with changes reviewed through pull requests.

### Current size of the data (3 Nov 2026)

| Report / table | Rows |
| --- | --- |
| Patients (clean) | 5,003 |
| Doctors (clean) | 502 |
| Hospitals (clean) | 51 |
| Insurance providers (clean) | 25 |
| Unique lab tests (clean) | 8,228 |
| Gold: patient profile (`patient_360`) | 5,003 |
| Gold: hospital daily summary | 6,691 |
| Gold: lab test monthly trends | 70 |

---

## Part 2 - Technical details

### 2.1 Technology stack

| Area | Technology |
| --- | --- |
| Orchestration | Azure Data Factory |
| Processing | Azure Databricks (PySpark, Spark SQL), Delta Lake, Unity Catalog |
| Storage | Azure Data Lake Storage Gen2 |
| Sources | Neon PostgreSQL (cloud database), CSV files in ADLS |
| Metadata and logging | Azure SQL Database |
| Secrets | Azure Key Vault |
| Alerts | Azure Logic App (e-mail) |
| Version control | GitHub (one repo for Databricks, one for ADF) |

### 2.2 Architecture overview

```
   Neon PostgreSQL            CSV files (ADLS)
  (hospitals, doctors,      (insurance providers,
      patients)                 lab results)
          \                        /
           \                      /
            v                    v
     Azure Data Factory  -- copy new data -->  LANDING (raw files)
     (schedule, control,                          |
      logging, alerts)                            v  Databricks
            |                                  BRONZE  (history of every load)
            |                                     |
            |                                     v  Databricks
            |                                  SILVER  (clean, latest records)
            |                                     |
            |                                     v  Databricks
            |                                  GOLD    (3 business reports)
            v
   Azure SQL control tables  --->  Logic App  --->  status e-mail
```

All Databricks tables live in Unity Catalog under one catalog (`adb_healthcare`) with one schema per layer.

### 2.3 How the pipeline runs (orchestration in ADF)

| Step | ADF pipeline | What happens |
| --- | --- | --- |
| 1 | `TGR_healthcare` | Daily schedule trigger (05:00) starts the master pipeline |
| 2 | `PL_master` | Runs the two source loads in parallel, then gold, then the e-mail |
| 3 | `PL_source_to_silver_main` | Reads the list of active tables from the control table and loops over them in parallel |
| 4 | `PL_source_to_silver_inner` | Per table: copy new data to landing, run `landing_to_bronze`, run `bronze_to_silver`, update watermark and audit log |
| 5 | `PL_silver_to_gold` | Runs one gold notebook per report |
| 6 | `PL_send_email` | Reads the audit log for the run and sends a Success/Failed e-mail via Logic App |

Three control tables in Azure SQL (schema `ctrl`) drive the pipeline. They are created and seeded with SQL scripts:

| Control table | What it holds |
| --- | --- |
| `table_config` | One row per object: 5 source tables (stage `SOURCE_TO_SILVER`) and 3 gold reports (stage `SILVER_TO_GOLD`), with load type, watermark column, merge keys and an on/off switch (`is_active`) |
| `watermark` | Last loaded value per incremental table (patients, lab_results); starts at 1900-01-01 so the first run loads the full history |
| `audit_log` | One row per table, per stage, per run: status (in progress / success / failed), rows written, start and end time |

Adding a new table or report means adding a row to `table_config`; no pipeline change is needed.

### 2.4 Databricks notebooks

| Notebook | Layer | Purpose |
| --- | --- | --- |
| `landing_to_bronze` | Bronze | Reads the raw files (CSV or Parquet) for one batch and appends them to bronze with a batch time stamp |
| `Silver/bronze_to_silver_upsert` | Silver | Writes the batch to silver using FULL, APPEND or MERGE (upsert), safe to rerun |
| `Prepare calendar table` | Silver | Date dimension (1940-2030) with built-in data quality tests |
| `Gold/NB_patient_360` | Gold | One row per patient: hospital, insurance and lab test summary |
| `Gold/NB_hospital_daily_summary` | Gold | One row per hospital per day: test volumes, abnormal/critical rates, day-over-day trend |
| `Gold/NB_lab_test_trends` | Gold | One row per test type per month: abnormal/critical rates, month-over-month trend |

Gold tables were designed from a source-to-target mapping spreadsheet (`Gold/sttm_silver_to_gold.xlsx`).

### 2.5 Source tables and load strategy

| Source | Table | Load strategy |
| --- | --- | --- |
| Neon PostgreSQL | hospitals | FULL (full reload each run) |
| Neon PostgreSQL | doctors | FULL |
| Neon PostgreSQL | patients | MERGE (upsert on `patient_id`, incremental on `updated_at`) |
| ADLS CSV | insurance_providers | FULL |
| ADLS CSV | lab_results | APPEND, incremental on `reported_at` (the upsert notebook turns this into a MERGE on `lab_result_id` to prevent duplicates) |

The source data was generated with SQL scripts (seed of 50 hospitals, ~500 doctors, ~5,000 patients), followed by incremental change scripts with new and updated records to test the incremental and upsert logic.

### 2.6 Key design decisions

* **Metadata-driven design**: one generic pipeline and generic notebooks serve all tables, driven by `ctrl.table_config`.
* **Incremental loading** with watermarks (Postgres `WHERE` filter, file modified-date filter).
* **Three load strategies**: FULL, APPEND and MERGE (upsert).
* **Idempotency (safe reruns)**: empty-batch guards, `REPLACE WHERE` for append, ordered MERGE for lab results.
* **Audit logging and alerting**: per-table status and row counts in `ctrl.audit_log`, and an e-mail per run.
* **Secrets in Key Vault**, not in code (for database password and Databricks token).
* **Testing in isolation**: `bronze_test` and `silver_test` schemas so tests never change real data; calendar data quality tests.
* **Git workflow**: feature branches and pull requests (PR #1 `feature_1`, PR #2 `bronze_to_silver_upsert_only`), ADF Git integration with `adf_publish` branch.

### 2.7 Project timeline

| Date (2026) | Milestone |
| --- | --- |
| 22 Oct | Databricks repo created; first ingestion of Neon Postgres data to landing |
| 26-27 Oct | Fixes: silver table auto-created on first APPEND/MERGE; notebooks skip safely when ADF copied no new files |
| 27 Oct | Calendar table and 3 gold notebooks built from the STTM |
| 28 Oct | PR #1 merged (`feature_1`) |
| 29 Oct | E-mail notification pipeline (`PL_send_email`) built and published |
| 30 Oct | Daily trigger `TGR_healthcare` added; upsert / rerun-safe silver notebook and dedupe notebook created, moved to `Silver/` folder; PR #2 merged into `main` |

### 2.8 Challenges and how they were solved

| Problem | Cause | Solution |
| --- | --- | --- |
| `lab_results` APPEND failed with TABLE_OR_VIEW_NOT_FOUND | Silver table did not exist yet | APPEND and MERGE now create the table on first run |
| `landing_to_bronze` failed with PATH_NOT_FOUND | Copy succeeded but wrote 0 files because no source file changed since the watermark, so the folder was never created | Notebook checks the folder and exits cleanly with 0; silver notebook also exits when the batch is empty |
| Duplicate lab results in silver | ADF Copy re-sent the same full CSV, and APPEND added it again | New `bronze_to_silver_upsert` (MERGE on `lab_result_id`) and one-time dedupe notebook |

### 2.9 Next steps

* **[OPEN ISSUE]** Point ADF to the notebooks' new `Silver/` location and to the upsert version.
* **[OPEN ISSUE]** Run the one-time clean-up of duplicate lab results in silver.
* **[OPEN ISSUE]** Fix the audit log run id in the source-to-silver pipeline and use a 24-hour batch time stamp.
* **[OPEN ISSUE]** Move the Logic App URL into Key Vault.
* Add a dashboard on top of the gold tables.
* Add CI/CD to deploy to test and production (only `dev` exists today).
* Add the source and control table SQL scripts to the repo (today they are kept outside Git).
* **[NOT DOCUMENTED]** CSV source folder layout, Logic App design and cluster setup are not stored in the repos yet.

### 2.10 Repository layout

```
healthcare_adb_repo/            (Databricks)
  landing_to_bronze
  Prepare calendar table
  Silver/
    bronze_to_silver
    bronze_to_silver_upsert
    one_time_dedupe_silver_lab_results
  Gold/
    NB_patient_360
    NB_hospital_daily_summary
    NB_lab_test_trends
    sttm_silver_to_gold.xlsx

healthcare_adf_repo/            (Azure Data Factory)
  pipeline/   PL_master, PL_source_to_silver_main, PL_source_to_silver_inner,
              PL_silver_to_gold, PL_send_email, test
  dataset/    DS_ADLS_Parquet, DS_CSV_binary, PostgreSqlTable1, AzureSqlTable1
  linkedService/  LS_PostgreSql1, LS_AzureDatabricks1, LS_AzureKeyVault1,
                  AzureSqlDatabase1, AzureDataLakeStorage1
  trigger/    TGR_healthcare
  factory/    adf-healthcare-01 (global parameters)
```
