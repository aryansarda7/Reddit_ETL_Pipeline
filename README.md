# Reddit_ETL_Pipeline

# Reddit ETL Data Engineering Project

## 📌 Project Overview
This project is a complete end-to-end data engineering pipeline that extracts data from Reddit, transforms it, performs sentiment analysis, stores it in AWS S3 using the Bronze–Silver–Gold layer architecture, and finally loads the analytical results into PostgreSQL for querying. The entire process is orchestrated using Apache Airflow and deployed on an EC2 instance.

---

## 🧱 Base Model (Initial Version)
In the initial version of the project, the workflow was as follows:

1. **Extract**:
   - Used the `praw` library to extract 100+ daily posts from the `dataengineering` subreddit.

2. **Transform**:
   - Used `pandas` to structure the extracted data.

3. **Load**:
   - Saved the output as a `.csv` file to an AWS S3 bucket using `s3fs`.

4. **Automation**:
   - Deployed a basic Airflow DAG on an EC2 instance to run the ETL script (`reddit_etl.py`) on-demand.

5. **Tools Used**:
   - Python, PRAW, Pandas, boto3, S3, Apache Airflow, EC2

---

## ✅ New Improvements You Made

The base project was refactored and scaled into a production-ready data pipeline with the following enhancements:

### 🔹 Modular Folder Structure
- Project broken down into subfolders:
  - `extract/`
  - `bronze/`
  - `silver/`
  - `gold/`
  - `load/`
  - `utils/`
  - `dags/`

### 🔹 Multi-Layer ETL Architecture
Implemented the **Bronze–Silver–Gold** data lake architecture:

- **Bronze Layer**: Raw Reddit data saved as partitioned files in S3 (`year/month/day`).
- **Silver Layer**: Cleaned and enriched data with new features like `post_length`, `sentiment_score`, and `sentiment_label`.
- **Gold Layer**: Final aggregated analytics tables:
  - `top_positive_posts`
  - `subreddit_sentiment_avg`
  - `sentiment_distribution`

### 🔹 Sentiment Analysis Integration
- Used **NLTK VADER** to calculate sentiment for each Reddit post.
- Added sentiment labels (positive/neutral/negative) and scores.

### 🔹 Optimized Storage Format
- Switched from CSV to **Parquet format** for faster reads and reduced storage.
- Saved partitioned files to **Amazon S3** using `pandas.to_parquet()`.

### 🔹 PostgreSQL Data Warehouse Integration
- Loaded Gold-layer analytics tables into **PostgreSQL** using `psycopg2` and the high-performance `COPY` command.
- Used **pgAdmin** to query and verify the data.

### 🔹 Fully Orchestrated with Apache Airflow
- DAG split into 5 tasks:
  - `extract_reddit`
  - `save_bronze_layer`
  - `transform_silver_layer`
  - `transform_gold_layer`
  - `load_to_postgres`
- DAG deployed on EC2 and scheduled/run using the Airflow UI.

---

## 🚀 Tools & Technologies Used
- **Languages**: Python
- **Libraries**: PRAW, Pandas, NLTK (VADER), s3fs, boto3, pyarrow, psycopg2
- **Cloud**: AWS S3, EC2
- **Workflow Orchestration**: Apache Airflow
- **Database**: PostgreSQL
- **UI Tools**: pgAdmin

---

## ✅ Status
All tasks successfully run via Airflow on EC2. Gold layer data is accessible and queryable via PostgreSQL.

---

## 📌 Summary
This project demonstrates the end-to-end process of building a scalable, production-grade ETL pipeline for Reddit data — from raw extraction to automated transformation and loading into a queryable warehouse.
