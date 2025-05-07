# Automated-IPL-Pipeline

# 🏏 IPL Data Engineering Pipeline

This project builds a fully automated data engineering pipeline to extract, transform, and load (ETL) IPL match data from the Cricbuzz API to AWS S3 and MySQL/Snowflake, generating analytics-ready tables for visualization and insight generation.

---

## 📌 Project Overview

**Architecture Flow:**

```
Cricbuzz API → AWS S3 → MySQL (RAW → SILVER → GOLD) / Snowflake → Tableau
```

### 🔹 Components

1. **`get_ipl_matches_auto.py`**  
   - Fetches completed IPL matches using RapidAPI.
   - Downloads scorecard and commentary.
   - Uploads JSON files to an S3 bucket.
   - Updates a log (`processed_matches.json`) to prevent reprocessing.

2. **`main_pipeline.py`**  
   - Orchestrates the data pipeline.
   - Loads raw files from S3 into MySQL.
   - Applies transformations to generate SILVER and GOLD tables.

3. **`raw_processor.py`**  
   - Loads JSON from S3 into `raw_scorecard` and `raw_commentary` MySQL tables.
   - Skips already-loaded match IDs.

4. **`transform_processor.py`**  
   - Transforms RAW to SILVER (batting, bowling, summary).
   - Transforms SILVER to GOLD (top batsmen, top bowlers, team stats).
   - Computes Net Run Rate (NRR), win status, and aggregates.

5. **`transform_scorecard_to_silver.py`** *(Optional Snowflake Version)*  
   - Transforms IPL raw JSON in Snowflake's `RAW` schema into SILVER layer tables.
   - Uses SQL and JSON parsing within Snowflake.

---

## 🛠️ Technologies Used

- **Python 3.x**
- **AWS S3** (`boto3`)
- **MySQL** / **Snowflake**
- **RapidAPI (Cricbuzz API)**
- **Tableau** (for visualization)
- **FuzzyWuzzy** (for name matching)
- **SQL** (MySQL, Snowflake SQL)

---

## ⚙️ Setup Instructions

1. **Install Dependencies**
   ```bash
   pip install boto3 mysql-connector-python fuzzywuzzy snowflake-connector-python
   ```

2. **Configure `config.py`**  
   Create a file with AWS, MySQL, and other credentials:
   ```python
   MYSQL_CONFIG = {
       'host': 'localhost',
       'user': 'root',
       'password': 'your_password',
       'database': 'ipl_project'
   }

   AWS_CONFIG = {
       'aws_access_key_id': 'YOUR_KEY',
       'aws_secret_access_key': 'YOUR_SECRET',
       'region_name': 'us-east-1'
   }

   BUCKET_NAME = 'ipl-project-arn'
   ```

3. **Run the Pipeline**
   ```bash
   python get_ipl_matches_auto.py      # Step 1: Download and upload match data
   python main_pipeline.py             # Step 2: Load into DB and transform
   ```

4. **[Optional] Run Snowflake Transformation**
   ```bash
   python transform_scorecard_to_silver.py
   ```

---

## 📊 Outputs

- **Raw Layer**: `raw_scorecard`, `raw_commentary`
- **Silver Layer**: `silver_match_summary`, `silver_batting`, `silver_bowling`
- **Gold Layer**:
  - `gold_top_batsmen`: Runs, strike rates, 4s/6s
  - `gold_top_bowlers`: Wickets, economy, overs
  - `gold_team_stats`: Wins, points, NRR

---

## 📈 Dashboard & Analysis

Use **Tableau** or **Power BI** to visualize:
- Team Standings with NRR
- Top 10 Batsmen and Bowlers
- Match-level summaries and trends

---

## 📬 Contact

Aryan Sarda  
Email: *your_email@example.com*  
LinkedIn: *[LinkedIn Profile]*

---

## 🏁 Status

✅ Fully Functional — Live ETL pipeline with modular S3 + DB support  
🛠️ Add enhancements like player comparison, live score feeds, or web dashboard
