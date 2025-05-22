# 🏏 Automated IPL Data & Analytics Platform

A fully automated end-to-end Data Engineering + Data Analysis project that fetches IPL match data via API, processes and transforms it through multiple ETL layers, and delivers real-time visual dashboards with advanced cricket KPIs. Built using Python, Airflow, AWS, MySQL, and Apache Superset.

## 📌 Project Highlights

- ⛓️ **Fully Automated ETL Pipeline:** Scheduled daily with Apache Airflow to fetch match data from the Cricbuzz API and store JSON files (scorecards & commentary) in AWS S3.
- 🛢️ **Layered Data Architecture:** Implements a 3-tier MySQL data warehouse (RAW → SILVER → GOLD) to clean, normalize, and structure semi-structured cricket data.
- 📊 **Advanced Cricket KPIs:** Calculates 15+ metrics like Net Run Rate, Head-to-Head performance, Boundary Dominance, Catch Efficiency, and Powerplay stats.
- 📈 **Real-Time Dashboards:** Uses Apache Superset to create interactive dashboards auto-refreshed via Superset API with dynamic GOLD-layer SQL views.
- 📐 **High Accuracy:** All analytics match official IPL data with <1% variance through rigorous logic and validation.

## 🧱 Architecture Overview

[Cricbuzz API] -> [Airflow DAG] — fetches match data -> [AWS S3] — stores JSON files -> [RAW Layer] — raw_commentary, raw_scorecard tables -> [SILVER Layer] — parsed batting, bowling, and match summary data -> [GOLD Layer] — advanced KPIs and team/player stats -> [Superset] — visual dashboards refreshed via API


## 🛠️ Tech Stack

- **Languages & Libraries:** Python (boto3, pandas, re, json, fuzzywuzzy, mysql.connector)
- **Workflow Orchestration:** Apache Airflow
- **Data Storage:** AWS S3, MySQL (RDS)
- **Visualization:** Apache Superset
- **Other Tools:** Regex, Levenshtein distance for name normalization, SQL window functions

## ⚙️ Key Components

### 1. `get_ipl_matches_auto.py`
Fetches completed match scorecards and commentary using Cricbuzz API and stores them as JSON files in S3.

### 2. `ipl_pipeline_dag.py`
Defines the Airflow DAG for automated execution of:
- Data Fetching → Pipeline Execution → Table Update → Dashboard Refresh

### 3. `main_pipeline.py`
Main orchestrator that:
- Creates all tables
- Loads S3 JSONs to RAW
- Transforms RAW → SILVER → GOLD
- Calculates advanced KPIs

### 4. `raw_processor.py`
Handles loading raw JSON files from S3 into MySQL.

### 5. `transform_processor.py`
Parses, cleans, and normalizes data into structured SILVER and GOLD tables.

### 6. `custom_stats_processor.py`
Computes custom metrics such as:
- Clean bowled economy
- Powerplay scoring trends
- Catch efficiency
- Head-to-head stats
- Player performance metrics

### 7. `update_mysql_tables.py`
Creates SQL summary views (e.g., points table, orange/purple cap, bowler effectiveness) for use in Superset.

### 8. `airflow_refresh.py`
Uses Superset’s API to refresh charts and dashboards daily after pipeline completion.

## 📊 Sample Dashboards

Link: [Live Superset Dashboard](https://ec2-3-21-144-211.us-east-2.compute.amazonaws.com/superset/dashboard/b3ab823b-19cd-46a9-adde-6ee5763572d2/?permalink_key=lDrJ2XXedaV&standalone=true)

- 📌 Team Standings with Dynamic NRR
- 🔥 Orange & Purple Cap Rankings
- 🧠 Powerplay Impact Visuals
- 🎯 Player Performance Metrics
- 🤝 Head-to-Head Win Matrix

## 📈 Results

- ✅ **100% data accuracy**, 0% duplication with smart match tracking
- 🔍 **15+ advanced cricket KPIs** implemented using Python & SQL
- 📉 **<1% variance** compared to official IPL stats
- ⚡ **25+ analytical tables** generated and visualized
- ⏱️ **Daily automation** reduces manual effort and ensures fresh analytics

## 🚀 How to Run Locally

> **Pre-requisites:** Python 3.10+, MySQL, Airflow, AWS account, Superset

1. Clone the repo  
2. Set your AWS and MySQL credentials in config files  
3. Upload sample match JSONs to your S3 bucket  
4. Start Airflow and trigger the DAG: `ipl_pipeline_dag`  
5. Connect Superset to your MySQL instance and import charts/dashboards  
6. Run `airflow_refresh.py` to refresh charts via Superset API  

## 📧 Contact

**Aryan Sarda**  
Email: [aryan.sarda7@gmail.com](mailto:aryan.sarda7@gmail.com)  
LinkedIn: [linkedin.com/in/aryan-sarda-35aa551a5/](https://www.linkedin.com/in/aryan-sarda-35aa551a5/)  
GitHub: [github.com/aryansarda7](https://github.com/aryansarda7)  

---
