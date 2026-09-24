# 🚨 Real-Time Banking Fraud Detection Platform

A real-time data engineering platform for detecting suspicious and fraudulent banking transactions using **Python, Apache Kafka, PySpark, PostgreSQL, Apache Airflow, Streamlit, Docker, and Git**.

The platform continuously generates banking transactions, streams them through Kafka, processes them using PySpark, applies fraud detection rules, stores the results in PostgreSQL, orchestrates pipeline validation with Airflow, and visualizes fraud analytics through a Streamlit dashboard.

---

## 🚀 Project Overview

The **Real-Time Banking Fraud Detection Platform** is an end-to-end data engineering project designed to simulate a real-time banking transaction monitoring system.

The system processes banking transactions through a streaming architecture:

```text
Transaction Generator
        ↓
      Kafka
        ↓
PySpark Streaming
        ↓
 Fraud Detection Rules
        ↓
   PostgreSQL
        ↓
 Streamlit Dashboard
```
Apache Airflow is used to orchestrate and validate the banking fraud pipeline.

```text

                    ┌──────────────────────┐
                    │ Transaction Generator│
                    │       Python         │
                    └──────────┬───────────┘
                               │
                               ▼
                    ┌──────────────────────┐
                    │        Kafka         │
                    │  bank-transactions   │
                    └──────────┬───────────┘
                               │
                               ▼
                    ┌──────────────────────┐
                    │    PySpark Stream    │
                    │ Fraud Detection Rules│
                    └──────────┬───────────┘
                               │
                               ▼
                    ┌──────────────────────┐
                    │     PostgreSQL       │
                    │ Transactions/Alerts  │
                    └──────────┬───────────┘
                               │
                    ┌──────────┴──────────┐
                    ▼                     ▼
          ┌─────────────────┐    ┌─────────────────┐
          │ Streamlit       │    │ Apache Airflow  │
          │ Dashboard       │    │ Orchestration   │
          └─────────────────┘    └─────────────────┘
```
## 🎯 Project Objectives

The main objectives of this project are:
Build a real-time banking transaction data pipeline.
Generate realistic banking transactions using Python.
Stream transactions through Apache Kafka.
Process streaming data using PySpark.
Implement rule-based fraud detection.
Assign risk scores to transactions.
Identify NORMAL, SUSPICIOUS, and FRAUD transactions.
Store processed transactions in PostgreSQL.
Store suspicious and fraudulent transactions as alerts.
Build a real-time analytics dashboard using Streamlit.
Orchestrate pipeline validation using Apache Airflow.
Containerize infrastructure components using Docker.
Create a GitHub-ready data engineering portfolio project.
🏗️ System Architecture
                        REAL-TIME BANKING FRAUD PLATFORM

 ┌─────────────────────┐
 │ Transaction         │
 │ Generator           │
 │ Python              │
 └──────────┬──────────┘
            │
            ▼
 ┌─────────────────────┐
 │ Apache Kafka        │
 │ bank-transactions   │
 └──────────┬──────────┘
            │
            ▼
 ┌─────────────────────┐
 │ PySpark Streaming   │
 │                     │
 │ • Read Kafka        │
 │ • Parse JSON        │
 │ • Apply Rules       │
 │ • Calculate Risk    │
 └──────────┬──────────┘
            │
            ▼
 ┌─────────────────────┐
 │ PostgreSQL          │
 │                     │
 │ • transactions      │
 │ • fraud_alerts      │
 │ • customers         │
 │ • daily_summary     │
 └──────────┬──────────┘
            │
       ┌────┴─────┐
       ▼          ▼
 ┌───────────┐ ┌──────────────┐
 │ Streamlit │ │ Apache       │
 │ Dashboard │ │ Airflow      │
 └───────────┘ └──────────────┘
📁 Project Folder Structure
real-time-banking-fraud-platform/
│
├── airflow/
│   ├── dags/
│   │   └── banking_fraud_pipeline.py
│   ├── logs/
│   ├── plugins/
│   └── docker-compose.yml
│
├── dashboard/
│   └── app.py
│
├── data/
│   ├── processed/
│   │   └── .gitkeep
│   ├── raw/
│   │   └── .gitkeep
│   └── sample/
│       └── .gitkeep
│
├── database/
│   └── schema.sql
│
├── docker/
│
├── docs/
│
├── iceberg/
│
├── kafka/
│   └── docker-compose.yml
│
├── spark/
│   ├── checkpoints/
│   ├── jars/
│   │   └── postgresql-42.7.8.jar
│   ├── fraud_rules.py
│   ├── requirements.txt
│   ├── streaming.py
│   └── test_fraud_rules.py
│
├── tests/
│
├── transaction-generator/
│   ├── generator.py
│   ├── producer.py
│   └── requirements.txt
│
├── .gitignore
└── README.md

Temporary files such as .venv, logs, Spark checkpoints, Python cache files, and environment files are excluded using .gitignore.

⚙️ Step 1 — Project Setup

The project was created as a modular real-time data engineering platform.

The main components were separated into individual directories:

airflow/
dashboard/
data/
database/
kafka/
spark/
transaction-generator/

Git was also initialized for version control.

git init

A .gitignore file was created to prevent temporary files and secrets from being committed.

🐍 Step 2 — Python Environment

A Python virtual environment was created to isolate project dependencies.

python -m venv .venv

Activate the environment on Windows:

.venv\Scripts\Activate.ps1

The project uses Python for:

Transaction generation
Kafka producer
Fraud detection rules
Spark processing
Dashboard development
Testing

Python dependencies are documented in:

spark/requirements.txt
transaction-generator/requirements.txt
📨 Step 3 — Kafka Setup

Apache Kafka was used as the real-time messaging and streaming layer.

Kafka runs using Docker.

Kafka configuration:

kafka/docker-compose.yml

The primary Kafka topic is:

bank-transactions

The architecture is:

Transaction Generator
        ↓
      Kafka
        ↓
bank-transactions
        ↓
   PySpark Streaming

Kafka allows transactions to be produced and consumed continuously.

💳 Step 4 — Transaction Generator

The transaction generator creates simulated banking transactions.

Source files:

transaction-generator/generator.py
transaction-generator/producer.py

Generated transaction information includes:

Transaction ID
Customer ID
Transaction amount
Location

Example:

TXN000001
Customer: CUST003
Amount: ₹73542.18
Location: Mumbai

The generator continuously produces transactions to simulate a real banking environment.

📤 Step 5 — Kafka Producer

The Kafka producer sends generated transactions to the Kafka topic:

bank-transactions

The producer acts as the bridge between transaction generation and real-time streaming.

Python Generator
       ↓
Kafka Producer
       ↓
Kafka Topic
       ↓
bank-transactions

Source file:

transaction-generator/producer.py

🗄️ Step 6 — PostgreSQL Database

PostgreSQL is used as the persistent storage layer.

Database schema:

database/schema.sql

The project contains tables for:

Customers

Stores customer information.

customer_id
customer_name
account_number
created_at
Transactions

Stores processed banking transactions.

transaction_id
customer_id
amount
location
transaction_time
risk_score
fraud_status
fraud_reason
Fraud Alerts

Stores suspicious and fraudulent transactions.

alert_id
transaction_id
customer_id
risk_score
fraud_status
fraud_reason
alert_time
Daily Transaction Summary

Stores daily transaction-level analytics.

summary_date
total_transactions
total_amount
fraud_transactions
suspicious_transactions
🔍 Step 7 — PySpark Fraud Detection

PySpark is used to process transactions in real time.

Fraud detection rules are implemented in:

spark/fraud_rules.py

The current rule-based detection system evaluates transaction amounts.

Risk Rules
Transaction Amount	Risk Score	Status
≤ ₹20,000	10	NORMAL
> ₹20,000	50	SUSPICIOUS
> ₹50,000	90	FRAUD
Fraud Reasons
NORMAL
    ↓
Normal transaction

SUSPICIOUS
    ↓
Medium-value transaction

FRAUD
    ↓
High-value transaction

Example:

Amount: ₹10,000
Risk Score: 10
Status: NORMAL
Amount: ₹30,000
Risk Score: 50
Status: SUSPICIOUS
Amount: ₹75,000
Risk Score: 90
Status: FRAUD
⚡ Step 8 — Spark Streaming → PostgreSQL

PySpark Structured Streaming consumes transactions from Kafka.

Source file:

spark/streaming.py

The streaming pipeline performs the following operations:

Kafka
  ↓
Read Streaming Data
  ↓
Parse Transaction JSON
  ↓
Apply Fraud Rules
  ↓
Calculate Risk Score
  ↓
Determine Fraud Status
  ↓
Write Results to PostgreSQL

The PostgreSQL JDBC driver is located at:

spark/jars/postgresql-42.7.8.jar

The streaming pipeline also uses a checkpoint directory to maintain streaming state.

Duplicate transaction handling was implemented using PostgreSQL conflict handling so that previously processed transactions do not stop the streaming pipeline.

🚨 Step 9 — Fraud Alerts & Analytics

Transactions classified as:

FRAUD
SUSPICIOUS

are recorded in the fraud_alerts table.

The pipeline therefore provides two levels of processing:

All Transactions
       │
       ├── NORMAL
       │
       ├── SUSPICIOUS
       │
       └── FRAUD

Fraud and suspicious transactions can then be analyzed through PostgreSQL and the Streamlit dashboard.

🌬️ Step 10 — Airflow Orchestration

Apache Airflow was added to orchestrate and validate the banking fraud pipeline.

Airflow configuration:

airflow/docker-compose.yml

DAG:

airflow/dags/banking_fraud_pipeline.py

DAG ID:

banking_fraud_pipeline

The DAG contains four tasks:

validate_pipeline
        ↓
check_postgres
        ↓
generate_daily_summary
        ↓
verify_fraud_alerts
Airflow Tasks
Task	Purpose
validate_pipeline	Validates the banking fraud pipeline
check_postgres	Checks PostgreSQL banking tables
generate_daily_summary	Generates the daily banking summary
verify_fraud_alerts	Verifies fraud and suspicious alerts

The DAG was manually triggered and successfully completed:

banking_fraud_pipeline → SUCCESS

This confirms that the Airflow orchestration layer is functioning correctly.

📊 Step 11 — Streamlit Dashboard

A Streamlit dashboard was created for monitoring banking transactions and fraud analytics.

Dashboard source:

dashboard/app.py

The dashboard provides information such as:

Total Transactions
Total Transaction Value
Fraud Transactions
Suspicious Transactions
Fraud Percentage
Transaction analytics
Fraud-related information

The dashboard runs locally at:

http://localhost:8501

Example project results during testing included:

Total Transactions: 2,411
Total Transaction Value: ₹121,155,202.25
Fraud Transactions: 1,243
Suspicious Transactions: 701
Fraud Percentage: 51.56%

These values represent the data generated during the project test run and will change as new transactions are processed.

🧪 Step 12 — Testing & Validation

Fraud detection rules were tested using:

spark/test_fraud_rules.py

Test cases included:

TXN001
Amount: ₹10,000
Expected: NORMAL
Risk Score: 10
TXN002
Amount: ₹30,000
Expected: SUSPICIOUS
Risk Score: 50
TXN003
Amount: ₹75,000
Expected: FRAUD
Risk Score: 90

All three fraud-rule scenarios were successfully validated.

End-to-End Validation

The complete data flow was also tested:

Transaction Generator
        ↓
Kafka
        ↓
PySpark
        ↓
Fraud Detection
        ↓
PostgreSQL
        ↓
Streamlit

Airflow was additionally tested and the DAG completed successfully.

🛠️ Technologies Used
Technology	Purpose
Python	Transaction generation and application logic
Apache Kafka	Real-time event streaming
PySpark	Stream processing and fraud detection
PostgreSQL	Transaction and fraud data storage
Apache Airflow	Pipeline orchestration
Streamlit	Data visualization dashboard
Docker	Infrastructure containerization
Git	Version control
GitHub	Source code hosting
PostgreSQL JDBC	Spark → PostgreSQL connectivity
🔧 Installation
1. Clone the Repository
git clone https://github.com/<YOUR_GITHUB_USERNAME>/real-time-banking-fraud-platform.git

Move into the project:

cd real-time-banking-fraud-platform
2. Create Python Virtual Environment
python -m venv .venv

Activate:

.venv\Scripts\Activate.ps1
3. Install Spark Dependencies
pip install -r spark/requirements.txt
4. Install Transaction Generator Dependencies
pip install -r transaction-generator/requirements.txt
5. Configure PostgreSQL Password

The project reads the PostgreSQL password through an environment variable.

Windows PowerShell:

$env:POSTGRES_PASSWORD="YOUR_POSTGRES_PASSWORD"

Do not hard-code passwords in Python files.

▶️ How to Run the Project

The project can be started component by component.

1. Start Kafka

From the project root:

docker compose -f .\kafka\docker-compose.yml up -d

Verify Docker containers:

docker ps
2. Start Transaction Generator

Run the transaction generator:

python .\transaction-generator\generator.py
3. Start Kafka Producer

Run the producer:

python .\transaction-generator\producer.py

Transactions should begin flowing into the Kafka topic.

4. Start PySpark Streaming

Set the required Hadoop environment variables on Windows if required:

$env:HADOOP_HOME="C:\hadoop"
$env:PATH="$env:HADOOP_HOME\bin;$env:PATH"

Then start the Spark streaming application:

python .\spark\streaming.py
5. Start Streamlit Dashboard
streamlit run .\dashboard\app.py

Open:

http://localhost:8501
6. Start Airflow

Airflow is configured under:

airflow/docker-compose.yml

Initialize the Airflow database:

docker compose -f .\airflow\docker-compose.yml up airflow-init

Start the Airflow services:

docker compose -f .\airflow\docker-compose.yml up -d airflow-api-server airflow-scheduler airflow-dag-processor

Check the services:

docker compose -f .\airflow\docker-compose.yml ps

Open Airflow:

http://localhost:8080
📈 Project Results

The completed project successfully demonstrates:

Real-time transaction generation.
Kafka-based transaction streaming.
PySpark Structured Streaming.
Rule-based fraud detection.
Risk score calculation.
Fraud and suspicious transaction classification.
PostgreSQL persistence.
Fraud alert storage.
Streamlit analytics.
Airflow orchestration.
Docker-based infrastructure.
End-to-end pipeline validation.
Example Fraud Detection
Transaction
     │
     ▼
Amount = ₹75,000
     │
     ▼
Risk Score = 90
     │
     ▼
Status = FRAUD
     │
     ▼
Fraud Alert
     │
     ▼
PostgreSQL
     │
     ▼
Streamlit Dashboard
📸 Screenshots

Add your actual project screenshots in this section.

Recommended screenshots:

1. Kafka
![Kafka Topic](docs/images/kafka-topic.png)
2. PySpark Streaming
![PySpark Streaming](docs/images/pyspark-streaming.png)
3. PostgreSQL Fraud Alerts
![PostgreSQL Fraud Alerts](docs/images/postgresql-fraud-alerts.png)
4. Airflow DAG
![Airflow DAG](docs/images/airflow-dag.png)
5. Streamlit Dashboard
![Streamlit Dashboard](docs/images/streamlit-dashboard.png)
6. Complete Architecture
![System Architecture](docs/images/system-architecture.png)
🔄 End-to-End Data Flow

The complete pipeline works as follows:

                  ┌──────────────────────┐
                  │ Transaction Generator│
                  │       Python         │
                  └──────────┬───────────┘
                             │
                             ▼
                  ┌──────────────────────┐
                  │        Kafka         │
                  │ bank-transactions    │
                  └──────────┬───────────┘
                             │
                             ▼
                  ┌──────────────────────┐
                  │       PySpark        │
                  │ Structured Streaming │
                  └──────────┬───────────┘
                             │
                             ▼
                  ┌──────────────────────┐
                  │  Fraud Detection     │
                  │  Risk Calculation    │
                  └──────────┬───────────┘
                             │
                             ▼
                  ┌──────────────────────┐
                  │     PostgreSQL       │
                  │                      │
                  │ Transactions         │
                  │ Fraud Alerts         │
                  │ Daily Summary        │
                  └──────────┬───────────┘
                             │
                    ┌────────┴────────┐
                    ▼                 ▼
           ┌────────────────┐ ┌────────────────┐
           │   Streamlit    │ │    Airflow     │
           │   Dashboard    │ │ Orchestration  │
           └────────────────┘ └────────────────┘
🐳 Docker Configuration

Docker is used to simplify infrastructure setup.

The project contains Docker Compose configurations for:

Kafka
kafka/docker-compose.yml
Airflow
airflow/docker-compose.yml

Docker provides isolated services for the infrastructure components.

Check running containers:

docker ps

Check Airflow services:

docker compose -f .\airflow\docker-compose.yml ps
🔐 Environment Variables

Sensitive credentials should never be committed to GitHub.

The project uses environment variables such as:

POSTGRES_PASSWORD

Example:

$env:POSTGRES_PASSWORD="YOUR_PASSWORD"

The Python application accesses the value using:

os.getenv("POSTGRES_PASSWORD")

The .env file is excluded from Git using .gitignore.

Important Security Rule

Never commit:

.env
passwords
API keys
tokens
database credentials
private keys
🧹 Git & GitHub

Git was initialized for version control:

git init

Project files were staged:

git add .

The initial project commit was created:

git commit -m "Initial commit - real-time banking fraud platform"

The repository can then be connected to GitHub:

git remote add origin https://github.com/<YOUR_GITHUB_USERNAME>/real-time-banking-fraud-platform.git

Rename the default branch:

git branch -M main

Push the project:

git push -u origin main
📋 Project Development Process

The complete development process was:

01. Project Setup
        ↓
02. Python Environment
        ↓
03. Kafka Setup
        ↓
04. Transaction Generator
        ↓
05. Kafka Producer
        ↓
06. PostgreSQL Database
        ↓
07. PySpark Fraud Detection
        ↓
08. Spark Streaming → PostgreSQL
        ↓
09. Fraud Alerts & Analytics
        ↓
10. Airflow Orchestration
        ↓
11. Streamlit Dashboard
        ↓
12. Testing & Validation
        ↓
13. Git Initialization
        ↓
14. Security Check
        ↓
15. GitHub Documentation
        ↓
16. GitHub Deployment
🚀 Future Improvements

The current platform uses rule-based fraud detection. Future versions could include:

Machine learning-based fraud detection.
Real-time ML model inference.
Advanced customer behavior analysis.
Transaction velocity detection.
Geographic anomaly detection.
Device and IP-based fraud detection.
Advanced Kafka partitioning.
Kafka Schema Registry.
Apache Iceberg integration.
Cloud deployment.
AWS / Azure / GCP integration.
Automated email or notification alerts.
More advanced Streamlit visualizations.
Data quality monitoring.
More comprehensive automated tests.
CI/CD using GitHub Actions.
Production-grade Airflow deployment.
Monitoring and observability.
🎓 Skills Demonstrated

This project demonstrates practical experience with:

Data Engineering
ETL / ELT concepts
Real-time data pipelines
Streaming data processing
Data storage
Data validation
Pipeline orchestration
Big Data
Apache Kafka
Apache Spark
PySpark Structured Streaming
Databases
PostgreSQL
SQL
JDBC
Relational data modeling
Orchestration
Apache Airflow
DAG development
Task dependencies
Pipeline validation
Visualization
Streamlit
Real-time analytics
Fraud monitoring
DevOps
Docker
Docker Compose
Environment variables
Git
GitHub
📌 Project Status
Project Status: ✅ Completed

Real-Time Streaming       ✅
Kafka                     ✅
PySpark                   ✅
Fraud Detection           ✅
PostgreSQL                ✅
Fraud Alerts              ✅
Streamlit Dashboard       ✅
Airflow Orchestration     ✅
Docker                    ✅
Testing                   ✅
Git                       ✅
GitHub Documentation      🟡 In Progress
👨‍💻 Author
Venkatesh

Data Engineer

I am interested in building scalable data pipelines, real-time streaming systems, data processing platforms, and analytics solutions.

Technical Interests
Data Engineering
Python
SQL
Apache Kafka
PySpark
PostgreSQL
Apache Airflow
Docker
Streamlit
Cloud Technologies

📍 Hyderabad, India
