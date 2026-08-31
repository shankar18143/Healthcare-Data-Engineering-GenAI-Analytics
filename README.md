
# Healthcare Data Engineering & GenAI Analytics

An end-to-end healthcare data engineering and GenAI analytics project built using the **MIMIC-III dataset** and **Azure Databricks**. The project demonstrates the complete data pipeline from healthcare data ingestion and PySpark-based processing to exploratory data analysis, visualization, Delta Lake storage, and GenAI-powered natural-language analytics.

## 📌 Project Overview

The objective of this project is to build a scalable healthcare data pipeline that transforms raw MIMIC-III healthcare data into structured, analytics-ready datasets and enables users to interact with the data using natural language.

The project uses **PySpark** for data engineering, **Delta Lake** for data storage, and **Databricks Genie Agent** for GenAI-powered analysis.

## 🔄 Project Workflow

```text
MIMIC-III Dataset
       ↓
Azure Databricks
       ↓
Data Ingestion
       ↓
PySpark Data Engineering
       ↓
EDA & Clinical Analytics
       ↓
Delta Lake
       ↓
Data Visualization
       ↓
Databricks Genie Agent
       ↓
Natural Language Healthcare Insights
```

## 📊 Data Used

The project works with seven core MIMIC-III datasets:

* Patients
* Admissions
* ICU Stays
* Diagnoses
* Procedures
* Prescriptions
* Lab Events

These datasets are processed and analyzed to understand patient demographics, hospital admissions, ICU utilization, clinical diagnoses, procedures, medications, and laboratory events.

## 🛠️ Technologies Used

* **Python**
* **PySpark**
* **SQL**
* **Azure Databricks**
* **Delta Lake**
* **Databricks Genie**
* **MIMIC-III**
* **Data Visualization**

## 🔍 Analytics Performed

The exploratory analysis is divided into four major areas:

### Patient Analytics

* Patient count
* Gender distribution
* Mortality-related analysis

### Hospital Analytics

* Admission types
* Admission locations
* Discharge locations
* Insurance analysis

### ICU Analytics

* ICU utilization
* ICU stays
* Length of stay
* Care-unit analysis
* Patient transfers

### Clinical Analytics

* Diagnoses
* Procedures
* Prescriptions
* Laboratory events

The project identified **136 ICU stays**, with an average ICU length of stay of **4.45 days**. MICU had the highest number of ICU stays with **77**, while CCU had the highest average length of stay at **5.75 days**.

## 🗄️ Delta Lake

After data engineering and analytics, the healthcare datasets are stored as Delta tables under the `healthcare_delta` schema:

```text
workspace.healthcare_delta
│
├── patients
├── admissions
├── icustays
├── diagnoses
├── procedures
├── prescriptions
└── labevents
```

These Delta tables serve as the data source for the GenAI application.

## 🤖 GenAI Integration

The project integrates the Delta Lake layer with a **Databricks Genie Agent**.

Users can ask healthcare questions in natural language. Genie interprets the question, identifies the relevant healthcare table, generates SQL, executes the query against Delta Lake, and returns the result in natural language.

### Example

**Question:**

> What is the average ICU length of stay?

**Result:**

> Average ICU Length of Stay = **4.45 days**

The generated SQL queries the `workspace.healthcare_delta.icustays` table, demonstrating that the response is generated from the actual project data rather than being hardcoded.

## 🎯 Key Objectives

* Build an end-to-end healthcare data pipeline.
* Ingest and process MIMIC-III healthcare data.
* Perform scalable data processing using PySpark.
* Conduct patient, hospital, ICU, and clinical analytics.
* Store processed healthcare data in Delta Lake.
* Create meaningful visualizations and insights.
* Integrate GenAI with healthcare data.
* Enable natural-language querying through Databricks Genie.

## 📈 Project Outcome

This project demonstrates the complete journey from:

**Raw Healthcare Data → Data Engineering → Analytics → Delta Lake → GenAI → Natural Language Insights**

It showcases practical skills in **data engineering, big-data processing, healthcare analytics, cloud technologies, data visualization, and Generative AI**.
