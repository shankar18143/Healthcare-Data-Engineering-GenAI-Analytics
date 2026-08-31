# Databricks notebook source
# MAGIC %md
# MAGIC # 🩺 Clinical Analytics Dashboard
# MAGIC
# MAGIC ## Objective
# MAGIC
# MAGIC This notebook analyzes clinical information from the MIMIC-III sample dataset.
# MAGIC
# MAGIC The analysis focuses on diagnoses, procedures, medications, and laboratory events to understand disease patterns and treatment practices.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Business Questions
# MAGIC
# MAGIC 1. What are the most common diagnoses?
# MAGIC 2. What procedures are performed most frequently?
# MAGIC 3. Which medications are prescribed most often?
# MAGIC 4. Which laboratory tests are performed most frequently?
# MAGIC 5. What clinical insights can be derived from the data?
# MAGIC
# MAGIC Dataset Used
# MAGIC
# MAGIC • DIAGNOSES_ICD
# MAGIC • PROCEDURES_ICD
# MAGIC • PRESCRIPTIONS
# MAGIC • LABEVENTS

# COMMAND ----------

diagnosis_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("/Volumes/workspace/default/mimic3_data/DIAGNOSES_ICD.csv")
)

print("Rows :", diagnosis_df.count())

print("Columns :", len(diagnosis_df.columns))

diagnosis_df.printSchema()

display(diagnosis_df.limit(5))

# COMMAND ----------

procedure_df =  (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("/Volumes/workspace/default/mimic3_data/PROCEDURES_ICD.csv")
)
print("Rows :", procedure_df.count())

print("Columns :", len(procedure_df.columns))

procedure_df.printSchema()

display(procedure_df.limit(5))

# COMMAND ----------

prescription_df =  (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("/Volumes/workspace/default/mimic3_data/PRESCRIPTIONS.csv")
)
print("Rows :", prescription_df.count())

print("Columns :", len(prescription_df.columns))

prescription_df.printSchema()

display(prescription_df.limit(5))

# COMMAND ----------

lab_df =   (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("/Volumes/workspace/default/mimic3_data/LABEVENTS.csv")
)
print("Rows :", lab_df.count())

print("Columns :", len(lab_df.columns))

lab_df.printSchema()

display(lab_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC # 🩺 Clinical Analytics Dashboard
# MAGIC
# MAGIC ## Objective
# MAGIC
# MAGIC This notebook analyzes clinical data from the MIMIC-III sample dataset to identify disease patterns, treatment practices, medication usage, and laboratory investigations.
# MAGIC
# MAGIC ### Business Questions
# MAGIC
# MAGIC 1. What are the most common diagnoses?
# MAGIC 2. Which procedures are performed most frequently?
# MAGIC 3. Which medications are prescribed most often?
# MAGIC 4. Which laboratory tests are performed most frequently?
# MAGIC 5. What clinical insights can be derived from the available data?
# MAGIC
# MAGIC ### Datasets Used
# MAGIC
# MAGIC - DIAGNOSES_ICD
# MAGIC - PROCEDURES_ICD
# MAGIC - PRESCRIPTIONS
# MAGIC - LABEVENTS

# COMMAND ----------

from pyspark.sql.functions import *

total_diagnoses = diagnosis_df.count()
total_procedures = procedure_df.count()
total_prescriptions = prescription_df.count()
total_lab_events = lab_df.count()

print("="*60)
print("CLINICAL ANALYTICS KPI")
print("="*60)

print(f"🩺 Total Diagnoses     : {total_diagnoses}")
print(f"⚕️ Total Procedures    : {total_procedures}")
print(f"💊 Total Prescriptions : {total_prescriptions}")
print(f"🧪 Total Lab Events    : {total_lab_events}")

# COMMAND ----------

diagnosis_count = (
    diagnosis_df
    .groupBy("icd9_code")
    .count()
    .orderBy(desc("count"))
    .limit(10)
)

display(diagnosis_count)

# COMMAND ----------

diagnosis_pd = diagnosis_count.toPandas()

# COMMAND ----------

import plotly.express as px

fig = px.bar(
    diagnosis_pd,
    x="count",
    y="icd9_code",
    orientation="h",
    text="count",
    color="count",
    color_continuous_scale="Blues",
    title="Top 10 Diagnosis Codes"
)

fig.update_layout(
    template="plotly_white",
    title_x=0.5,
    yaxis_title="ICD-9 Diagnosis Code",
    xaxis_title="Frequency"
)

fig.show()

# COMMAND ----------

procedure_count = (
    procedure_df
    .groupBy("icd9_code")
    .count()
    .orderBy(desc("count"))
    .limit(10)
)

display(procedure_count)

# COMMAND ----------

procedure_pd = procedure_count.toPandas()

# COMMAND ----------

fig = px.bar(
    procedure_pd,
    x="count",
    y="icd9_code",
    orientation="h",
    text="count",
    color="count",
    color_continuous_scale="Greens",
    title="Top 10 Procedure Codes"
)

fig.update_layout(
    template="plotly_white",
    title_x=0.5
)

fig.show()

# COMMAND ----------

# DBTITLE 1,medication
drug_count = (
    prescription_df
    .groupBy("drug")
    .count()
    .orderBy(desc("count"))
    .limit(10)
)

display(drug_count)

# COMMAND ----------

drug_pd = drug_count.toPandas()

# COMMAND ----------

fig = px.bar(
    drug_pd,
    x="count",
    y="drug",
    orientation="h",
    text="count",
    color="count",
    color_continuous_scale="Reds",
    title="Top 10 Prescribed Medications"
)

fig.update_layout(
    template="plotly_white",
    title_x=0.5,
    height=600
)

fig.show()

# COMMAND ----------

# DBTITLE 1,labaratory events
labitems_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("/Volumes/workspace/default/mimic3_data/D_LABITEMS.csv")
)

print(labitems_df.count())

display(labitems_df.limit(5))

# COMMAND ----------

lab_analysis_df = (
    lab_df.join(
        labitems_df,
        on="itemid",
        how="left"
    )
)

# COMMAND ----------

top_lab_tests = (
    lab_analysis_df
    .groupBy("label")
    .count()
    .orderBy(desc("count"))
    .limit(10)
)

display(top_lab_tests)

# COMMAND ----------

top_lab_pd = top_lab_tests.toPandas()

# COMMAND ----------

import plotly.express as px

fig = px.bar(
    top_lab_pd,
    x="count",
    y="label",
    orientation="h",
    text="count",
    color="count",
    color_continuous_scale="Teal",
    title="Top 10 Laboratory Tests"
)

fig.update_layout(
    template="plotly_white",
    title_x=0.5,
    xaxis_title="Number of Tests",
    yaxis_title="Laboratory Test",
    height=600
)

fig.update_traces(textposition="outside")

fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Business Insight
# MAGIC
# MAGIC The visualization highlights the most frequently performed laboratory investigations.
# MAGIC
# MAGIC Frequently ordered laboratory tests provide insight into routine clinical monitoring and diagnostic priorities. Understanding laboratory utilization helps hospitals optimize laboratory resources and identify commonly monitored clinical parameters.

# COMMAND ----------

# MAGIC %md
# MAGIC # Executive Insights
# MAGIC
# MAGIC 1. The clinical dataset contains **1,761 diagnosis records**, **506 procedure records**, **10,398 prescription records**, and **76,074 laboratory events**.
# MAGIC
# MAGIC 2. The most frequently recorded diagnosis code was **4019** with **85 occurrences**.
# MAGIC
# MAGIC 3. The most frequently performed procedure code was **3893** with **42 occurrences**.
# MAGIC
# MAGIC 4. The most commonly prescribed medication was **Aspirin** with **310 prescriptions**.
# MAGIC
# MAGIC 5. The most frequently performed laboratory test was **Glucose** with **1,450 tests**.
# MAGIC
# MAGIC These findings provide a comprehensive overview of disease patterns, treatment practices, medication utilization, and laboratory testing within the available clinical dataset.

# COMMAND ----------

