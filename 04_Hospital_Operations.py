# Databricks notebook source
# MAGIC %md
# MAGIC # 🏥 Hospital Operations Dashboard
# MAGIC
# MAGIC ## Healthcare Analytics Pipeline using Azure Blob, Databricks, Delta Lake & GenAI
# MAGIC
# MAGIC ### Objective
# MAGIC
# MAGIC This notebook analyzes hospital admission records from the MIMIC-III sample dataset.
# MAGIC
# MAGIC The objective is to understand patient admission patterns, hospital utilization, insurance coverage, discharge outcomes, and demographic characteristics.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Business Questions
# MAGIC
# MAGIC 1. What types of admissions are most common?
# MAGIC 2. Where do patients come from?
# MAGIC 3. Where are patients discharged?
# MAGIC 4. What is the average hospital length of stay?
# MAGIC 5. Which insurance providers cover most patients?
# MAGIC 6. What are the demographic characteristics of admitted patients?
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Dataset Used
# MAGIC
# MAGIC ADMISSIONS.csv

# COMMAND ----------

from pyspark.sql.functions import *
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

# COMMAND ----------



admissions_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("/Volumes/workspace/default/mimic3_data/ADMISSIONS.csv")
)

# COMMAND ----------

print("=" * 60)
print("ADMISSIONS DATASET VALIDATION")
print("=" * 60)

print("Rows :", admissions_df.count())
print("Columns :", len(admissions_df.columns))

admissions_df.printSchema()

display(admissions_df.limit(5))

# COMMAND ----------

# Total Admissions
total_admissions = admissions_df.count()

# Hospital Deaths
hospital_deaths = admissions_df.filter(
    col("hospital_expire_flag") == 1
).count()

# Average Length of Stay (Days)
admissions_los = admissions_df.withColumn(
    "los_days",
    datediff(col("dischtime"), col("admittime"))
)

average_los = admissions_los.select(
    round(avg("los_days"), 2)
).first()[0]

# Most Common Insurance
top_insurance = (
    admissions_df
    .groupBy("insurance")
    .count()
    .orderBy(desc("count"))
    .first()[0]
)

print("=" * 60)
print("HOSPITAL OPERATIONS KPI")
print("=" * 60)

print(f"🏥 Total Admissions      : {total_admissions}")
print(f"⚕️ Hospital Deaths      : {hospital_deaths}")
print(f"📅 Average LOS (Days)   : {average_los}")
print(f"💳 Top Insurance        : {top_insurance}")

# COMMAND ----------

admission_type_df = (
    admissions_df
    .groupBy("admission_type")
    .count()
    .orderBy(desc("count"))
)

display(admission_type_df)

# COMMAND ----------

admission_type_pd = admission_type_df.toPandas()

# COMMAND ----------

import plotly.express as px

fig = px.bar(
    admission_type_pd,
    x="admission_type",
    y="count",
    text="count",
    title="Admission Type Distribution",
    color="admission_type"
)

fig.update_traces(textposition="outside")

fig.update_layout(
    xaxis_title="Admission Type",
    yaxis_title="Number of Admissions",
    template="plotly_white",
    showlegend=False,
    title_x=0.5,
    height=500
)

fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Business Insight
# MAGIC
# MAGIC The chart shows the distribution of hospital admissions by admission type.
# MAGIC
# MAGIC Emergency admissions indicate unplanned healthcare demand, while elective admissions represent scheduled care. Understanding this distribution helps hospitals optimize staffing, bed allocation, and emergency preparedness.

# COMMAND ----------

admission_location_df = (
    admissions_df
    .groupBy("admission_location")
    .count()
    .orderBy(desc("count"))
)

display(admission_location_df)

# COMMAND ----------

admission_location_pd = admission_location_df.toPandas()

# COMMAND ----------

import plotly.express as px

fig = px.bar(
    admission_location_pd,
    x="count",
    y="admission_location",
    orientation="h",
    text="count",
    color="count",
    color_continuous_scale="Blues",
    title="Patient Admission Source"
)

fig.update_traces(textposition="outside")

fig.update_layout(
    template="plotly_white",
    title_x=0.5,
    xaxis_title="Number of Admissions",
    yaxis_title="Admission Source",
    height=550
)

fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Business Insight
# MAGIC
# MAGIC The visualization shows where patients originated before hospital admission.
# MAGIC
# MAGIC Understanding admission sources helps hospitals identify the primary entry points for patients and supports operational planning for emergency departments, referral networks, and inter-hospital transfers.

# COMMAND ----------

insurance_df = (
    admissions_df
    .groupBy("insurance")
    .count()
    .orderBy(desc("count"))
)

display(insurance_df)

# COMMAND ----------

insurance_pd = insurance_df.toPandas()

# COMMAND ----------

fig = px.pie(
    insurance_pd,
    names="insurance",
    values="count",
    title="Insurance Distribution"
)

fig.update_layout(
    template="plotly_white",
    title_x=0.5,
    height=500
)

fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Business Insight
# MAGIC
# MAGIC Insurance distribution provides insight into the financial profile of the patient population.
# MAGIC
# MAGIC Understanding the proportion of Medicare, Medicaid, Private, and Government insurance helps hospitals forecast reimbursement patterns and support financial planning.

# COMMAND ----------

discharge_df = (
    admissions_df
    .groupBy("discharge_location")
    .count()
    .orderBy(desc("count"))
)

display(discharge_df)

# COMMAND ----------

discharge_pd = discharge_df.toPandas()

# COMMAND ----------

import plotly.express as px

fig = px.bar(
    discharge_pd,
    x="count",
    y="discharge_location",
    orientation="h",
    text="count",
    color="count",
    color_continuous_scale="Greens",
    title="Patient Discharge Destination"
)

fig.update_traces(textposition="outside")

fig.update_layout(
    template="plotly_white",
    title_x=0.5,
    xaxis_title="Number of Patients",
    yaxis_title="Discharge Location",
    height=600
)

fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Business Insight
# MAGIC
# MAGIC This visualization shows the destinations of patients after discharge.
# MAGIC
# MAGIC Most patients are expected to return home, while others require continued care through rehabilitation centers, nursing facilities, hospice services, or other healthcare institutions.
# MAGIC
# MAGIC Understanding discharge destinations helps hospitals improve discharge planning, optimize bed availability, and coordinate post-hospital care.

# COMMAND ----------

religion_df = (
    admissions_df
    .groupBy("religion")
    .count()
    .orderBy(desc("count"))
)

display(religion_df)

# COMMAND ----------

religion_pd = religion_df.toPandas()

# COMMAND ----------

fig = px.bar(
    religion_pd,
    x="religion",
    y="count",
    text="count",
    color="count",
    color_continuous_scale="Purples",
    title="Religion Distribution"
)

fig.update_layout(
    template="plotly_white",
    title_x=0.5,
    xaxis_title="Religion",
    yaxis_title="Patients",
    height=500
)

fig.update_traces(textposition="outside")

fig.show()

# COMMAND ----------

ethnicity_df = (
    admissions_df
    .groupBy("ethnicity")
    .count()
    .orderBy(desc("count"))
)

display(ethnicity_df)

# COMMAND ----------

ethnicity_pd = ethnicity_df.toPandas()

# COMMAND ----------

fig = px.bar(
    ethnicity_pd,
    x="count",
    y="ethnicity",
    orientation="h",
    text="count",
    color="count",
    color_continuous_scale="Oranges",
    title="Ethnicity Distribution"
)

fig.update_layout(
    template="plotly_white",
    title_x=0.5,
    xaxis_title="Patients",
    yaxis_title="Ethnicity",
    height=600
)

fig.update_traces(textposition="outside")

fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Business Insight
# MAGIC
# MAGIC Ethnicity analysis helps understand the diversity of the patient population and provides context for healthcare utilization and population-based studies.

# COMMAND ----------

los_df = admissions_df.withColumn(
    "length_of_stay",
    datediff(col("dischtime"), col("admittime"))
)

display(
    los_df.select(
        "subject_id",
        "admittime",
        "dischtime",
        "length_of_stay"
    )
)

# COMMAND ----------

los_df.select("length_of_stay").describe().show()

# COMMAND ----------

los_df.select("length_of_stay").describe().show()

# COMMAND ----------

los_pd = los_df.select("length_of_stay").toPandas()

# COMMAND ----------

fig = px.histogram(
    los_pd,
    x="length_of_stay",
    nbins=15,
    title="Hospital Length of Stay Distribution"
)

fig.update_layout(
    template="plotly_white",
    title_x=0.5,
    xaxis_title="Length of Stay (Days)",
    yaxis_title="Number of Admissions",
    height=500
)

fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Business Insight
# MAGIC
# MAGIC Length of Stay (LOS) is a key hospital performance indicator.
# MAGIC
# MAGIC Understanding LOS helps hospitals evaluate bed utilization, patient throughput, and operational efficiency.

# COMMAND ----------

expire_df = (
    admissions_df
    .groupBy("hospital_expire_flag")
    .count()
)

display(expire_df)

# COMMAND ----------

expire_pd = expire_df.toPandas()

expire_pd["Status"] = expire_pd["hospital_expire_flag"].map({
    0: "Alive",
    1: "Expired"
})

# COMMAND ----------

fig = px.pie(
    expire_pd,
    names="Status",
    values="count",
    title="Hospital Outcome Distribution"
)

fig.update_layout(
    template="plotly_white",
    title_x=0.5,
    height=500
)

fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Business Insight
# MAGIC
# MAGIC This chart summarizes hospital outcomes for the admission records.
# MAGIC
# MAGIC Monitoring hospital mortality is an important quality indicator and helps evaluate clinical outcomes across the patient population.

# COMMAND ----------

