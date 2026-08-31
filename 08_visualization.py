# Databricks notebook source
# MAGIC %md
# MAGIC # 📊 Healthcare Analytics Visualization
# MAGIC
# MAGIC ## Objective
# MAGIC
# MAGIC This notebook presents the key visualizations generated from the cleaned healthcare datasets.
# MAGIC
# MAGIC The visualizations summarize:
# MAGIC
# MAGIC - Patient demographics
# MAGIC - Hospital operations
# MAGIC - ICU utilization
# MAGIC - Clinical activity
# MAGIC
# MAGIC The objective is to communicate the major findings of the healthcare analytics pipeline through clear and interactive visualizations.

# COMMAND ----------

from pyspark.sql.functions import *
import pandas as pd
import plotly.express as px

patients = spark.table("healthcare_delta.patients")
admissions = spark.table("healthcare_delta.admissions")
icu = spark.table("healthcare_delta.icustays")
diagnoses = spark.table("healthcare_delta.diagnoses")
procedures = spark.table("healthcare_delta.procedures")
prescriptions = spark.table("healthcare_delta.prescriptions")
labevents = spark.table("healthcare_delta.labevents")

# COMMAND ----------

# MAGIC %md
# MAGIC # 👥 Patient Analytics
# MAGIC
# MAGIC This section presents the demographic and mortality characteristics of the patient cohort.

# COMMAND ----------

gender_pd = (
    patients
    .groupBy("gender")
    .count()
    .orderBy(desc("count"))
    .toPandas()
)

fig = px.bar(
    gender_pd,
    x="gender",
    y="count",
    text="count",
    title="Gender Distribution"
)

fig.update_layout(
    template="plotly_white",
    title_x=0.5,
    xaxis_title="Gender",
    yaxis_title="Number of Patients"
)

fig.update_traces(textposition="outside")

fig.show()

# COMMAND ----------

# DBTITLE 1,deceased
mortality_pd = (
    patients
    .groupBy("expire_flag")
    .count()
    .toPandas()
)

mortality_pd["status"] = mortality_pd["expire_flag"].map({
    0: "Alive",
    1: "Deceased"
})

fig = px.pie(
    mortality_pd,
    names="status",
    values="count",
    title="Patient Mortality Distribution"
)

fig.update_layout(
    template="plotly_white",
    title_x=0.5
)

fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC # 🏥 Hospital Operations
# MAGIC
# MAGIC This section visualizes admission patterns, patient flow, insurance distribution, and hospital outcomes.

# COMMAND ----------

admission_type_pd = (
    admissions
    .groupBy("admission_type")
    .count()
    .orderBy(desc("count"))
    .toPandas()
)

fig = px.bar(
    admission_type_pd,
    x="admission_type",
    y="count",
    text="count",
    title="Admission Type Distribution"
)

fig.update_layout(
    template="plotly_white",
    title_x=0.5,
    xaxis_title="Admission Type",
    yaxis_title="Admissions"
)

fig.update_traces(textposition="outside")

fig.show()

# COMMAND ----------

insurance_pd = (
    admissions
    .groupBy("insurance")
    .count()
    .orderBy(desc("count"))
    .toPandas()
)

fig = px.pie(
    insurance_pd,
    names="insurance",
    values="count",
    title="Insurance Distribution"
)

fig.update_layout(
    template="plotly_white",
    title_x=0.5
)

fig.show()

# COMMAND ----------

discharge_pd = (
    admissions
    .groupBy("discharge_location")
    .count()
    .orderBy(desc("count"))
    .toPandas()
)

fig = px.bar(
    discharge_pd,
    x="count",
    y="discharge_location",
    orientation="h",
    text="count",
    title="Discharge Location Distribution"
)

fig.update_layout(
    template="plotly_white",
    title_x=0.5,
    height=600,
    xaxis_title="Admissions",
    yaxis_title="Discharge Location"
)

fig.update_traces(textposition="outside")

fig.show()

# COMMAND ----------

hospital_los_pd = (
    admissions
    .withColumn(
        "length_of_stay",
        datediff(col("dischtime"), col("admittime"))
    )
    .select("length_of_stay")
    .filter(col("length_of_stay").isNotNull())
    .toPandas()
)

fig = px.histogram(
    hospital_los_pd,
    x="length_of_stay",
    nbins=20,
    title="Hospital Length of Stay Distribution"
)

fig.update_layout(
    template="plotly_white",
    title_x=0.5,
    xaxis_title="Length of Stay (Days)",
    yaxis_title="Number of Admissions"
)

fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC # 🏥 ICU Analytics
# MAGIC
# MAGIC This section presents ICU utilization, length of stay, and patient movement between care units.

# COMMAND ----------

first_unit_pd = (
    icu
    .groupBy("first_careunit")
    .count()
    .orderBy(desc("count"))
    .toPandas()
)

fig = px.bar(
    first_unit_pd,
    x="first_careunit",
    y="count",
    text="count",
    title="ICU Admissions by First Care Unit"
)

fig.update_layout(
    template="plotly_white",
    title_x=0.5,
    xaxis_title="ICU Care Unit",
    yaxis_title="ICU Stays"
)

fig.update_traces(textposition="outside")

fig.show()

# COMMAND ----------

icu_los_pd = (
    icu
    .groupBy("first_careunit")
    .agg(
        round(avg("los"), 2).alias("average_los")
    )
    .orderBy(desc("average_los"))
    .toPandas()
)

fig = px.bar(
    icu_los_pd,
    x="first_careunit",
    y="average_los",
    text="average_los",
    title="Average ICU Length of Stay by Care Unit"
)

fig.update_layout(
    template="plotly_white",
    title_x=0.5,
    xaxis_title="ICU Care Unit",
    yaxis_title="Average LOS (Days)"
)

fig.update_traces(textposition="outside")

fig.show()

# COMMAND ----------

transfer_pd = (
    icu
    .withColumn(
        "transfer_status",
        when(
            col("first_careunit") == col("last_careunit"),
            "No Transfer"
        ).otherwise("Transferred")
    )
    .groupBy("transfer_status")
    .count()
    .toPandas()
)

fig = px.pie(
    transfer_pd,
    names="transfer_status",
    values="count",
    title="ICU Patient Transfer Distribution"
)

fig.update_layout(
    template="plotly_white",
    title_x=0.5
)

fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC # 🩺 Clinical Analytics
# MAGIC
# MAGIC This section presents the most frequently recorded diagnoses, procedures, medications, and laboratory investigations.

# COMMAND ----------

top_diagnoses = (
    diagnoses
    .groupBy("icd9_code")
    .count()
    .orderBy(desc("count"))
    .limit(10)
)

display(top_diagnoses)

# COMMAND ----------

top_diagnoses_pd = top_diagnoses.toPandas()

# COMMAND ----------

fig = px.bar(
    top_diagnoses_pd,
    x="count",
    y="icd9_code",
    orientation="h",
    text="count",
    title="Top 10 Diagnosis Codes"
)

fig.update_layout(
    template="plotly_white",
    title_x=0.5,
    xaxis_title="Number of Records",
    yaxis_title="ICD-9 Diagnosis Code",
    height=550
)

fig.update_traces(textposition="outside")

fig.show()

# COMMAND ----------

# DBTITLE 1,procedure_codes
top_procedures = (
    procedures
    .groupBy("icd9_code")
    .count()
    .orderBy(desc("count"))
    .limit(10)
)

display(top_procedures)

# COMMAND ----------

top_procedures_pd = top_procedures.toPandas()

# COMMAND ----------

import plotly.express as px

# Convert icd9_code to string for categorical display
top_procedures_pd['icd9_code'] = top_procedures_pd['icd9_code'].astype(str)

fig = px.bar(
    top_procedures_pd,
    x="count",
    y="icd9_code",
    orientation="h",
    text="count",
    title="Top 10 Procedure Codes"
)

fig.update_layout(
    template="plotly_white",
    title_x=0.5,
    xaxis_title="Number of Records",
    yaxis_title="ICD-9 Procedure Code",
    height=550
)

fig.update_traces(textposition="outside")

fig.show()

# COMMAND ----------

# DBTITLE 1,medication
top_medications = (
    prescriptions
    .filter(col("drug").isNotNull())
    .groupBy("drug")
    .count()
    .orderBy(desc("count"))
    .limit(10)
)

display(top_medications)

# COMMAND ----------

top_medications_pd = top_medications.toPandas()

# COMMAND ----------

fig = px.bar(
    top_medications_pd,
    x="count",
    y="drug",
    orientation="h",
    text="count",
    title="Top 10 Prescribed Medications"
)

fig.update_layout(
    template="plotly_white",
    title_x=0.5,
    xaxis_title="Number of Prescriptions",
    yaxis_title="Medication",
    height=600
)

fig.update_traces(textposition="outside")

fig.show()

# COMMAND ----------

# DBTITLE 1,lab_events
lab_visual_df = labevents

# COMMAND ----------

# DBTITLE 1,Cell 26
top_lab_tests = (
    lab_visual_df
    .filter(col("itemid").isNotNull())
    .groupBy("itemid")
    .count()
    .orderBy(desc("count"))
    .limit(10)
)

display(top_lab_tests)

# COMMAND ----------

top_lab_tests_pd = top_lab_tests.toPandas()

# COMMAND ----------

# DBTITLE 1,Cell 28
fig = px.bar(
    top_lab_tests_pd,
    x="count",
    y="itemid",
    orientation="h",
    text="count",
    title="Top 10 Laboratory Tests"
)

fig.update_layout(
    template="plotly_white",
    title_x=0.5,
    xaxis_title="Number of Tests",
    yaxis_title="Lab Test Item ID",
    height=600
)

fig.update_traces(textposition="outside")

fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC # 📌 Overall Healthcare Analytics Summary
# MAGIC
# MAGIC ## Patient Analytics
# MAGIC
# MAGIC The patient analysis examined demographic characteristics and mortality patterns within the available patient cohort.
# MAGIC
# MAGIC ## Hospital Operations
# MAGIC
# MAGIC Hospital analytics explored admission patterns, insurance distribution, discharge destinations, and length of stay to understand healthcare service utilization.
# MAGIC
# MAGIC ## ICU Analytics
# MAGIC
# MAGIC ICU analysis examined care-unit utilization, ICU length of stay, and patient transfers. MICU recorded the highest number of ICU stays, while CCU had the highest average ICU length of stay.
# MAGIC
# MAGIC ## Clinical Analytics
# MAGIC
# MAGIC Clinical analysis examined diagnosis codes, procedure codes, medication usage, and laboratory investigations to identify the most frequently recorded clinical activities.
# MAGIC
# MAGIC ## Overall Conclusion
# MAGIC
# MAGIC The visualization dashboard brings together the major findings from patient, hospital, ICU, and clinical analytics into a single presentation-oriented view.
# MAGIC
# MAGIC These visualizations provide a clear overview of patient characteristics, hospital operations, critical-care utilization, and clinical activity.

# COMMAND ----------

