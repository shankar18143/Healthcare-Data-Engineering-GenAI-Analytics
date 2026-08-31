# Databricks notebook source
# MAGIC %md
# MAGIC # 🏥 Patient Profile Dashboard
# MAGIC
# MAGIC ## Healthcare Analytics Pipeline using Azure, Databricks, Delta Lake & GenAI
# MAGIC
# MAGIC ### Objective
# MAGIC
# MAGIC This notebook analyzes patient demographic information from the MIMIC-III sample dataset. It provides an overview of the patient cohort by examining gender distribution, mortality characteristics, hospital deaths, age distribution, and data quality.
# MAGIC
# MAGIC ### Business Questions
# MAGIC
# MAGIC 1. What is the gender distribution of the patient cohort?
# MAGIC 2. What is the mortality status of the patient cohort?
# MAGIC 3. How many deaths occurred during hospitalization?
# MAGIC 4. What is the age distribution of the patients?
# MAGIC 5. What is the quality of the demographic data?
# MAGIC
# MAGIC ### Dataset Used
# MAGIC
# MAGIC **PATIENTS.csv**

# COMMAND ----------

from pyspark.sql.functions import *
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

# COMMAND ----------

patients_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("/Volumes/workspace/default/mimic3_data/PATIENTS.csv")
)

# COMMAND ----------

display(patients_df)

# COMMAND ----------

print("="*60)
print("PATIENTS DATASET VALIDATION")
print("="*60)

print("Total Rows :", patients_df.count())
print("Total Columns :", len(patients_df.columns))

patients_df.printSchema()

display(patients_df.limit(10))

# COMMAND ----------

total_patients = patients_df.count()

male_patients = patients_df.filter(col("gender")=="M").count()

female_patients = patients_df.filter(col("gender")=="F").count()

deceased_patients = patients_df.filter(col("expire_flag")==1).count()

hospital_deaths = patients_df.filter(col("dod_hosp").isNotNull()).count()

print("Total Patients :", total_patients)
print("Male :", male_patients)
print("Female :", female_patients)
print("Mortality :", deceased_patients)
print("Hospital Death :", hospital_deaths)

# COMMAND ----------

gender_df = (
    patients_df
    .groupBy("gender")
    .count()
    .orderBy(desc("count"))
)

gender_pd = gender_df.toPandas()

plt.figure(figsize=(6,4))

plt.bar(
    gender_pd["gender"],
    gender_pd["count"]
)

plt.title("Gender Distribution")

plt.xlabel("Gender")

plt.ylabel("Patients")

plt.grid(alpha=0.3)

plt.show()

# COMMAND ----------

mortality_df = (
    patients_df
    .groupBy("expire_flag")
    .count()
)

mortality_pd = mortality_df.toPandas()

plt.figure(figsize=(6,6))

plt.pie(
    mortality_pd["count"],
    labels=["Deceased"],
    autopct="%1.1f%%",
    startangle=90
)

plt.title("Mortality Distribution")

plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC The provided patient sample contains only deceased patients (expire_flag = 1). Therefore, this chart describes the cohort rather than comparing survivors and non-survivors

# COMMAND ----------

hospital_df = (
    patients_df
    .withColumn(
        "Hospital Death",
        when(col("dod_hosp").isNull(),"No")
        .otherwise("Yes")
    )
    .groupBy("Hospital Death")
    .count()
)

hospital_pd = hospital_df.toPandas()

plt.figure(figsize=(6,4))

plt.bar(
    hospital_pd["Hospital Death"],
    hospital_pd["count"]
)

plt.title("Hospital Death Distribution")

plt.ylabel("Patients")

plt.show()

# COMMAND ----------

null_summary = []

for column in patients_df.columns:
    nulls = patients_df.filter(col(column).isNull()).count()
    null_summary.append([column, nulls])

null_df = pd.DataFrame(
    null_summary,
    columns=["Column","Missing Values"]
)

null_df

# COMMAND ----------

plt.figure(figsize=(8,4))

plt.barh(
    null_df["Column"],
    null_df["Missing Values"]
)

plt.title("Missing Values by Column")

plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC # Executive Insights
# MAGIC
# MAGIC - The patient cohort consists of 100 records.
# MAGIC - Female patients slightly outnumber male patients.
# MAGIC - All records belong to a deceased patient cohort.
# MAGIC - Not every death was recorded as an in-hospital death.
# MAGIC - Demographic fields are largely complete, while some hospital death information is missing.

# COMMAND ----------

# MAGIC %md
# MAGIC # Conclusion
# MAGIC
# MAGIC The Patient Profile Dashboard provides an overview of the demographic characteristics of the available patient cohort.
# MAGIC
# MAGIC The analysis established a baseline understanding of the dataset and identified that the provided sample contains only deceased patients. This finding influences the interpretation of mortality-related analyses and is documented as a dataset characteristic rather than a limitation of the analysis.
# MAGIC
# MAGIC The next notebook focuses on Hospital Operations using the ADMISSIONS dataset, where richer operational insights such as admission type, insurance, length of stay, and discharge patterns will be explored.

# COMMAND ----------

