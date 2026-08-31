# Databricks notebook source
# MAGIC %md
# MAGIC # 💎 Delta Lake Implementation
# MAGIC
# MAGIC ## Objective
# MAGIC
# MAGIC This notebook converts the cleaned MIMIC-III datasets into Delta Lake tables.
# MAGIC
# MAGIC Delta Lake provides:
# MAGIC - ACID transactions
# MAGIC - Schema enforcement
# MAGIC - Faster queries
# MAGIC - Reliable storage
# MAGIC - Support for downstream analytics and AI applications

# COMMAND ----------

spark.sql("""
CREATE DATABASE IF NOT EXISTS healthcare_delta
""")

# COMMAND ----------

spark.sql("""
USE healthcare_delta
""")

# COMMAND ----------

patients_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("/Volumes/workspace/default/mimic3_data/PATIENTS.csv")
)

patients_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("patients")

# COMMAND ----------

admissions_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("/Volumes/workspace/default/mimic3_data/ADMISSIONS.csv")
)
admissions_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("admissions")

# COMMAND ----------

icu_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("/Volumes/workspace/default/mimic3_data/ICUSTAYS.csv")
)
icu_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("icustays")

# COMMAND ----------

diagnosis_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("/Volumes/workspace/default/mimic3_data/DIAGNOSES_ICD.csv")
)
diagnosis_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("diagnoses")

# COMMAND ----------

procedure_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("/Volumes/workspace/default/mimic3_data/PROCEDURES_ICD.csv")
)
procedure_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("procedures")

# COMMAND ----------

prescription_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("/Volumes/workspace/default/mimic3_data/PRESCRIPTIONS.csv")
)
prescription_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("prescriptions")

# COMMAND ----------

lab_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("/Volumes/workspace/default/mimic3_data/LABEVENTS.csv")
)
lab_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("labevents")

# COMMAND ----------

spark.sql("SHOW TABLES").show(truncate=False)

# COMMAND ----------

spark.sql("""
SELECT COUNT(*)
FROM patients
""").show()

# COMMAND ----------

spark.sql("""
SELECT COUNT(*)
FROM admissions
""").show()

# COMMAND ----------

spark.sql("""
SELECT COUNT(*)
FROM icustays
""").show()

# COMMAND ----------

spark.sql("""
SELECT COUNT(*)
FROM diagnoses
""").show()

# COMMAND ----------

# MAGIC %md
# MAGIC # Conclusion
# MAGIC
# MAGIC All cleaned healthcare datasets have been successfully stored as Delta Lake tables.
# MAGIC
# MAGIC These Delta tables provide a reliable and optimized storage layer for advanced analytics, dashboarding, and GenAI-powered healthcare applications.

# COMMAND ----------

