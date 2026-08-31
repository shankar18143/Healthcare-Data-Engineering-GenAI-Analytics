# Databricks notebook source
# Load DataFrames from UC Volume
DATASETS = [
    "ADMISSIONS.csv", "CALLOUT.csv", "CAREGIVERS.csv", "CHARTEVENTS.csv",
    "CPTEVENTS.csv", "D_CPT.csv", "D_ICD_DIAGNOSES.csv", "D_ICD_PROCEDURES.csv",
    "D_ITEMS.csv", "D_LABITEMS.csv", "DATETIMEEVENTS.csv", "DIAGNOSES_ICD.csv",
    "DRGCODES.csv", "ICUSTAYS.csv", "INPUTEVENTS_CV.csv", "INPUTEVENTS_MV.csv",
    "LABEVENTS.csv", "MICROBIOLOGYEVENTS.csv", "NOTEEVENTS.csv", "OUTPUTEVENTS.csv",
    "PATIENTS.csv", "PRESCRIPTIONS.csv", "PROCEDUREEVENTS_MV.csv", "PROCEDURES_ICD.csv",
    "SERVICES.csv", "TRANSFERS.csv"
]

dataframes = {}
for file in DATASETS:
    table_name = file.replace(".csv", "")
    volume_path = f"/Volumes/workspace/default/mimic3_data/{file}"
    dataframes[table_name] = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(volume_path)
    )

cleaned_dataframes = {}

for name, df in dataframes.items():

    print(f"Cleaning {name}...")

    # Remove duplicate rows
    df = df.dropDuplicates()

    # Remove rows that are completely null
    df = df.na.drop(how="all")

    cleaned_dataframes[name] = df

print("All datasets cleaned successfully.")

# COMMAND ----------

# Print schema for a sample cleaned DataFrame
cleaned_dataframes['PATIENTS'].printSchema()

# COMMAND ----------

display(cleaned_dataframes['PATIENTS'].limit(5))

# COMMAND ----------

