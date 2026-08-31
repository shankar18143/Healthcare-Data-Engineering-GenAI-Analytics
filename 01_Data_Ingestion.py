# Databricks notebook source
# TODO: Update these variables with your actual Azure Blob Storage details
storage_account = "mimic3storage"
container = "mimic3"  # Replace with actual container name
file_path = "PATIENTS.csv"  # Replace with actual file path in container
sas_token = "sv=2026-02-06&ss=bfqt&srt=co&sp=rwdlacupiytfx&se=2026-08-12T01:03:28Z&st=2026-08-06T16:48:28Z&spr=https&sig=1IgoNVR4lJFI%2FcXVqjtp5tsD0lY4yLvKSN7A6iiNc3U%3D"

# Construct the full blob URL
blob_url = f"https://{storage_account}.blob.core.windows.net/{container}/{file_path}?{sas_token}"

import requests

# Download CSV from Azure Blob Storage
response = requests.get(blob_url)
response.raise_for_status()

# Save to volume
import os
volume_path = "/Volumes/workspace/default/mimic3_data/PATIENTS.csv"
os.makedirs(os.path.dirname(volume_path), exist_ok=True)
with open(volume_path, 'wb') as f:
    f.write(response.content)

# Read from volume
patients_df = (
    spark.read
         .option("header", "true")
         .csv(volume_path)
)

patients_df.show(5)

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *

print("Spark Version:", spark.version)

# COMMAND ----------

# Azure Blob Storage Details

STORAGE_ACCOUNT = "mimic3storage"
CONTAINER_NAME = "mimic3"

# Base URL
BASE_URL = f"https://{STORAGE_ACCOUNT}.blob.core.windows.net/{CONTAINER_NAME}"

# Paste ONLY the SAS Token
SAS_TOKEN = "sv=2026-02-06&ss=bfqt&srt=co&sp=rwdlacupiytfx&se=2026-08-12T01:03:28Z&st=2026-08-06T16:48:28Z&spr=https&sig=1IgoNVR4lJFI%2FcXVqjtp5tsD0lY4yLvKSN7A6iiNc3U%3D"

# COMMAND ----------

DATASETS = [
    "ADMISSIONS.csv",
    "CALLOUT.csv",
    "CAREGIVERS.csv",
    "CHARTEVENTS.csv",
    "CPTEVENTS.csv",
    "D_CPT.csv",
    "D_ICD_DIAGNOSES.csv",
    "D_ICD_PROCEDURES.csv",
    "D_ITEMS.csv",
    "D_LABITEMS.csv",
    "DATETIMEEVENTS.csv",
    "DIAGNOSES_ICD.csv",
    "DRGCODES.csv",
    "ICUSTAYS.csv",
    "INPUTEVENTS_CV.csv",
    "INPUTEVENTS_MV.csv",
    "LABEVENTS.csv",
    "MICROBIOLOGYEVENTS.csv",
    "NOTEEVENTS.csv",
    "OUTPUTEVENTS.csv",
    "PATIENTS.csv",
    "PRESCRIPTIONS.csv",
    "PROCEDUREEVENTS_MV.csv",
    "PROCEDURES_ICD.csv",
    "SERVICES.csv",
    "TRANSFERS.csv"
]

# COMMAND ----------

import requests
import os

def load_dataset(file_name):
    # Download from Azure to UC Volume first
    url = f"{BASE_URL}/{file_name}?{SAS_TOKEN}"
    response = requests.get(url)
    response.raise_for_status()
    
    # Save to volume
    volume_path = f"/Volumes/workspace/default/mimic3_data/{file_name}"
    os.makedirs(os.path.dirname(volume_path), exist_ok=True)
    with open(volume_path, 'wb') as f:
        f.write(response.content)
    
    # Read from volume
    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(volume_path)
    )
    
    return df

# COMMAND ----------

dataframes = {}

for file in DATASETS:

    table_name = file.replace(".csv", "")

    print(f"Loading {table_name}...")

    try:

        dataframes[table_name] = load_dataset(file)

        print(f"✓ Loaded {table_name}")

    except Exception as e:

        print(f"✗ Failed {table_name}")

        print(e)

# COMMAND ----------

print(dataframes.keys())

# COMMAND ----------

# Compute stats for all DataFrames before the loop to avoid repeated Analyze RPCs
stats = {}
for name, df in dataframes.items():
    stats[name] = {
        'columns': len(df.columns),
        'rows': df.count()
    }

# Display the stats
for name in stats:
    print("=" * 60)
    print(name)
    print("Rows :", stats[name]['rows'])
    print("Columns :", stats[name]['columns'])
    print("=" * 60)

# COMMAND ----------

print(repr(volume_path))

# COMMAND ----------

print(volume_path.split("/"))

# COMMAND ----------

print("Length:", len(volume_path))
print("Starts with:", volume_path[:30])
print("Ends with:", volume_path[-50:])

# COMMAND ----------

