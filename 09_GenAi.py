# Databricks notebook source
# MAGIC %md
# MAGIC %md
# MAGIC # 🤖 GenAI Healthcare Analytics Application
# MAGIC
# MAGIC ## Objective
# MAGIC
# MAGIC Develop a Generative AI application that allows users to ask
# MAGIC natural-language questions about the healthcare data.
# MAGIC
# MAGIC The application retrieves information from the Delta Lake Raw Layer
# MAGIC and generates understandable analytical responses.
# MAGIC
# MAGIC ## Data Source
# MAGIC
# MAGIC The application uses the following Delta Lake tables:
# MAGIC
# MAGIC - healthcare_delta.patients
# MAGIC - healthcare_delta.admissions
# MAGIC - healthcare_delta.icustays
# MAGIC - healthcare_delta.diagnoses
# MAGIC - healthcare_delta.procedures
# MAGIC - healthcare_delta.prescriptions
# MAGIC - healthcare_delta.labevents
# MAGIC
# MAGIC ## Workflow
# MAGIC
# MAGIC User Question
# MAGIC
# MAGIC       ↓
# MAGIC GenAI Application
# MAGIC
# MAGIC       ↓
# MAGIC Understand User Intent
# MAGIC
# MAGIC       ↓
# MAGIC Identify Required Delta Table
# MAGIC
# MAGIC       ↓
# MAGIC Generate / Execute Analytical Query
# MAGIC
# MAGIC       ↓
# MAGIC Retrieve Data from Delta Lake Raw Layer
# MAGIC
# MAGIC       ↓
# MAGIC Generate Natural-Language Response

# COMMAND ----------

# DBTITLE 1,Delta Raw Layer tables
spark.sql("SHOW TABLES IN healthcare_delta").show(truncate=False)

# COMMAND ----------

# Delta Lake Raw Layer

patients_raw = spark.table("healthcare_delta.patients")
admissions_raw = spark.table("healthcare_delta.admissions")
icustays_raw = spark.table("healthcare_delta.icustays")
diagnoses_raw = spark.table("healthcare_delta.diagnoses")
procedures_raw = spark.table("healthcare_delta.procedures")
prescriptions_raw = spark.table("healthcare_delta.prescriptions")
labevents_raw = spark.table("healthcare_delta.labevents")

print("✅ Delta Raw Layer loaded successfully")


# COMMAND ----------

from pyspark.sql.functions import *

def get_patient_count():
    return patients_raw.count()


def get_admission_summary():
    return (
        admissions_raw
        .groupBy("admission_type")
        .count()
        .orderBy(desc("count"))
    )


def get_icu_summary():
    return (
        icustays_raw
        .groupBy("first_careunit")
        .count()
        .orderBy(desc("count"))
    )


def get_average_icu_los():
    return (
        icustays_raw
        .select(
            round(avg("los"), 2).alias("average_los")
        )
        .first()["average_los"]
    )


def get_top_medications():
    return (
        prescriptions_raw
        .filter(col("drug").isNotNull())
        .groupBy("drug")
        .count()
        .orderBy(desc("count"))
        .limit(10)
    )


def get_top_diagnoses():
    return (
        diagnoses_raw
        .groupBy("icd9_code")
        .count()
        .orderBy(desc("count"))
        .limit(10)
    )

# COMMAND ----------

# MAGIC %pip install --upgrade typing-extensions databricks-openai databricks-sdk

# COMMAND ----------

# DBTITLE 1,Restart Python
dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Genie Agent connection
AGENT_ID = "01f19309f3811c578335b99a98c0a97a"

print("Genie Agent configured")

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks_openai import DatabricksOpenAI

AGENT_ID = "YOUR_ACTUAL_AGENT_ID"

w = WorkspaceClient()

host = (
    f"https://{w.config.host}"
    if not w.config.host.startswith("http")
    else w.config.host
)

client = DatabricksOpenAI(
    workspace_client=w
)

client.base_url = f"{host}/api/2.0/genie/agents/{AGENT_ID}"

print("✅ Genie Agent configured")

# COMMAND ----------

question = "What is the average ICU length of stay?"

# Reconfigure client with the correct agent ID from Cell 7
host = f"https://{w.config.host}" if not w.config.host.startswith("http") else w.config.host
client.base_url = f"{host}/api/2.0/genie/agents/{AGENT_ID}"

stream = client.responses.create(
    model="genie-agent",
    input=[
        {
            "type": "message",
            "role": "user",
            "content": [
                {
                    "type": "input_text",
                    "text": question
                }
            ]
        }
    ],
    stream=True
)

conversation_id = None

for event in stream:

    if event.type == "response.created":
        conversation_id = event.response.conversation_id
        print("Conversation:", conversation_id)

    elif event.type == "response.output_item.done":
        print("Output item:", event.item.type)

    elif event.type == "response.completed":
        print("✅ Genie response completed")
        print(event.response)

    elif event.type == "response.failed":
        print("❌ Genie failed:")
        print(event.response.error)

# COMMAND ----------

# MAGIC %md
# MAGIC # 🧪 GenAI Application Demonstration
# MAGIC
# MAGIC ## Question
# MAGIC
# MAGIC What is the average ICU length of stay?
# MAGIC
# MAGIC ## GenAI Processing
# MAGIC
# MAGIC The Genie Agent identified the `icustays` table as the relevant
# MAGIC healthcare dataset and generated an analytical SQL query.
# MAGIC
# MAGIC The query accessed:
# MAGIC
# MAGIC `workspace.healthcare_delta.icustays`
# MAGIC
# MAGIC The query calculated:
# MAGIC
# MAGIC - Average ICU length of stay
# MAGIC - Total ICU stays
# MAGIC - Minimum ICU length of stay
# MAGIC - Maximum ICU length of stay
# MAGIC
# MAGIC ## Result
# MAGIC
# MAGIC The dataset contains 136 ICU stays.
# MAGIC
# MAGIC The average ICU length of stay is approximately **4.45 days**.
# MAGIC
# MAGIC The minimum ICU stay is approximately **0.11 days**, while the maximum
# MAGIC ICU stay is approximately **35.41 days**.
# MAGIC
# MAGIC ## Key Observation
# MAGIC
# MAGIC The GenAI application successfully retrieved and analyzed data directly
# MAGIC from the Delta Lake Raw Layer using a natural-language question.

# COMMAND ----------

# MAGIC %md
# MAGIC # Conclusion
# MAGIC
# MAGIC The AI/GenAI healthcare analytics application was successfully developed
# MAGIC using Databricks Genie Agent.
# MAGIC
# MAGIC The application retrieves healthcare data directly from the Delta Lake
# MAGIC Raw Layer and allows users to interact with the data using natural-language
# MAGIC questions.
# MAGIC
# MAGIC The Genie Agent automatically identifies the relevant healthcare table,
# MAGIC generates analytical SQL queries, executes them on the Delta Lake data,
# MAGIC and presents the results in an understandable format.
# MAGIC
# MAGIC The application was successfully tested with multiple healthcare
# MAGIC analytics questions, including:
# MAGIC
# MAGIC - Patient analysis
# MAGIC - Hospital admission analysis
# MAGIC - ICU length-of-stay analysis
# MAGIC - Diagnosis analysis
# MAGIC - Medication analysis
# MAGIC
# MAGIC For example, the application identified an average ICU length of stay
# MAGIC of approximately 4.45 days across 136 ICU stays and identified the most
# MAGIC frequently recorded diagnosis codes from the diagnosis data.
# MAGIC
# MAGIC Overall, the project demonstrates an end-to-end healthcare data
# MAGIC engineering and GenAI analytics workflow:
# MAGIC
# MAGIC MIMIC Dataset
# MAGIC
# MAGIC → Data Ingestion
# MAGIC
# MAGIC → Data Engineering
# MAGIC
# MAGIC → Delta Lake Raw Layer
# MAGIC
# MAGIC → EDA & Visualization
# MAGIC
# MAGIC → GenAI Application
# MAGIC
# MAGIC → Natural-Language Healthcare Insights

# COMMAND ----------

