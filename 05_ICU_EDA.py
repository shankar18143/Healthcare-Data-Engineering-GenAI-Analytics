# Databricks notebook source
# MAGIC %md
# MAGIC # 🏥 ICU Analytics Dashboard
# MAGIC
# MAGIC ## Objective
# MAGIC
# MAGIC This notebook analyzes Intensive Care Unit (ICU) admissions using the ICUSTAYS dataset.
# MAGIC
# MAGIC The analysis focuses on ICU utilization, patient movement between care units, and ICU length of stay.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Business Questions
# MAGIC
# MAGIC 1. Which ICU admits the most patients?
# MAGIC 2. Which ICU discharges the most patients?
# MAGIC 3. What is the average ICU Length of Stay?
# MAGIC 4. Which ICU has the longest average stay?
# MAGIC 5. How often do patients transfer between ICU units?
# MAGIC
# MAGIC Dataset Used:
# MAGIC ICUSTAYS.csv

# COMMAND ----------

from pyspark.sql.functions import avg, max
import builtins

# Total ICU Stays
total_icu = icu_df.count()

# Average LOS
avg_los_value = icu_df.select(avg("los")).first()[0]
avg_los = builtins.round(avg_los_value, 2)

# Maximum LOS
max_los_value = icu_df.select(max("los")).first()[0]
max_los = builtins.round(max_los_value, 2)

# ICU Types
icu_types = icu_df.select("first_careunit").distinct().count()

print("="*60)
print("ICU KPI DASHBOARD")
print("="*60)

print(f"🏥 Total ICU Stays : {total_icu}")
print(f"📅 Average LOS     : {avg_los} Days")
print(f"📈 Maximum LOS     : {max_los} Days")
print(f"🏨 ICU Types       : {icu_types}")

# COMMAND ----------

# DBTITLE 1,first care unit
first_unit_df = (
    icu_df
    .groupBy("first_careunit")
    .count()
    .orderBy(desc("count"))
)

display(first_unit_df)

# COMMAND ----------

first_unit_pd = first_unit_df.toPandas()

# COMMAND ----------

import plotly.express as px

fig = px.bar(
    first_unit_pd,
    x="first_careunit",
    y="count",
    text="count",
    color="first_careunit",
    title="Patients by First Care Unit"
)

fig.update_layout(
    template="plotly_white",
    title_x=0.5,
    showlegend=False
)

fig.show()

# COMMAND ----------

# DBTITLE 1,last care unit
last_unit_df = (
    icu_df
    .groupBy("last_careunit")
    .count()
    .orderBy(desc("count"))
)

display(last_unit_df)

# COMMAND ----------

last_unit_pd = last_unit_df.toPandas()

# COMMAND ----------

fig = px.bar(
    last_unit_pd,
    x="last_careunit",
    y="count",
    text="count",
    color="last_careunit",
    title="Patients by Last Care Unit"
)

fig.update_layout(
    template="plotly_white",
    title_x=0.5,
    showlegend=False
)

fig.show()

# COMMAND ----------

los_pd = icu_df.select("los").toPandas()

# COMMAND ----------

fig = px.histogram(
    los_pd,
    x="los",
    nbins=20,
    title="ICU Length of Stay Distribution"
)

fig.update_layout(
    template="plotly_white",
    title_x=0.5,
    xaxis_title="Length of Stay (Days)",
    yaxis_title="Number of ICU Stays"
)

fig.show()

# COMMAND ----------

avg_los_df = (
    icu_df
    .groupBy("first_careunit")
    .agg(round(avg("los"),2).alias("Average_LOS"))
    .orderBy(desc("Average_LOS"))
)

display(avg_los_df)

# COMMAND ----------

avg_los_pd = avg_los_df.toPandas()

# COMMAND ----------

fig = px.bar(
    avg_los_pd,
    x="first_careunit",
    y="Average_LOS",
    text="Average_LOS",
    color="Average_LOS",
    color_continuous_scale="Viridis",
    title="Average ICU Length of Stay by Care Unit"
)

fig.update_layout(
    template="plotly_white",
    title_x=0.5
)

fig.show()

# COMMAND ----------

from pyspark.sql.functions import when

transfer_df = (
    icu_df
    .withColumn(
        "Transfer",
        when(
            col("first_careunit")==col("last_careunit"),
            "No Transfer"
        ).otherwise("Transferred")
    )
    .groupBy("Transfer")
    .count()
)

display(transfer_df)

# COMMAND ----------

transfer_pd = transfer_df.toPandas()

# COMMAND ----------

fig = px.pie(
    transfer_pd,
    names="Transfer",
    values="count",
    title="ICU Transfer Distribution"
)

fig.update_layout(
    template="plotly_white",
    title_x=0.5
)

fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC # Executive Insights
# MAGIC
# MAGIC ### 1. ICU Utilization
# MAGIC The ICU dataset contains **136 ICU stays** across **5 specialized ICU units**, providing a comprehensive view of critical care utilization.
# MAGIC
# MAGIC ### 2. Most Utilized ICU
# MAGIC The **Medical Intensive Care Unit (MICU)** received the highest number of admissions (**77 patients**), accounting for more than half of all ICU stays. This indicates that medical emergencies form the largest share of critical care admissions.
# MAGIC
# MAGIC ### 3. Length of Stay
# MAGIC The **average ICU Length of Stay (LOS)** was **4.45 days**, while the **maximum recorded stay** was **35.41 days**. This suggests that although most patients require short-term intensive care, a small number of patients need prolonged treatment.
# MAGIC
# MAGIC ### 4. ICU Performance
# MAGIC Among all ICU units, the **Coronary Care Unit (CCU)** recorded the highest average ICU stay (**5.75 days**), followed closely by the **Surgical ICU (SICU)** with **5.67 days**. These units may manage patients requiring longer monitoring and specialized treatment.
# MAGIC
# MAGIC ### 5. Patient Movement
# MAGIC The majority of patients (**128 out of 136, approximately 94%**) remained in the same ICU throughout their stay, while only **8 patients (approximately 6%)** were transferred between ICU units. This indicates stable patient management with relatively few inter-unit transfers.

# COMMAND ----------

# MAGIC %md
# MAGIC # Conclusion
# MAGIC
# MAGIC The ICU EDA provides valuable insights into critical care utilization within the hospital.
# MAGIC
# MAGIC Key findings show that:
# MAGIC - MICU handled the largest number of ICU admissions.
# MAGIC - The average ICU stay was 4.45 days.
# MAGIC - CCU recorded the longest average ICU stay.
# MAGIC - Most patients remained in the same ICU during treatment, indicating stable care pathways.
# MAGIC
# MAGIC These findings help healthcare administrators understand ICU workload, optimize resource allocation, and improve operational planning for critical care services.