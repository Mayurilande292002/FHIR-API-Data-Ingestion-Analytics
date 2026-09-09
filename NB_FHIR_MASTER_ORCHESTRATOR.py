# Databricks notebook source
from datetime import datetime, timezone
import traceback

print("FHIR Master Orchestrator started")

# COMMAND ----------

PIPELINE_NAME = "PL_FHIR_Master"

INGESTION_NOTEBOOK = "/Workspace/FHIR_WORKSPACE/Notebooks/NB_FHIR_INGESTION"

BRONZE_SILVER_NOTEBOOK = "/Workspace/FHIR_WORKSPACE/Notebooks/NB_BRONZE_TO_SILVER"

SILVER_GOLD_NOTEBOOK = "/Workspace/FHIR_WORKSPACE/Notebooks/NB_SILVER_TO_GOLD"

NOTEBOOK_TIMEOUT = 3600

print("Pipeline:", PIPELINE_NAME)

print("Execution order:")
print("1. FHIR Ingestion")
print("2. Bronze to Silver")
print("3. Silver to Gold")

# COMMAND ----------

pipeline_start = datetime.now(timezone.utc)

print("=" * 80)
print("FHIR DATA PIPELINE STARTED")
print("=" * 80)

print("Pipeline Name:", PIPELINE_NAME)
print("Start Time:", pipeline_start)


# COMMAND ----------

print("=" * 80)
print("STEP 1 - FHIR API INGESTION")
print("=" * 80)

try:

    ingestion_result = dbutils.notebook.run(
        INGESTION_NOTEBOOK,
        NOTEBOOK_TIMEOUT
    )

    print(
        "FHIR ingestion completed successfully."
    )

    print(
        "Result:",
        ingestion_result
    )

except Exception as e:

    print(
        "FHIR ingestion failed."
    )

    print(
        str(e)
    )

    raise

# COMMAND ----------

print("=" * 80)
print("STEP 2 - BRONZE TO SILVER")
print("=" * 80)

try:

    silver_result = dbutils.notebook.run(
        BRONZE_SILVER_NOTEBOOK,
        NOTEBOOK_TIMEOUT
    )

    print("Bronze to Silver completed successfully.")
    print("Result:", silver_result)

except Exception as e:

    print("Bronze to Silver failed.")
    print("Error type:", type(e).__name__)
    print("Error details:", repr(e))

    raise

# COMMAND ----------

print("=" * 80)
print("STEP 3 - SILVER TO GOLD")
print("=" * 80)

try:

    gold_result = dbutils.notebook.run(
        SILVER_GOLD_NOTEBOOK,
        NOTEBOOK_TIMEOUT
    )

    print(
        "Silver to Gold completed successfully."
    )

    print(
        "Result:",
        gold_result
    )

except Exception as e:

    print(
        "Silver to Gold failed."
    )

    print(
        str(e)
    )

    raise

# COMMAND ----------

GOLD_TABLES = [
    "gold_patient",
    "gold_encounter",
    "gold_observation",
    "gold_condition",
    "gold_patient_summary",
    "gold_clinical_summary"
]

print("=" * 80)
print("FINAL GOLD VALIDATION")
print("=" * 80)

for table_name in GOLD_TABLES:

    if spark.catalog.tableExists(table_name):

        count = spark.table(
            table_name
        ).count()

        print(
            f"{table_name:<30}"
            f"{count:>10} records"
        )

    else:

        print(
            f"WARNING: {table_name} not found"
        )

# COMMAND ----------

pipeline_end = datetime.now(timezone.utc)

duration = (
    pipeline_end - pipeline_start
)

print("=" * 80)
print("FHIR DATA PIPELINE COMPLETED")
print("=" * 80)

print("Pipeline Name:", PIPELINE_NAME)
print("Start Time:", pipeline_start)
print("End Time:", pipeline_end)
print("Duration:", duration)

print("""
Pipeline Flow:

FHIR API
   ↓
NB_FHIR_Ingestion
   ↓
Raw + Bronze
   ↓
NB_Bronze_to_Silver
   ↓
Silver + SCD Type 2
   ↓
NB_Silver_to_Gold
   ↓
Gold Analytics
""")

# COMMAND ----------

dbutils.notebook.exit(
    "SUCCESS"
)