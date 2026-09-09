# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from datetime import datetime, timezone

print("Silver to Gold notebook started")

# COMMAND ----------

SILVER_TABLES = {
    "Patient": "silver_patient",
    "Encounter": "silver_encounter",
    "Observation": "silver_observation",
    "Condition": "silver_condition"
}

GOLD_TABLES = {
    "Patient": "gold_patient",
    "Encounter": "gold_encounter",
    "Observation": "gold_observation",
    "Condition": "gold_condition",
    "PatientSummary": "gold_patient_summary",
    "ClinicalSummary": "gold_clinical_summary"
}

PROCESSING_TIMESTAMP = datetime.now(timezone.utc)

print("Silver tables:")
for resource, table in SILVER_TABLES.items():
    print(f"{resource}: {table}")

print("\nGold tables:")
for resource, table in GOLD_TABLES.items():
    print(f"{resource}: {table}")

print("\nProcessing timestamp:", PROCESSING_TIMESTAMP)

# COMMAND ----------

print("=" * 70)
print("VALIDATING SILVER TABLES")
print("=" * 70)

for resource, table_name in SILVER_TABLES.items():

    if spark.catalog.tableExists(table_name):

        count = spark.table(table_name).count()

        print(
            f"{resource:<15}"
            f"{table_name:<30}"
            f"{count:>10} records"
        )

    else:

        raise Exception(
            f"Silver table does not exist: {table_name}"
        )

print("\nSilver validation completed.")

# COMMAND ----------

silver_patient = (
    spark.table(SILVER_TABLES["Patient"])
    .filter(F.col("is_current") == True)
)

silver_encounter = (
    spark.table(SILVER_TABLES["Encounter"])
    .filter(F.col("is_current") == True)
)

silver_observation = (
    spark.table(SILVER_TABLES["Observation"])
    .filter(F.col("is_current") == True)
)

silver_condition = (
    spark.table(SILVER_TABLES["Condition"])
    .filter(F.col("is_current") == True)
)

print("Current Silver records:")

print("Patients      :", silver_patient.count())
print("Encounters    :", silver_encounter.count())
print("Observations  :", silver_observation.count())
print("Conditions    :", silver_condition.count())

# COMMAND ----------

gold_patient = (

    silver_patient

    .select(
        F.col("patient_id"),

        F.col("patient_full_name"),

        F.col("patient_gender"),

        F.to_date(
            F.col("patient_birth_date")
        ).alias(
            "birth_date"
        ),

        F.col("patient_active"),

        F.col("patient_deceased"),

        F.col("patient_city"),

        F.col("patient_state"),

        F.col("patient_country"),

        F.col("effective_start_date"),

        F.col("effective_end_date"),

        F.col("is_current"),

        F.col("created_timestamp"),

        F.col("updated_timestamp")
    )

    .withColumn(
        "patient_age",

        F.when(
            F.col("birth_date").isNotNull(),

            F.floor(
                F.months_between(
                    F.current_date(),
                    F.col("birth_date")
                ) / 12
            )
        )
    )
)

display(
    gold_patient.limit(20)
)

# COMMAND ----------

(
    gold_patient.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(
        GOLD_TABLES["Patient"]
    )
)

print(
    f"Created {GOLD_TABLES['Patient']}"
)

# COMMAND ----------

gold_encounter = (

    silver_encounter

    .select(

        F.col("encounter_id"),

        F.col("patient_id"),

        F.col("encounter_status"),

        F.col("encounter_class"),

        F.to_timestamp(
            F.col("encounter_start")
        ).alias(
            "encounter_start"
        ),

        F.to_timestamp(
            F.col("encounter_end")
        ).alias(
            "encounter_end"
        ),

        F.col("effective_start_date"),

        F.col("effective_end_date"),

        F.col("is_current")
    )

    .withColumn(
        "encounter_date",
        F.to_date(
            F.col("encounter_start")
        )
    )

    .withColumn(
        "encounter_duration_minutes",

        F.when(

            F.col("encounter_start").isNotNull()
            &
            F.col("encounter_end").isNotNull(),

            (
                F.unix_timestamp("encounter_end")
                -
                F.unix_timestamp("encounter_start")
            ) / 60
        )
    )
)

display(
    gold_encounter.limit(20)
)

# COMMAND ----------

(
    gold_encounter.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(
        GOLD_TABLES["Encounter"]
    )
)

print(
    f"Created {GOLD_TABLES['Encounter']}"
)

# COMMAND ----------

gold_observation = (

    silver_observation

    .select(

        F.col("observation_id"),

        F.col("patient_id"),

        F.col("observation_status"),

        F.col("observation_code"),

        F.col("observation_display"),

        F.col("observation_value"),

        F.col("observation_unit"),

        F.to_timestamp(
            F.col("observation_effective")
        ).alias(
            "observation_effective"
        ),

        F.col("effective_start_date"),

        F.col("effective_end_date"),

        F.col("is_current")
    )

    .withColumn(
        "observation_date",

        F.to_date(
            F.col("observation_effective")
        )
    )

    .withColumn(
        "observation_value_numeric",

        F.col("observation_value").cast(
            "double"
        )
    )
)

display(
    gold_observation.limit(20)
)

# COMMAND ----------

(
    gold_observation.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(
        GOLD_TABLES["Observation"]
    )
)

print(
    f"Created {GOLD_TABLES['Observation']}"
)

# COMMAND ----------

gold_condition = (

    silver_condition

    .select(

        F.col("condition_id"),

        F.col("patient_id"),

        F.col("condition_clinical_status"),

        F.col("condition_verification_status"),

        F.col("condition_code"),

        F.col("condition_display"),

        F.to_date(
            F.col("onset_date")
        ).alias(
            "onset_date"
        ),

        F.col("effective_start_date"),

        F.col("effective_end_date"),

        F.col("is_current")
    )
)

display(
    gold_condition.limit(20)
)

# COMMAND ----------

(
    gold_condition.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(
        GOLD_TABLES["Condition"]
    )
)

print(
    f"Created {GOLD_TABLES['Condition']}"
)

# COMMAND ----------

encounter_summary = (

    gold_encounter

    .groupBy("patient_id")

    .agg(

        F.count(
            "encounter_id"
        ).alias(
            "total_encounters"
        ),

        F.min(
            "encounter_date"
        ).alias(
            "first_encounter_date"
        ),

        F.max(
            "encounter_date"
        ).alias(
            "last_encounter_date"
        )
    )
)

observation_summary = (

    gold_observation

    .groupBy("patient_id")

    .agg(

        F.count(
            "observation_id"
        ).alias(
            "total_observations"
        )
    )
)

condition_summary = (

    gold_condition

    .groupBy("patient_id")

    .agg(

        F.count(
            "condition_id"
        ).alias(
            "total_conditions"
        )
    )
)

gold_patient_summary = (

    gold_patient.alias("p")

    .join(
        encounter_summary.alias("e"),
        F.col("p.patient_id")
        ==
        F.col("e.patient_id"),
        "left"
    )

    .join(
        observation_summary.alias("o"),
        F.col("p.patient_id")
        ==
        F.col("o.patient_id"),
        "left"
    )

    .join(
        condition_summary.alias("c"),
        F.col("p.patient_id")
        ==
        F.col("c.patient_id"),
        "left"
    )

    .select(

        F.col("p.patient_id"),

        F.col("p.patient_full_name"),

        F.col("p.patient_gender"),

        F.col("p.birth_date"),

        F.col("p.patient_age"),

        F.col("p.patient_city"),

        F.col("p.patient_state"),

        F.col("p.patient_country"),

        F.coalesce(
            F.col("e.total_encounters"),
            F.lit(0)
        ).alias(
            "total_encounters"
        ),

        F.coalesce(
            F.col("o.total_observations"),
            F.lit(0)
        ).alias(
            "total_observations"
        ),

        F.coalesce(
            F.col("c.total_conditions"),
            F.lit(0)
        ).alias(
            "total_conditions"
        ),

        F.col("e.first_encounter_date"),

        F.col("e.last_encounter_date")
    )
)

display(
    gold_patient_summary.limit(20)
)

# COMMAND ----------

(
    gold_patient_summary.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(
        GOLD_TABLES["PatientSummary"]
    )
)

print(
    f"Created {GOLD_TABLES['PatientSummary']}"
)

# COMMAND ----------

clinical_summary = (

    gold_patient

    .select(
        "patient_id",
        "patient_gender",
        "patient_age",
        "patient_state"
    )

    .join(
        gold_encounter
        .groupBy("patient_id")
        .agg(
            F.count("*").alias(
                "encounter_count"
            )
        ),
        "patient_id",
        "left"
    )

    .join(
        gold_observation
        .groupBy("patient_id")
        .agg(
            F.count("*").alias(
                "observation_count"
            )
        ),
        "patient_id",
        "left"
    )

    .join(
        gold_condition
        .groupBy("patient_id")
        .agg(
            F.count("*").alias(
                "condition_count"
            )
        ),
        "patient_id",
        "left"
    )

    .fillna(
        0,
        subset=[
            "encounter_count",
            "observation_count",
            "condition_count"
        ]
    )
)

display(
    clinical_summary.limit(20)
)

# COMMAND ----------

(
    clinical_summary.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(
        GOLD_TABLES["ClinicalSummary"]
    )
)

print(
    f"Created {GOLD_TABLES['ClinicalSummary']}"
)

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE VIEW vw_patient_analytics AS
SELECT
    *
FROM gold_patient_summary
""")

spark.sql("""
CREATE OR REPLACE VIEW vw_encounter_analytics AS
SELECT
    *
FROM gold_encounter
""")

spark.sql("""
CREATE OR REPLACE VIEW vw_observation_analytics AS
SELECT
    *
FROM gold_observation
""")

spark.sql("""
CREATE OR REPLACE VIEW vw_condition_analytics AS
SELECT
    *
FROM gold_condition
""")

print("Gold analytical views created.")

# COMMAND ----------

print("=" * 80)
print("GOLD TABLE VALIDATION")
print("=" * 80)

for resource, table_name in GOLD_TABLES.items():

    if spark.catalog.tableExists(table_name):

        count = spark.table(
            table_name
        ).count()

        print(
            f"{resource:<20}"
            f"{table_name:<30}"
            f"{count:>10} records"
        )

    else:

        print(
            f"{table_name} does not exist"
        )

# COMMAND ----------

display(
    spark.sql("""
        SELECT
            patient_id,
            patient_full_name,
            patient_gender,
            patient_age,
            total_encounters,
            total_observations,
            total_conditions,
            first_encounter_date,
            last_encounter_date
        FROM gold_patient_summary
        ORDER BY total_encounters DESC
        LIMIT 20
    """)
)

# COMMAND ----------

display(
    spark.sql("""
        SELECT
            encounter_status,
            encounter_class,
            COUNT(*) AS encounter_count
        FROM gold_encounter
        GROUP BY
            encounter_status,
            encounter_class
        ORDER BY encounter_count DESC
    """)
)

# COMMAND ----------

display(
    spark.sql("""
        SELECT
            observation_code,
            observation_display,
            COUNT(*) AS observation_count
        FROM gold_observation
        GROUP BY
            observation_code,
            observation_display
        ORDER BY observation_count DESC
        LIMIT 20
    """)
)

# COMMAND ----------

display(
    spark.sql("""
        SELECT
            condition_code,
            condition_display,
            condition_clinical_status,
            COUNT(*) AS condition_count
        FROM gold_condition
        GROUP BY
            condition_code,
            condition_display,
            condition_clinical_status
        ORDER BY condition_count DESC
        LIMIT 20
    """)
)

# COMMAND ----------


print("=" * 80)
print("GOLD DATA QUALITY")
print("=" * 80)

checks = [

    (
        "Gold Patient",
        "gold_patient",
        "patient_id"
    ),

    (
        "Gold Encounter",
        "gold_encounter",
        "encounter_id"
    ),

    (
        "Gold Observation",
        "gold_observation",
        "observation_id"
    ),

    (
        "Gold Condition",
        "gold_condition",
        "condition_id"
    )
]

for name, table_name, key_column in checks:

    df = spark.table(table_name)

    total = df.count()

    null_keys = (
        df
        .filter(
            F.col(key_column).isNull()
        )
        .count()
    )

    duplicates = (
        df
        .groupBy(key_column)
        .count()
        .filter(
            F.col("count") > 1
        )
        .count()
    )

    print(f"\n{name}")
    print(f"Total records       : {total}")
    print(f"Null IDs            : {null_keys}")
    print(f"Duplicate IDs       : {duplicates}")

# COMMAND ----------

print("=" * 80)
print("SILVER TO GOLD COMPLETED")
print("=" * 80)

print("""
Gold analytical layer created:

    gold_patient
    gold_encounter
    gold_observation
    gold_condition

Analytical summary tables:

    gold_patient_summary
    gold_clinical_summary

Analytical views:

    vw_patient_analytics
    vw_encounter_analytics
    vw_observation_analytics
    vw_condition_analytics

Transformations completed:

    ✓ Current SCD records selected
    ✓ Patient dimension created
    ✓ Encounter fact created
    ✓ Observation fact created
    ✓ Condition fact created
    ✓ Patient-level analytics created
    ✓ Clinical summary created
    ✓ Data quality checks completed
    ✓ Gold views created

Next step:
    Databricks Workflow / Orchestration
""")

print("=" * 80)

# COMMAND ----------

for table_name in [
    "gold_patient",
    "gold_encounter",
    "gold_observation",
    "gold_condition",
    "gold_patient_summary",
    "gold_clinical_summary"
]:
    count = spark.table(table_name).count()
    print(f"{table_name}: {count} records")