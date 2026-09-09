# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *
from delta.tables import DeltaTable

import json
from datetime import datetime, timezone

print("Bronze to Silver started")

# COMMAND ----------

BRONZE_TABLES = {
    "Patient": "bronze_patient",
    "Encounter": "bronze_encounter",
    "Observation":"bronze_observation",
    "Condition":"bronze_condition"           
}

SILVER_TABLES = {
    "Patient": "silver_patient",
    "Encounter": "silver_encounter",
    "Observation":"silver_observation",
    "Condition":"silver_condition"
}

PROCESSING_TIMESTAMP = datetime.now(timezone.utc)

ACTIVE_FLAG = True
INACTIVE_FLAG = False

print("Bronze tables:")
for resource, tables in BRONZE_TABLES.items():
    print(f"{resource}:{table}")
print("\nSilver tables:")
for resource, tables in SILVER_TABLES.items():
    print(f"{resource}:{table}")

print("\nProcessing timestamp:", PROCESSING_TIMESTAMP)

# COMMAND ----------

print("=" * 70)
print("VALIDATING BRONZE TABLES")
print("=" * 70)

for resource, table_name in BRONZE_TABLES.items():
    if spark.catalog.tableExists(table_name):
        count = spark.table(table_name).count()

        print(
            f"{resource:<15} "
            f"{table_name:<25} "
            f"{count:>10} records"
        )
    else:
        raise Exception(
            f"Bronze table does not exist: {table_name}"
        )
print("\nBronze table validation completed.")

# COMMAND ----------

bronze_patient = spark.table(
    BRONZE_TABLES["Patient"]
)

bronze_encounter = spark.table(
    BRONZE_TABLES["Encounter"]
)

bronze_observation = spark.table(
    BRONZE_TABLES["Observation"]
)

bronze_condition = spark.table(
    BRONZE_TABLES["Condition"]
)

print("Bronze tables loaded successfully.")

print("Patient:", bronze_patient.count())
print("Encounter:", bronze_encounter.count())
print("Observation:", bronze_observation.count())
print("Condition:", bronze_condition.count())


# COMMAND ----------

display(
    bronze_patient.select(
        "resource_id",
        "resource_type",
        "resource_json",
        "extraction_timestamp",
        "source_page"
    ).limit(5)
)


# COMMAND ----------

def parse_resource_json(df):

    return (
        df
        .withColumn(
            "resource_map",
            F.from_json(
                F.col("resource_json"),
                "map<string,string>"
            )
        )
    )

print("Generic JSON parser function created.")

# COMMAND ----------

patient_df = (

    bronze_patient

    .withColumn(
        "patient_id",
        F.get_json_object(
            F.col("resource_json"),
            "$.id"
        )
    )

    .withColumn(
        "patient_gender",
        F.get_json_object(
            F.col("resource_json"),
            "$.gender"
        )
    )

    .withColumn(
        "patient_birth_date",
        F.get_json_object(
            F.col("resource_json"),
            "$.birthDate"
        )
    )

    .withColumn(
        "patient_active",
        F.get_json_object(
            F.col("resource_json"),
            "$.active"
        )
    )

    .withColumn(
        "patient_deceased",
        F.get_json_object(
            F.col("resource_json"),
            "$.deceasedBoolean"
        )
    )

    .withColumn(
        "resource_last_updated",
        F.get_json_object(
            F.col("resource_json"),
            "$.meta.lastUpdated"
        )
    )
)

display(
    patient_df.select(
        "patient_id",
        "patient_gender",
        "patient_birth_date",
        "patient_active",
        "resource_last_updated"
    ).limit(10)
)

# COMMAND ----------

patient_df = (

    patient_df

    .withColumn(
        "patient_family_name",
        F.get_json_object(
            F.col("resource_json"),
            "$.name[0].family"
        )
    )

    .withColumn(
        "patient_given_name",
        F.get_json_object(
            F.col("resource_json"),
            "$.name[0].given[0]"
        )
    )

    .withColumn(
        "patient_full_name",
        F.concat_ws(
            " ",
            F.col("patient_given_name"),
            F.col("patient_family_name")
        )
    )
)

display(
    patient_df.select(
        "patient_id",
        "patient_full_name",
        "patient_gender",
        "patient_birth_date"
    ).limit(10)
)

# COMMAND ----------

patient_df = (

    patient_df

    .withColumn(
        "patient_city",
        F.get_json_object(
            F.col("resource_json"),
            "$.address[0].city"
        )
    )

    .withColumn(
        "patient_state",
        F.get_json_object(
            F.col("resource_json"),
            "$.address[0].state"
        )
    )

    .withColumn(
        "patient_country",
        F.get_json_object(
            F.col("resource_json"),
            "$.address[0].country"
        )
    )
)

# COMMAND ----------

silver_patient_stage = (

    patient_df

    .select(
        "patient_id",
        "patient_full_name",
        "patient_family_name",
        "patient_given_name",
        "patient_gender",
        "patient_birth_date",
        "patient_active",
        "patient_deceased",
        "patient_city",
        "patient_state",
        "patient_country",
        "resource_last_updated",
        "extraction_timestamp",
        "api_url_or_params",
        "source_page",
        "ingestion_date"
    )

  
    .filter(
        F.col("patient_id").isNotNull()
    )

  
    .withColumn(
        "patient_full_name",
        F.trim(F.col("patient_full_name"))
    )

    .withColumn(
        "patient_gender",
        F.lower(
            F.trim(F.col("patient_gender"))
        )
    )
)

print(
    "Patient records after cleaning:",
    silver_patient_stage.count()
)

# COMMAND ----------

patient_window = Window.partitionBy(
    "patient_id"
).orderBy(
    F.col("extraction_timestamp").desc(),
    F.col("resource_last_updated").desc_nulls_last()
)

silver_patient_dedup = (

    silver_patient_stage

    .withColumn(
        "row_number",
        F.row_number().over(
            patient_window
        )
    )

    .filter(
        F.col("row_number") == 1
    )

    .drop("row_number")
)

print(
    "Patient records after deduplication:",
    silver_patient_dedup.count()
)

# COMMAND ----------

silver_patient_dedup = (

    silver_patient_dedup

    .withColumn(
        "record_hash",

        F.sha2(

            F.concat_ws(
                "||",

                F.coalesce(
                    F.col("patient_full_name"),
                    F.lit("")
                ),

                F.coalesce(
                    F.col("patient_gender"),
                    F.lit("")
                ),

                F.coalesce(
                    F.col("patient_birth_date"),
                    F.lit("")
                ),

                F.coalesce(
                    F.col("patient_active"),
                    F.lit("")
                ),

                F.coalesce(
                    F.col("patient_city"),
                    F.lit("")
                ),

                F.coalesce(
                    F.col("patient_state"),
                    F.lit("")
                ),

                F.coalesce(
                    F.col("patient_country"),
                    F.lit("")
                )
            ),

            256
        )
    )
)

# COMMAND ----------

silver_patient_stage_final = (

    silver_patient_dedup

    .withColumn(
        "effective_start_date",
        F.current_date()
    )

    .withColumn(
        "effective_end_date",
        F.lit("9999-12-31").cast("date")
    )

    .withColumn(
        "is_current",
        F.lit(True)
    )

    .withColumn(
        "created_timestamp",
        F.current_timestamp()
    )

    .withColumn(
        "updated_timestamp",
        F.current_timestamp()
    )
)

display(
    silver_patient_stage_final.limit(10)
)

# COMMAND ----------


encounter_df = (

    bronze_encounter

    .withColumn(
        "encounter_id",
        F.get_json_object(
            F.col("resource_json"),
            "$.id"
        )
    )

    .withColumn(
        "encounter_status",
        F.get_json_object(
            F.col("resource_json"),
            "$.status"
        )
    )

    .withColumn(
        "encounter_class",
        F.get_json_object(
            F.col("resource_json"),
            "$.class.code"
        )
    )

    .withColumn(
        "encounter_start",
        F.get_json_object(
            F.col("resource_json"),
            "$.period.start"
        )
    )

    .withColumn(
        "encounter_end",
        F.get_json_object(
            F.col("resource_json"),
            "$.period.end"
        )
    )

    .withColumn(
        "patient_reference",
        F.get_json_object(
            F.col("resource_json"),
            "$.subject.reference"
        )
    )

    .withColumn(
        "resource_last_updated",
        F.get_json_object(
            F.col("resource_json"),
            "$.meta.lastUpdated"
        )
)
)
display(
    encounter_df.select(
        "encounter_id",
        "encounter_status",
        "encounter_class",
        "encounter_start",
        "encounter_end",
        "patient_reference"
    ).limit(10)
)

# COMMAND ----------

silver_encounter_stage = (

    encounter_df

    .select(
        "encounter_id",
        "encounter_status",
        "encounter_class",
        "encounter_start",
        "encounter_end",
        "patient_reference",
        "resource_last_updated",
        "extraction_timestamp",
        "api_url_or_params",
        "source_page",
        "ingestion_date"
    )

    .filter(
        F.col("encounter_id").isNotNull()
    )

    .withColumn(
        "patient_id",
        F.regexp_replace(
            F.col("patient_reference"),
            "^Patient/",
            ""
        )
    )

    .withColumn(
        "encounter_status",
        F.lower(
            F.trim(F.col("encounter_status"))
        )
    )
)

# COMMAND ----------

encounter_window = Window.partitionBy(
    "encounter_id"
).orderBy(
    F.col("extraction_timestamp").desc(),
    F.col("resource_last_updated").desc_nulls_last()
)

silver_encounter_dedup = (

    silver_encounter_stage

    .withColumn(
        "row_number",
        F.row_number().over(
            encounter_window
        )
    )

    .filter(
        F.col("row_number") == 1
    )

    .drop("row_number")
)

# COMMAND ----------

silver_encounter_stage_final = (

    silver_encounter_dedup

    .withColumn(
        "record_hash",

        F.sha2(

            F.concat_ws(
                "||",

                F.coalesce(
                    F.col("encounter_status"),
                    F.lit("")
                ),

                F.coalesce(
                    F.col("encounter_class"),
                    F.lit("")
                ),

                F.coalesce(
                    F.col("encounter_start"),
                    F.lit("")
                ),

                F.coalesce(
                    F.col("encounter_end"),
                    F.lit("")
                ),

                F.coalesce(
                    F.col("patient_id"),
                    F.lit("")
                )
            ),

            256
        )
    )

    .withColumn(
        "effective_start_date",
        F.current_date()
    )

    .withColumn(
        "effective_end_date",
        F.lit("9999-12-31").cast("date")
    )

    .withColumn(
        "is_current",
        F.lit(True)
    )

    .withColumn(
        "created_timestamp",
        F.current_timestamp()
    )

    .withColumn(
        "updated_timestamp",
        F.current_timestamp()
    )
)

# COMMAND ----------

observation_df = (

    bronze_observation

    .withColumn(
        "observation_id",
        F.get_json_object(
            F.col("resource_json"),
            "$.id"
        )
    )

    .withColumn(
        "observation_status",
        F.get_json_object(
            F.col("resource_json"),
            "$.status"
        )
    )

    .withColumn(
        "observation_code",
        F.get_json_object(
            F.col("resource_json"),
            "$.code.coding[0].code"
        )
    )

    .withColumn(
        "observation_display",
        F.get_json_object(
            F.col("resource_json"),
            "$.code.coding[0].display"
        )
    )

    .withColumn(
        "observation_value",
        F.get_json_object(
            F.col("resource_json"),
            "$.valueQuantity.value"
        )
    )

    .withColumn(
        "observation_unit",
        F.get_json_object(
            F.col("resource_json"),
            "$.valueQuantity.unit"
        )
    )

    .withColumn(
        "observation_effective",
        F.get_json_object(
            F.col("resource_json"),
            "$.effectiveDateTime"
        )
    )

    .withColumn(
        "patient_reference",
        F.get_json_object(
            F.col("resource_json"),
            "$.subject.reference"
        )
    )

    .withColumn(
        "resource_last_updated",
        F.get_json_object(
            F.col("resource_json"),
            "$.meta.lastUpdated"
        )
    )
)

display(
    observation_df.select(
        "observation_id",
        "observation_status",
        "observation_code",
        "observation_display",
        "observation_value",
        "observation_unit",
        "patient_reference"
    ).limit(10)
)


# COMMAND ----------

silver_observation_stage = (

    observation_df

    .select(
        "observation_id",
        "observation_status",
        "observation_code",
        "observation_display",
        "observation_value",
        "observation_unit",
        "observation_effective",
        "patient_reference",
        "resource_last_updated",
        "extraction_timestamp",
        "api_url_or_params",
        "source_page",
        "ingestion_date"
    )

    .filter(
        F.col("observation_id").isNotNull()
    )

    .withColumn(
        "patient_id",
        F.regexp_replace(
            F.col("patient_reference"),
            "^Patient/",
            ""
        )
    )

    .withColumn(
        "observation_status",
        F.lower(
            F.trim(
                F.col("observation_status")
            )
        )
    )
)

# COMMAND ----------

observation_window = Window.partitionBy(
    "observation_id"
).orderBy(
    F.col("extraction_timestamp").desc(),
    F.col("resource_last_updated").desc_nulls_last()
)

silver_observation_dedup = (

    silver_observation_stage

    .withColumn(
        "row_number",
        F.row_number().over(
            observation_window
        )
    )

    .filter(
        F.col("row_number") == 1
    )

    .drop("row_number")
)

# COMMAND ----------

silver_observation_stage_final = (

    silver_observation_dedup

    .withColumn(
        "record_hash",

        F.sha2(

            F.concat_ws(
                "||",

                F.coalesce(
                    F.col("observation_status"),
                    F.lit("")
                ),

                F.coalesce(
                    F.col("observation_code"),
                    F.lit("")
                ),

                F.coalesce(
                    F.col("observation_display"),
                    F.lit("")
                ),

                F.coalesce(
                    F.col("observation_value"),
                    F.lit("")
                ),

                F.coalesce(
                    F.col("observation_unit"),
                    F.lit("")
                ),

                F.coalesce(
                    F.col("observation_effective"),
                    F.lit("")
                ),

                F.coalesce(
                    F.col("patient_id"),
                    F.lit("")
                )
            ),

            256
        )
    )

    .withColumn(
        "effective_start_date",
        F.current_date()
    )

    .withColumn(
        "effective_end_date",
        F.lit("9999-12-31").cast("date")
    )

    .withColumn(
        "is_current",
        F.lit(True)
    )

    .withColumn(
        "created_timestamp",
        F.current_timestamp()
    )

    .withColumn(
        "updated_timestamp",
        F.current_timestamp()
    )
)

# COMMAND ----------

condition_df = (

    bronze_condition

    .withColumn(
        "condition_id",
        F.get_json_object(
            F.col("resource_json"),
            "$.id"
        )
    )

    .withColumn(
        "condition_clinical_status",
        F.get_json_object(
            F.col("resource_json"),
            "$.clinicalStatus.coding[0].code"
        )
    )

    .withColumn(
        "condition_verification_status",
        F.get_json_object(
            F.col("resource_json"),
            "$.verificationStatus.coding[0].code"
        )
    )

    .withColumn(
        "condition_code",
        F.get_json_object(
            F.col("resource_json"),
            "$.code.coding[0].code"
        )
    )

    .withColumn(
        "condition_display",
        F.get_json_object(
            F.col("resource_json"),
            "$.code.coding[0].display"
        )
    )

    .withColumn(
        "onset_date",
        F.get_json_object(
            F.col("resource_json"),
            "$.onsetDateTime"
        )
    )

    .withColumn(
        "patient_reference",
        F.get_json_object(
            F.col("resource_json"),
            "$.subject.reference"
        )
    )

    .withColumn(
        "resource_last_updated",
        F.get_json_object(
            F.col("resource_json"),
            "$.meta.lastUpdated"
        )
    )
)

display(
    condition_df.select(
        "condition_id",
        "condition_clinical_status",
        "condition_verification_status",
        "condition_code",
        "condition_display",
        "onset_date",
        "patient_reference"
    ).limit(10)
)

# COMMAND ----------

silver_condition_stage = (

    condition_df

    .select(
        "condition_id",
        "condition_clinical_status",
        "condition_verification_status",
        "condition_code",
        "condition_display",
        "onset_date",
        "patient_reference",
        "resource_last_updated",
        "extraction_timestamp",
        "api_url_or_params",
        "source_page",
        "ingestion_date"
    )

    .filter(
        F.col("condition_id").isNotNull()
    )

    .withColumn(
        "patient_id",
        F.regexp_replace(
            F.col("patient_reference"),
            "^Patient/",
            ""
        )
    )

    .withColumn(
        "condition_clinical_status",
        F.lower(
            F.trim(
                F.col("condition_clinical_status")
            )
        )
    )
)

# COMMAND ----------

condition_window = Window.partitionBy(
    "condition_id"
).orderBy(
    F.col("extraction_timestamp").desc(),
    F.col("resource_last_updated").desc_nulls_last()
)

silver_condition_dedup = (

    silver_condition_stage

    .withColumn(
        "row_number",
        F.row_number().over(
            condition_window
        )
    )

    .filter(
        F.col("row_number") == 1
    )

    .drop("row_number")
)

# COMMAND ----------

silver_condition_stage_final = (

    silver_condition_dedup

    .withColumn(
        "record_hash",

        F.sha2(

            F.concat_ws(
                "||",

                F.coalesce(
                    F.col("condition_clinical_status"),
                    F.lit("")
                ),

                F.coalesce(
                    F.col("condition_verification_status"),
                    F.lit("")
                ),

                F.coalesce(
                    F.col("condition_code"),
                    F.lit("")
                ),

                F.coalesce(
                    F.col("condition_display"),
                    F.lit("")
                ),

                F.coalesce(
                    F.col("onset_date"),
                    F.lit("")
                ),

                F.coalesce(
                    F.col("patient_id"),
                    F.lit("")
                )
            ),

            256
        )
    )

    .withColumn(
        "effective_start_date",
        F.current_date()
    )

    .withColumn(
        "effective_end_date",
        F.lit("9999-12-31").cast("date")
    )

    .withColumn(
        "is_current",
        F.lit(True)
    )

    .withColumn(
        "created_timestamp",
        F.current_timestamp()
    )

    .withColumn(
        "updated_timestamp",
        F.current_timestamp()
    )
)

# COMMAND ----------

def apply_scd_type_2(
    source_df,
    target_table,
    business_key
):

    print("=" * 70)
    print(f"PROCESSING SCD TYPE 2: {target_table}")
    print("=" * 70)


    if not spark.catalog.tableExists(
        target_table
    ):

        print(
            f"Target table does not exist. "
            f"Creating {target_table}"
        )

        (
            source_df.write
            .format("delta")
            .mode("overwrite")
            .saveAsTable(target_table)
        )

        print(
            f"Created {target_table}"
        )

        return

  
    target_df = spark.table(
        target_table
    )

    current_target = (

        target_df

        .filter(
            F.col("is_current") == True
        )

    )

    source_alias = source_df.alias("source")

    target_alias = current_target.alias(
        "target"
    )

    join_condition = (

        F.col(
            f"source.{business_key}"
        )
        ==
        F.col(
            f"target.{business_key}"
        )

    )

    comparison_df = (

        source_alias

        .join(
            target_alias,
            join_condition,
            "left"
        )

        .select(
            F.col(
                f"source.{business_key}"
            ).alias(
                business_key
            ),

            F.col(
                "source.record_hash"
            ).alias(
                "source_hash"
            ),

            F.col(
                "target.record_hash"
            ).alias(
                "target_hash"
            )
        )
    )

    changed_or_new = (

        comparison_df

        .filter(
            F.col("target_hash").isNull()
            |
            (
                F.col("source_hash")
                !=
                F.col("target_hash")
            )
        )

    )

    changed_or_new_count = (
        changed_or_new.count()
    )

    print(
        f"New/changed records: "
        f"{changed_or_new_count}"
    )

    if changed_or_new_count == 0:

        print(
            f"No changes detected for "
            f"{target_table}"
        )

        return

    
    changed_existing = (

        comparison_df

        .filter(
            F.col("target_hash").isNotNull()
            &
            (
                F.col("source_hash")
                !=
                F.col("target_hash")
            )
        )

        .select(
            business_key
        )

        .distinct()
    )

    changed_count = (
        changed_existing.count()
    )

    print(
        f"Existing records changed: "
        f"{changed_count}"
    )

    
    if changed_count > 0:

        delta_target = DeltaTable.forName(
            spark,
            target_table
        )

        (
            delta_target.alias("target")
            .merge(
                changed_existing.alias("changes"),
                f"""
                target.{business_key} =
                changes.{business_key}
                AND target.is_current = true
                """
            )
            .whenMatchedUpdate(
                set={
                    "is_current": "false",
                    "effective_end_date":
                        "current_date()",
                    "updated_timestamp":
                        "current_timestamp()"
                }
            )
            .execute()
        )

        print(
            "Old versions expired."
        )

    
    records_to_insert = (

        source_df.alias("source")

        .join(
            changed_or_new.alias("changes"),

            F.col(
                f"source.{business_key}"
            )
            ==
            F.col(
                f"changes.{business_key}"
            ),

            "inner"
        )

        .select(
            "source.*"
        )
    )

    (
        records_to_insert.write
        .format("delta")
        .mode("append")
        .saveAsTable(target_table)
    )

    print(
        f"Inserted {records_to_insert.count()} "
        f"new/current versions."
    )

    print(
        f"SCD Type 2 completed for {target_table}"
    )

# COMMAND ----------

apply_scd_type_2(
    source_df=silver_patient_stage_final,
    target_table=SILVER_TABLES["Patient"],
    business_key="patient_id"
)

# COMMAND ----------

apply_scd_type_2(
    source_df=silver_encounter_stage_final,
    target_table=SILVER_TABLES["Encounter"],
    business_key="encounter_id"
)

# COMMAND ----------

apply_scd_type_2(
    source_df=silver_observation_stage_final,
    target_table=SILVER_TABLES["Observation"],
    business_key="observation_id"
)

# COMMAND ----------

apply_scd_type_2(
    source_df=silver_condition_stage_final,
    target_table=SILVER_TABLES["Condition"],
    business_key="condition_id"
)

# COMMAND ----------

print("=" * 80)
print("SILVER TABLE VALIDATION")
print("=" * 80)

for resource, table_name in SILVER_TABLES.items():

    df = spark.table(table_name)

    print(
        f"{resource:<20} "
        f"{df.count():>10} total records"
    )

    current_count = (
        df
        .filter(
            F.col("is_current") == True
        )
        .count()
    )

    historical_count = (
        df
        .filter(
            F.col("is_current") == False
        )
        .count()
    )

    print(
        f"{'Current records':<20} "
        f"{current_count:>10}"
    )

    print(
        f"{'Historical records':<20} "
        f"{historical_count:>10}"
    )

    print("-" * 80)

# COMMAND ----------

display(
    spark.table(
        "silver_patient"
    )
    .select(
        "patient_id",
        "patient_full_name",
        "patient_gender",
        "patient_birth_date",
        "record_hash",
        "effective_start_date",
        "effective_end_date",
        "is_current"
    )
    .orderBy(
        F.col("patient_id")
    )
    .limit(20)
)

# COMMAND ----------

display(
    spark.table(
        "silver_encounter"
    )
    .select(
        "encounter_id",
        "patient_id",
        "encounter_status",
        "encounter_class",
        "encounter_start",
        "encounter_end",
        "effective_start_date",
        "effective_end_date",
        "is_current"
    )
    .limit(20)
)

# COMMAND ----------

display(
    spark.table(
        "silver_observation"
    )
    .select(
        "observation_id",
        "patient_id",
        "observation_code",
        "observation_display",
        "observation_value",
        "observation_unit",
        "observation_effective",
        "is_current"
    )
    .limit(20)
)

# COMMAND ----------

display(
    spark.table(
        "silver_condition"
    )
    .select(
        "condition_id",
        "patient_id",
        "condition_code",
        "condition_display",
        "condition_clinical_status",
        "condition_verification_status",
        "onset_date",
        "effective_start_date",
        "effective_end_date",
        "is_current"
    )
    .limit(20)
)

# COMMAND ----------

duplicate_checks = {
    "Patient": ("silver_patient", "patient_id"),
    "Encounter": ("silver_encounter", "encounter_id"),
    "Observation": ("silver_observation", "observation_id"),
    "Condition": ("silver_condition", "condition_id")
}

for resource, (table_name, key_column) in duplicate_checks.items():

    duplicates = (

        spark.table(table_name)

        .filter(
            F.col("is_current") == True
        )

        .groupBy(key_column)

        .count()

        .filter(
            F.col("count") > 1
        )
    )

    duplicate_count = duplicates.count()

    print(
        f"{resource:<15} "
        f"duplicate current IDs: "
        f"{duplicate_count}"
    )

# COMMAND ----------

print("=" * 80)
print("SILVER DATA QUALITY SUMMARY")
print("=" * 80)

for resource, (table_name, key_column) in duplicate_checks.items():

    df = spark.table(table_name)

    total_records = df.count()

    current_records = (
        df.filter(
            F.col("is_current") == True
        ).count()
    )

    historical_records = (
        df.filter(
            F.col("is_current") == False
        ).count()
    )

    null_key_records = (
        df.filter(
            F.col(key_column).isNull()
        ).count()
    )

    print(f"\nResource: {resource}")
    print(f"Total records       : {total_records}")
    print(f"Current records     : {current_records}")
    print(f"Historical records  : {historical_records}")
    print(f"Null business keys  : {null_key_records}")

# COMMAND ----------

print("""
FHIR SILVER TABLE RELATIONSHIPS

================================

silver_patient
      |
      | patient_id
      |
      +--------------------------+
      |                          |
      v                          v
silver_encounter        silver_observation
      |                          |
      | patient_id               | patient_id
      |                          |
      +-------------+------------+
                    |
                    v
             silver_condition
""")

# COMMAND ----------

print("=" * 80)
print("BRONZE TO SILVER COMPLETED")
print("=" * 80)

print("""
Resources processed:
    1. Patient
    2. Encounter
    3. Observation
    4. Condition

Transformations:
    ✓ JSON parsing
    ✓ Field extraction
    ✓ Data cleansing
    ✓ Null business-key filtering
    ✓ Deduplication
    ✓ Record hashing
    ✓ SCD Type 2 versioning
    ✓ Data quality validation

Silver tables:
    ✓ silver_patient
    ✓ silver_encounter
    ✓ silver_observation
    ✓ silver_condition

Next step:
    Silver → Gold analytical model
""")

print("=" * 80)