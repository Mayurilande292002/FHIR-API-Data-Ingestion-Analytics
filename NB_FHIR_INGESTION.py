# Databricks notebook source
import requests
import json
import time
import traceback
from datetime import datetime, timedelta, timezone
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    TimestampType

)
print("FHIR ingestion notebook started")

# COMMAND ----------

BASE_URL = "https://hapi.fhir.org/baseR4"

RESOURCES = [
    "Patient",
    "Encounter",
    "Observation",
    "Condition"
]

PAGE_SIZE = 100

LOOKBACK_DAYS = 3

REQUEST_TIMEOUT = 60

MAX_RETRIES = 3

RETRY_WAIT_SECONDS = 5

RAW_BASE_PATH = "/Workspace/FHIR_WORKSPACE/Workspace/Raw"

BRONZE_TABLE_PREFIX = "bronze_"

EXTRACTION_TIMESTAMP = datetime.now(timezone.utc)

print("Base URL:", BASE_URL)
print("Reaources:", RESOURCES)
print("Lookback days:", LOOKBACK_DAYS)
print("Page size:", PAGE_SIZE)
print("Extraction timestamp:", EXTRACTION_TIMESTAMP)

# COMMAND ----------

# DBTITLE 1,e
end_date = datetime.now(timezone.utc).date()
start_date = end_date - timedelta(days= LOOKBACK_DAYS)

print(f"Start Date:{start_date}")
print(f"End Date: {end_date}")

# COMMAND ----------

import os
for resource in RESOURCES:
    resource_path = f"{RAW_BASE_PATH}/{resource}"
    os.makedirs(resource_path, exist_ok=True)
    print(f"Directory ready: {resource_path}")

print("All raw directories are ready.")

# COMMAND ----------

def make_api_request(url, params=None):
    """
    Make API GET request with retry logic.
    """

    for attempt in range(1, MAX_RETRIES + 1):

        try:
            response = requests.get(
                url,
                params=params,
                timeout=REQUEST_TIMEOUT,
                headers={
                    "Accept": "application/fhir+json"
                }
            )

            response.raise_for_status()

            return response

        except requests.exceptions.RequestException as e:

            print(
                f"API request failed "
                f"(Attempt {attempt}/{MAX_RETRIES}): {e}"
            )

            if attempt < MAX_RETRIES:
                time.sleep(RETRY_WAIT_SECONDS)
            else:
                raise

# COMMAND ----------

def get_next_page_url(bundle):
    links = bundle.get("link", [])
    for link in links:
        if link.get('relation')=="next":
            return link.get("url")
    return None

# COMMAND ----------

def fetch_fhir_resources(
    resource,
    start_date,
    end_date
):
    initial_url = f"{BASE_URL}/{resource}"

    params = [
        (
            "_count",
            PAGE_SIZE
        ),
        (
            "_lastUpdated",
            f"ge{start_date}T00:00:00Z"
        ),
        (
            "_lastUpdated",
            f"lt{end_date + timedelta(days=1)}T00:00:00Z"
        )
    ]

    current_url = initial_url
    current_params = params

    all_entries = []
    raw_responses = []

    page_number = 1

    api_call_start = datetime.now(timezone.utc)

    while current_url:

        print(
            f"Fetching {resource} - "
            f"Page {page_number}"
        )

        response = make_api_request(
            current_url,
            current_params
        )

        response_json = response.json()

        raw_responses.append({
            "page_number": page_number,
            "url": response.url,
            "response": response_json
        })

        entries = response_json.get(
            "entry",
            []
        )

        all_entries.extend(entries)

        print(
            f"Page {page_number}: "
            f"{len(entries)} records"
        )

        next_url = get_next_page_url(
            response_json
        )

        current_url = next_url

        current_params = None

        page_number += 1

    api_call_end = datetime.now(timezone.utc)

    return {
        "resource": resource,
        "records": all_entries,
        "raw_responses": raw_responses,
        "pages": page_number - 1,
        "api_call_start": api_call_start,
        "api_call_end": api_call_end
    }

# COMMAND ----------

test_result = fetch_fhir_resources(
    resource="Patient",
    start_date=start_date,
    end_date=end_date
)

print(
    "Patient records:",
    len(test_result["records"])
)

print(
    "Pages:",
    test_result["pages"]
)

# COMMAND ----------

def save_raw_response(
    resource,
    raw_responses,
    execution_date
):
    date_path = (
        f"{RAW_BASE_PATH}/"
        f"{resource}/"
        f"{execution_date}"
    )

    import os
    os.makedirs(date_path, exist_ok=True)

    saved_files = []

    for page_data in raw_responses:

        page_number = page_data["page_number"]

        file_path = (
            f"{date_path}/"
            f"batch_{page_number:04d}.json"
        )

        json_content = json.dumps(
            page_data["response"],
            indent=2
        )

        with open(
            file_path,
            "w",
            encoding="utf-8"
        ) as f:
            f.write(json_content)

        saved_files.append(file_path)

        print(
            f"Saved raw response: {file_path}"
        )

    return saved_files

# COMMAND ----------

bronze_schema = StructType([
    StructField(
        "resource_id",
        StringType(),
        True
    ),

    StructField(
        "resource_type",
        StringType(),
        True
    ),
    StructField(
        "resource_json",
        StringType(),
        True
    ),
    StructField(
        "extraction_timestamp",
        TimestampType(),
        True
    ),
    StructField(
        "api_url_or_params",
        StringType(),
        True
    ),
    StructField(
        "source_page",
        LongType(),
        True
    ),
    StructField(
        "ingestion_date",
        StringType(),
        True
    )
])

# COMMAND ----------

def create_bronze_dataframe(
    result,
    extraction_timestamp
):
    resource = result["resource"]

    records = []

    for page_data in result["raw_responses"]:

        page_number = page_data["page_number"]

        response_json = page_data["response"]

        for entry in response_json.get(
            "entry",
            []
        ):

            resource_data = entry.get(
                "resource",
                {}
            )

            resource_id = resource_data.get(
                "id"
            )

            records.append({
                "resource_id":
                    resource_id,

                "resource_type":
                    resource_data.get(
                        "resourceType",
                        resource
                    ),

                "resource_json":
                    json.dumps(
                        resource_data
                    ),

                "extraction_timestamp":
                    extraction_timestamp,

                "api_url_or_params":
                    page_data["url"],

                "source_page":
                    page_number,

                "ingestion_date":
                    extraction_timestamp.date().isoformat()
            })

    return spark.createDataFrame(
        records,
        schema=bronze_schema
    )

# COMMAND ----------

def write_bronze_table(
    df,
    resource
):
    table_name = (
        f"{BRONZE_TABLE_PREFIX}"
        f"{resource.lower()}"
    )

    (
        df.write
        .format("delta")
        .mode("append")
        .option(
            "mergeSchema",
            "true"
        )
        .saveAsTable(table_name)

    )

    print(
        f"Bronze table written: {table_name}"
    )

    return table_name

# COMMAND ----------

metadata_schema = StructType([
    StructField(
        "pipeline_name",
        StringType(),
        True
    ),
    StructField(
        "resource_name",
        StringType(),
        True
    ),
    StructField(
        "start_time",
        TimestampType(),
        True
    ),
    StructField(
        "end_time",
        TimestampType(),
        True
    ),
    StructField(
        "records_read",
        LongType(),
        True
    ),
    StructField(
        "pages_read",
        LongType(),
        True
    ),
    StructField(
        "records_written",
        LongType(),
        True
    ),
    StructField(
        "status",
        StringType(),
        True
    ),
    StructField(
        "api_url_or_params",
        StringType(),
        True
    ),
    StructField(
        "error_message",
        StringType(),
        True
    ),
    StructField(
        "ingestion_date",
        StringType(),
        True
    ),
    StructField(
        "execution_timestamp",
        TimestampType(),
        True
    )
])

# COMMAND ----------

if not spark.catalog.tableExists(
    "pipeline_execution_log"
):
    empty_df = spark.createDataFrame(
        [],
        metadata_schema
    )

    (
        empty_df.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(
            "pipeline_execution_log"
        )
    )
    print(
        "Created pipeline_execution_log"
    )

else:
    print(
        "pipeline_execution_log already exists"
    )

# COMMAND ----------

def log_execution(
    resource_name,
    start_time,
    end_time,
    records_read,
    pages_read,
    records_written,
    status,
    api_url_or_params,
    error_message=None
):
    log_record = [{
        "pipeline_name":
            "PL_FHIR_Master",

        "resource_name":
            resource_name,

        "start_time":
            start_time,

        "end_time":
            end_time,

        "records_read":
            records_read,

        "pages_read":
            pages_read,

        "records_written":
            records_written,

        "status":
            status,

        "api_url_or_params":
            api_url_or_params,

        "error_message":
            error_message,

        "ingestion_date":
            datetime.now(
                timezone.utc
            ).date().isoformat(),

        "execution_timestamp":
            datetime.now(
                timezone.utc
            )
    }]

    log_df = spark.createDataFrame(
        log_record,
        schema=metadata_schema
    )

    (
        log_df.write
        .format("delta")
        .mode("append")
        .saveAsTable(
            "pipeline_execution_log"
        )
    )

    print(
        f"Execution logged for {resource_name}"
    )

# COMMAND ----------

overall_start = datetime.now(timezone.utc)

execution_date = datetime.now(
    timezone.utc
).date().isoformat()

print("=" * 70)
print("FHIR INGESTION STARTED")
print("=" * 70)

for resource in RESOURCES:

    resource_start = datetime.now(
        timezone.utc
    )

    print("\n")
    print("=" * 70)
    print(f"PROCESSING RESOURCE: {resource}")
    print("=" * 70)

    try:

        result = fetch_fhir_resources(
            resource=resource,
            start_date=start_date,
            end_date=end_date
        )

        records_read = len(
            result["records"]
        )

        pages_read = result["pages"]

        print(
            f"Total records fetched: "
            f"{records_read}"
        )

        print(
            f"Total pages fetched: "
            f"{pages_read}"
        )

        
        raw_files = save_raw_response(
            resource=resource,
            raw_responses=result[
                "raw_responses"
            ],
            execution_date=execution_date
        )

        print(
            f"Raw files saved: "
            f"{len(raw_files)}"
        )


        bronze_df = create_bronze_dataframe(
            result=result,
            extraction_timestamp=EXTRACTION_TIMESTAMP
        )

        records_written = bronze_df.count()

      
        table_name = write_bronze_table(
            df=bronze_df,
            resource=resource
        )

        resource_end = datetime.now(
            timezone.utc
        )


        log_execution(

            resource_name=resource,

            start_time=resource_start,

            end_time=resource_end,

            records_read=records_read,

            pages_read=pages_read,

            records_written=records_written,

            status="SUCCESS",

            api_url_or_params=(
                f"{BASE_URL}/{resource}"
            ),

            error_message=None
        )

        print(
            f"{resource} ingestion completed successfully"
        )

    except Exception as e:

        resource_end = datetime.now(
            timezone.utc
        )

        error_message = (
            str(e)
            + "\n"
            + traceback.format_exc()
        )

        print(
            f"ERROR processing {resource}:"
        )

        print(error_message)

    

        log_execution(

            resource_name=resource,

            start_time=resource_start,

            end_time=resource_end,

            records_read=0,

            pages_read=0,

            records_written=0,

            status="FAILED",

            api_url_or_params=(
                f"{BASE_URL}/{resource}"
            ),

            error_message=error_message
        )


        continue


overall_end = datetime.now(timezone.utc)

print("\n")
print("=" * 70)
print("FHIR INGESTION COMPLETED")
print("=" * 70)

print(
    "Overall duration:",
    overall_end - overall_start
)

# COMMAND ----------

for resource in RESOURCES:
    table_name = (
        f"bronze_{resource.lower()}"
    )
    if spark.catalog.tableExists(
        table_name
    ):
        df = spark.table(
            table_name
        )
        print("\n")
        print(
            f"===={table_name}===="
        )

        print(
            "Record count:",
            df.count()
        )
        df.show(
            5,
            truncate = False
        )
    else:
        print(
            f"{table_name} does not exist"
        )

# COMMAND ----------

log_df = spark.table(
    "pipeline_execution_log"
)
display(
    log_df
    .orderBy(
        F.col("execution_timestamp").desc()
    )
)

# COMMAND ----------

for resource in RESOURCES:
    path = (
        f"{RAW_BASE_PATH}/"
        f"{resource}/"
        f"{execution_date}"
    )
    print("\n")
    print(
        f"Files for {resource}:"
    )

    try:
        files = dbutils.fs.ls(
            path
        )

        for file in files:

            print(
                file.path
            )
    except Exception as e:
        print(
            f"No files found: {str(e)}"
        )

# COMMAND ----------

display(
    spark.sql("""
        SELECT 
            resource_id,
            resource_type,
            extraction_timestamp,
            ingestion_date,
            source_page
        FROM bronze_patient
        ORDER BY extraction_timestamp DESC
        LIMIT 20
    """)
)

# COMMAND ----------

for resource in RESOURCES:
    table_name = (
        f"bronze_{resource.lower()}"

    )
    print("\n")
    print(
        f"Duplicate check: {table_name}"
    )
    
    duplicate_df = spark.sql(f"""
        SELECT 
            resource_id,
            COUNT(*) AS record_count
        FROM {table_name}
        GROUP BY resource_id
        HAVING COUNT(*) > 1
        ORDER BY record_count DESC
    """)
    display(
        duplicate_df.limit(20)
    )



# COMMAND ----------

print("=" * 80)
print("FHIR INGESTION SUMMARY")
print("=" * 80)

for resource in RESOURCES:
    table_name = (
        f"bronze_{resource.lower()}"
    )
    try:
        count = spark.table(
            table_name
        ).count()
        print(
            f"{resource:<20} "
            f"{count:>10} records"
        )
    except Exception as e:
        print(
            f"{resource:<20} FAILED"
        )          
print("=" * 80)

successful = spark.sql("""
    SELECT
        resource_name,
        records_read,
        records_written,
        pages_read,
        status
    FROM pipeline_execution_log
    WHERE execution_timestamp = 
    (
        SELECT
            MAX(execution_timestamp)
            FROM pipeline_execution_log
    )
    """)
display(successful)

# COMMAND ----------

dbutils.notebook.exit("SUCCESS")