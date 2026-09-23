"""
Snowflake to BigQuery Migration Script
Extracts data from Snowflake, stages in GCS, loads into BigQuery.
"""
import snowflake.connector
import csv
import getpass
import subprocess
import os
import sys

# --- Configuration ---
SNOWFLAKE_CONFIG = {
    "account": "A5701997473071-MPA05784",
    "user": "RAMAMURTHY.VALAVANDAN@MASTECHDIGITAL.COM",
    "role": "PUBLIC",
}

SF_DATABASE = "MUSIC_DATA"
SF_SCHEMA = "PUBLIC"
SF_TABLE = "MUSIC_TRACKS"

GCS_BUCKET = "gs://ctoteam-data"
GCS_PATH = f"{GCS_BUCKET}/snowflake-migration/music_tracks.csv"

BQ_PROJECT = "ctoteam"
BQ_DATASET = "music_data"
BQ_TABLE = "music_tracks"
BQ_FULL_TABLE = f"{BQ_PROJECT}:{BQ_DATASET}.{BQ_TABLE}"
BQ_SCHEMA_FILE = "/home/appadmin/GCP-Studio/Concord/bq_schema_music_tracks.json"

LOCAL_EXPORT = "/home/appadmin/GCP-Studio/Concord/snowflake_export.csv"


def step1_extract_from_snowflake(password):
    """Extract data from Snowflake to local CSV."""
    print("\n=== STEP 1: Extract from Snowflake ===")
    conn = snowflake.connector.connect(
        account=SNOWFLAKE_CONFIG["account"],
        user=SNOWFLAKE_CONFIG["user"],
        password=password,
        role=SNOWFLAKE_CONFIG["role"],
        database=SF_DATABASE,
        schema=SF_SCHEMA,
    )
    cur = conn.cursor()

    cur.execute("SHOW WAREHOUSES")
    warehouses = cur.fetchall()
    if warehouses:
        cur.execute(f"USE WAREHOUSE {warehouses[0][0]}")

    cur.execute(f"SELECT * FROM {SF_TABLE}")
    rows = cur.fetchall()
    columns = [desc[0] for desc in cur.description]

    with open(LOCAL_EXPORT, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(columns)
        writer.writerows(rows)

    print(f"  Extracted {len(rows)} rows -> {LOCAL_EXPORT}")
    cur.close()
    conn.close()
    return len(rows)


def step2_upload_to_gcs():
    """Upload CSV to Google Cloud Storage."""
    print("\n=== STEP 2: Upload to GCS ===")
    result = subprocess.run(
        ["gsutil", "cp", LOCAL_EXPORT, GCS_PATH],
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        print(f"  ERROR: {result.stderr}")
        sys.exit(1)
    print(f"  Uploaded -> {GCS_PATH}")


def step3_load_into_bigquery():
    """Load from GCS into BigQuery."""
    print("\n=== STEP 3: Load into BigQuery ===")

    # Create dataset if not exists
    subprocess.run(
        ["bq", "mk", "--dataset", "--location=US",
         f"{BQ_PROJECT}:{BQ_DATASET}"],
        capture_output=True, text=True,
    )

    # Drop existing table to avoid duplicates
    subprocess.run(
        ["bq", "rm", "-f", "-t", BQ_FULL_TABLE],
        capture_output=True, text=True,
    )

    # Load from GCS
    result = subprocess.run(
        ["bq", "load",
         "--source_format=CSV",
         "--skip_leading_rows=1",
         f"--schema={BQ_SCHEMA_FILE}",
         BQ_FULL_TABLE,
         GCS_PATH],
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        print(f"  ERROR: {result.stderr}")
        sys.exit(1)
    print(f"  Loaded into {BQ_FULL_TABLE}")


def step4_validate():
    """Compare row counts between source and target."""
    print("\n=== STEP 4: Validate ===")
    result = subprocess.run(
        ["bq", "query", "--use_legacy_sql=false", "--format=csv",
         f"SELECT COUNT(*) as cnt FROM `{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}`"],
        capture_output=True, text=True,
    )
    lines = result.stdout.strip().split("\n")
    bq_count = int(lines[-1]) if len(lines) > 1 else 0
    print(f"  BigQuery row count: {bq_count}")

    result = subprocess.run(
        ["bq", "query", "--use_legacy_sql=false", "--format=csv",
         f"SELECT ARTIST_NAME, COUNT(*) as tracks, SUM(STREAMS) as total_streams "
         f"FROM `{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}` "
         f"GROUP BY ARTIST_NAME ORDER BY total_streams DESC LIMIT 5"],
        capture_output=True, text=True,
    )
    print(f"\n  Top artists in BigQuery:\n{result.stdout}")
    return bq_count


def main():
    password = getpass.getpass("Enter Snowflake password: ")

    row_count = step1_extract_from_snowflake(password)
    step2_upload_to_gcs()
    step3_load_into_bigquery()
    bq_count = step4_validate()

    print("\n" + "=" * 50)
    if bq_count == row_count:
        print(f"SUCCESS: {row_count} rows migrated from Snowflake to BigQuery")
    else:
        print(f"WARNING: Snowflake={row_count}, BigQuery={bq_count} — row count mismatch!")

    # Cleanup staging file
    os.remove(LOCAL_EXPORT)
    print(f"Cleaned up {LOCAL_EXPORT}")
    print("=" * 50)


if __name__ == "__main__":
    main()
