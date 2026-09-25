"""
Snowflake-to-BigQuery Connector using Storage Integration.
Uses Snowflake's native COPY INTO to stage data in GCS (music_revenue bucket),
then loads into BigQuery. Authenticates with RSA key pair.
"""
import snowflake.connector
import subprocess
import os
import sys
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

SNOWFLAKE_CONFIG = {
    "account": "OWBTACS-NX61521",
    "user": "DIRAC",
    "role": "ACCOUNTADMIN",
}

RSA_KEY_PATH = os.path.expanduser("~/.ssh/snowflake_bq_transfer_rsa_key.p8")

SF_DATABASE = "MUSIC2"
SF_SCHEMA = "PUBLIC"

GCS_BUCKET = "music_revenue"
GCS_STAGE_PATH = "snowflake-export"
STORAGE_INTEGRATION_NAME = "GCS_MUSIC_INTEGRATION"
STAGE_NAME = "GCS_MUSIC_STAGE"

BQ_PROJECT = "ctoteam"
BQ_DATASET = "music_data"


def load_private_key():
    with open(RSA_KEY_PATH, "rb") as key_file:
        return serialization.load_pem_private_key(
            key_file.read(),
            password=None,
            backend=default_backend(),
        )


def connect_snowflake():
    private_key = load_private_key()
    pkb = private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    return snowflake.connector.connect(
        account=SNOWFLAKE_CONFIG["account"],
        user=SNOWFLAKE_CONFIG["user"],
        role=SNOWFLAKE_CONFIG["role"],
        private_key=pkb,
    )


def setup_storage_integration(cur):
    """Create or replace the GCS storage integration and external stage."""
    print("\n=== Setting up Storage Integration ===")

    cur.execute(f"""
        CREATE STORAGE INTEGRATION IF NOT EXISTS {STORAGE_INTEGRATION_NAME}
          TYPE = EXTERNAL_STAGE
          STORAGE_PROVIDER = 'GCS'
          ENABLED = TRUE
          STORAGE_ALLOWED_LOCATIONS = ('gcs://{GCS_BUCKET}/')
    """)
    print(f"  Storage integration: {STORAGE_INTEGRATION_NAME}")

    cur.execute(f"DESC STORAGE INTEGRATION {STORAGE_INTEGRATION_NAME}")
    rows = cur.fetchall()
    for row in rows:
        if row[0] in ("STORAGE_GCP_SERVICE_ACCOUNT", "STORAGE_ALLOWED_LOCATIONS"):
            print(f"  {row[0]}: {row[2]}")

    cur.execute(f"USE DATABASE {SF_DATABASE}")
    cur.execute(f"USE SCHEMA {SF_SCHEMA}")

    cur.execute(f"""
        CREATE STAGE IF NOT EXISTS {STAGE_NAME}
          STORAGE_INTEGRATION = {STORAGE_INTEGRATION_NAME}
          URL = 'gcs://{GCS_BUCKET}/{GCS_STAGE_PATH}/'
          FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"'
                         COMPRESSION = NONE SKIP_HEADER = 0)
    """)
    print(f"  Stage: {STAGE_NAME} -> gcs://{GCS_BUCKET}/{GCS_STAGE_PATH}/")


def list_tables(cur):
    """List all tables in the database."""
    cur.execute(f"USE DATABASE {SF_DATABASE}")
    cur.execute(f"USE SCHEMA {SF_SCHEMA}")
    cur.execute("SHOW TABLES")
    tables = cur.fetchall()
    table_names = [row[1] for row in tables]
    print(f"\n  Tables in {SF_DATABASE}.{SF_SCHEMA}: {table_names}")
    return table_names


def export_table_to_gcs(cur, table_name):
    """Export a Snowflake table to GCS via COPY INTO stage."""
    print(f"\n=== Exporting {table_name} to GCS ===")

    gcs_file = f"{table_name.lower()}"
    cur.execute(f"""
        COPY INTO @{STAGE_NAME}/{gcs_file}/
        FROM {SF_DATABASE}.{SF_SCHEMA}.{table_name}
        FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"'
                       COMPRESSION = NONE)
        HEADER = TRUE
        OVERWRITE = TRUE
        SINGLE = TRUE
        MAX_FILE_SIZE = 5368709120
    """)
    result = cur.fetchall()
    rows_unloaded = result[0][0] if result else 0
    print(f"  Exported {rows_unloaded} rows -> gcs://{GCS_BUCKET}/{GCS_STAGE_PATH}/{gcs_file}/")
    return rows_unloaded


def load_table_to_bigquery(table_name, sf_row_count):
    """Load a table from GCS into BigQuery."""
    print(f"\n=== Loading {table_name} into BigQuery ===")

    bq_table_name = table_name.lower()
    bq_full = f"{BQ_PROJECT}:{BQ_DATASET}.{bq_table_name}"
    gcs_uri = f"gs://{GCS_BUCKET}/{GCS_STAGE_PATH}/{bq_table_name}/*.csv"

    subprocess.run(
        ["bq", "mk", "--dataset", "--location=US", f"{BQ_PROJECT}:{BQ_DATASET}"],
        capture_output=True, text=True,
    )

    subprocess.run(
        ["bq", "rm", "-f", "-t", bq_full],
        capture_output=True, text=True,
    )

    result = subprocess.run(
        ["bq", "load",
         "--source_format=CSV",
         "--skip_leading_rows=1",
         "--autodetect",
         bq_full,
         gcs_uri],
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        print(f"  ERROR: {result.stderr}")
        return False

    result = subprocess.run(
        ["bq", "query", "--use_legacy_sql=false", "--format=csv",
         f"SELECT COUNT(*) as cnt FROM `{BQ_PROJECT}.{BQ_DATASET}.{bq_table_name}`"],
        capture_output=True, text=True,
    )
    lines = result.stdout.strip().split("\n")
    bq_count = int(lines[-1]) if len(lines) > 1 else 0

    status = "OK" if bq_count == sf_row_count else "MISMATCH"
    print(f"  {bq_table_name}: Snowflake={sf_row_count}, BigQuery={bq_count} [{status}]")
    return bq_count == sf_row_count


def main():
    if not os.path.exists(RSA_KEY_PATH):
        print(f"ERROR: RSA private key not found at {RSA_KEY_PATH}")
        sys.exit(1)

    tables_arg = sys.argv[1:] if len(sys.argv) > 1 else None

    print(f"Snowflake account: {SNOWFLAKE_CONFIG['account']}")
    print(f"Database: {SF_DATABASE}")
    print(f"GCS bucket: gs://{GCS_BUCKET}/{GCS_STAGE_PATH}/")
    print(f"BigQuery target: {BQ_PROJECT}.{BQ_DATASET}")

    conn = connect_snowflake()
    cur = conn.cursor()

    cur.execute("SHOW WAREHOUSES")
    warehouses = cur.fetchall()
    if warehouses:
        cur.execute(f"USE WAREHOUSE {warehouses[0][0]}")
        print(f"Using warehouse: {warehouses[0][0]}")
    else:
        print("ERROR: No warehouses available.")
        sys.exit(1)

    setup_storage_integration(cur)

    all_tables = list_tables(cur)
    target_tables = tables_arg if tables_arg else all_tables

    results = {}
    for table_name in target_tables:
        if table_name.upper() not in [t.upper() for t in all_tables]:
            print(f"  WARNING: Table {table_name} not found, skipping")
            continue

        sf_rows = export_table_to_gcs(cur, table_name.upper())
        success = load_table_to_bigquery(table_name.upper(), sf_rows)
        results[table_name] = {"rows": sf_rows, "success": success}

    cur.close()
    conn.close()

    print("\n" + "=" * 60)
    print("MIGRATION SUMMARY")
    print("=" * 60)
    for table, info in results.items():
        status = "SUCCESS" if info["success"] else "FAILED"
        print(f"  {table}: {info['rows']} rows [{status}]")
    print("=" * 60)


if __name__ == "__main__":
    main()
