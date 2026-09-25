"""
Migrate Sample Pop Artist TikTok Data from Snowflake Marketplace to BigQuery.
Two tables: SM_AUTHOR_TIKTOK_VIEW (authors) and SM_TIKTOK_VIEW (posts).
Uses RSA key pair authentication (no password required).
"""
import snowflake.connector
import csv
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

GCS_BUCKET = "gs://ctoteam-data"
BQ_PROJECT = "ctoteam"
BQ_DATASET = "tiktok_music"
LOCAL_DIR = "/home/appadmin/GCP-Studio/Concord"

TABLES = [
    {
        "sf_view": "SM_AUTHOR_TIKTOK_VIEW",
        "local_csv": f"{LOCAL_DIR}/tiktok_authors.csv",
        "gcs_path": f"{GCS_BUCKET}/snowflake-migration/tiktok_authors.csv",
        "bq_table": "authors",
    },
    {
        "sf_view": "SM_TIKTOK_VIEW",
        "local_csv": f"{LOCAL_DIR}/tiktok_posts.csv",
        "gcs_path": f"{GCS_BUCKET}/snowflake-migration/tiktok_posts.csv",
        "bq_table": "posts",
    },
]


def load_private_key():
    with open(RSA_KEY_PATH, "rb") as key_file:
        return serialization.load_pem_private_key(
            key_file.read(),
            password=None,
            backend=default_backend(),
        )


def connect_snowflake():
    """Connect to Snowflake using RSA key pair authentication."""
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


def find_shared_database(cur):
    """Find the Socialgist TikTok shared database."""
    cur.execute("SHOW DATABASES")
    databases = cur.fetchall()
    name_idx = next(i for i, d in enumerate(cur.description) if d[0] == "name")
    origin_idx = next(i for i, d in enumerate(cur.description) if d[0] == "origin")

    for row in databases:
        db_name = row[name_idx]
        origin = row[origin_idx] or ""
        if "SOCIALGIST" in db_name.upper() or "TIKTOK" in db_name.upper() or origin:
            print(f"  Found shared database: {db_name} (origin: {origin})")
            return db_name

    print("\nAvailable databases:")
    for row in databases:
        print(f"  {row[name_idx]}")
    print("\nERROR: Could not auto-detect the TikTok shared database.")
    print("The marketplace listing may not be installed yet.")
    print("Re-run with: python3 migrate_tiktok_to_bigquery.py <database_name>")
    sys.exit(1)


def step1_extract(db_override=None):
    """Extract both tables from Snowflake to local CSVs."""
    print("\n=== STEP 1: Extract from Snowflake (RSA key pair auth) ===")
    conn = connect_snowflake()
    cur = conn.cursor()

    cur.execute("SHOW WAREHOUSES")
    warehouses = cur.fetchall()
    if warehouses:
        cur.execute(f"USE WAREHOUSE {warehouses[0][0]}")
    else:
        print("ERROR: No warehouses available.")
        sys.exit(1)

    if db_override:
        sf_database = db_override
    else:
        sf_database = find_shared_database(cur)

    cur.execute(f'USE DATABASE "{sf_database}"')

    cur.execute("SHOW SCHEMAS")
    schemas = cur.fetchall()
    schema_names = [row[1] for row in schemas]
    print(f"  Available schemas: {schema_names}")

    schema = "PUBLIC"
    for s in schema_names:
        if s not in ("INFORMATION_SCHEMA", "PUBLIC"):
            schema = s
            break
    cur.execute(f'USE SCHEMA "{schema}"')
    print(f"  Using schema: {schema}")

    row_counts = {}
    for tbl in TABLES:
        view = tbl["sf_view"]
        local_csv = tbl["local_csv"]

        print(f"\n  Extracting {view}...")
        try:
            cur.execute(f'SELECT * FROM "{view}"')
        except Exception:
            cur.execute(f"SHOW VIEWS")
            views = cur.fetchall()
            print(f"  Available views: {[v[1] for v in views]}")
            cur.execute(f"SHOW TABLES")
            tables = cur.fetchall()
            print(f"  Available tables: {[t[1] for t in tables]}")
            raise

        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]

        with open(local_csv, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(columns)
            writer.writerows(rows)

        row_counts[view] = len(rows)
        print(f"  {view}: {len(rows)} rows -> {local_csv}")

    cur.close()
    conn.close()
    return row_counts


def step2_upload_to_gcs():
    """Upload CSVs to Google Cloud Storage."""
    print("\n=== STEP 2: Upload to GCS ===")
    for tbl in TABLES:
        result = subprocess.run(
            ["gsutil", "cp", tbl["local_csv"], tbl["gcs_path"]],
            capture_output=True, text=True,
        )
        if result.returncode != 0:
            print(f"  ERROR uploading {tbl['local_csv']}: {result.stderr}")
            sys.exit(1)
        print(f"  Uploaded {tbl['local_csv']} -> {tbl['gcs_path']}")


def step3_load_into_bigquery():
    """Load from GCS into BigQuery with auto-detect schema."""
    print("\n=== STEP 3: Load into BigQuery ===")

    subprocess.run(
        ["bq", "mk", "--dataset", "--location=US",
         f"{BQ_PROJECT}:{BQ_DATASET}"],
        capture_output=True, text=True,
    )
    print(f"  Dataset: {BQ_PROJECT}:{BQ_DATASET}")

    for tbl in TABLES:
        bq_full = f"{BQ_PROJECT}:{BQ_DATASET}.{tbl['bq_table']}"

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
             tbl["gcs_path"]],
            capture_output=True, text=True,
        )
        if result.returncode != 0:
            print(f"  ERROR loading {tbl['bq_table']}: {result.stderr}")
            sys.exit(1)
        print(f"  Loaded -> {bq_full}")


def step4_validate(sf_counts):
    """Validate row counts match between Snowflake and BigQuery."""
    print("\n=== STEP 4: Validate ===")
    all_match = True

    for tbl in TABLES:
        bq_ref = f"`{BQ_PROJECT}.{BQ_DATASET}.{tbl['bq_table']}`"
        result = subprocess.run(
            ["bq", "query", "--use_legacy_sql=false", "--format=csv",
             f"SELECT COUNT(*) as cnt FROM {bq_ref}"],
            capture_output=True, text=True,
        )
        lines = result.stdout.strip().split("\n")
        bq_count = int(lines[-1]) if len(lines) > 1 else 0
        sf_count = sf_counts.get(tbl["sf_view"], 0)

        status = "OK" if bq_count == sf_count else "MISMATCH"
        print(f"  {tbl['bq_table']}: Snowflake={sf_count}, BigQuery={bq_count} [{status}]")
        if bq_count != sf_count:
            all_match = False

    result = subprocess.run(
        ["bq", "query", "--use_legacy_sql=false", "--format=prettyjson",
         f"SELECT NAME, FOLLOWERS, HEARTS FROM `{BQ_PROJECT}.{BQ_DATASET}.authors` "
         f"ORDER BY FOLLOWERS DESC LIMIT 5"],
        capture_output=True, text=True,
    )
    print(f"\n  Top 5 authors by followers:\n{result.stdout}")

    return all_match


def main():
    db_override = sys.argv[1] if len(sys.argv) > 1 else None

    if not os.path.exists(RSA_KEY_PATH):
        print(f"ERROR: RSA private key not found at {RSA_KEY_PATH}")
        print("Generate one with:")
        print("  openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out ~/.ssh/snowflake_rsa_key.p8 -nocrypt")
        sys.exit(1)

    print(f"Using RSA key (passwordless): {RSA_KEY_PATH}")
    print("Usage: python3 migrate_tiktok_to_bigquery.py [database_name]\n")

    sf_counts = step1_extract(db_override)
    step2_upload_to_gcs()
    step3_load_into_bigquery()
    success = step4_validate(sf_counts)

    print("\n" + "=" * 50)
    if success:
        print("SUCCESS: TikTok music data migrated to BigQuery")
    else:
        print("WARNING: Row count mismatch detected")

    for tbl in TABLES:
        if os.path.exists(tbl["local_csv"]):
            os.remove(tbl["local_csv"])
    print("Cleaned up local CSVs")
    print("=" * 50)


if __name__ == "__main__":
    main()
