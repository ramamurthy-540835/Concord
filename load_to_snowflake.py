import snowflake.connector
import csv
import os
import sys

SNOWFLAKE_CONFIG = {
    "account": "OWBTACS-NX61521",
    "user": "DIRAC",
    "role": "ACCOUNTADMIN",
    "warehouse": None,
}

CSV_FILE = "/home/appadmin/GCP-Studio/Concord/music_tracks_clean.csv"
DATABASE = "MUSIC2"
SCHEMA = "PUBLIC"
TABLE = "MUSIC_TRACKS"

CREATE_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {DATABASE}.{SCHEMA}.{TABLE} (
    TRACK_ID INTEGER NOT NULL,
    TRACK_NAME VARCHAR NOT NULL,
    ARTIST_NAME VARCHAR NOT NULL,
    GENRE VARCHAR NOT NULL,
    RELEASE_YEAR INTEGER NOT NULL,
    DURATION_SECONDS INTEGER NOT NULL,
    STREAMS INTEGER NOT NULL,
    ALBUM_NAME VARCHAR NOT NULL,
    LABEL VARCHAR NOT NULL
)
"""

def main():
    password = os.environ.get("SF_PASSWORD") or sys.argv[1] if len(sys.argv) > 1 else None
    if not password:
        print("Usage: SF_PASSWORD=<your_password> python3 load_to_snowflake.py")
        print("   or: python3 load_to_snowflake.py <your_password>")
        sys.exit(1)

    print("Connecting to Snowflake...")
    conn = snowflake.connector.connect(
        account=SNOWFLAKE_CONFIG["account"],
        user=SNOWFLAKE_CONFIG["user"],
        password=password,
        role=SNOWFLAKE_CONFIG["role"],
    )
    cur = conn.cursor()

    # List available warehouses
    cur.execute("SHOW WAREHOUSES")
    warehouses = cur.fetchall()
    if warehouses:
        wh_name = warehouses[0][0]
        print(f"Using warehouse: {wh_name}")
        cur.execute(f"USE WAREHOUSE {wh_name}")
    else:
        print("ERROR: No warehouses available. Ask your admin to grant access.")
        sys.exit(1)

    # Create database and table
    cur.execute(f"CREATE DATABASE IF NOT EXISTS {DATABASE}")
    cur.execute(f"USE DATABASE {DATABASE}")
    cur.execute(f"USE SCHEMA {SCHEMA}")
    print(f"Created database: {DATABASE}")

    cur.execute(CREATE_TABLE_SQL)
    print(f"Created table: {TABLE}")

    # Load CSV data
    print(f"Loading data from {CSV_FILE}...")
    with open(CSV_FILE, "r") as f:
        reader = csv.reader(f)
        header = next(reader)  # skip header
        rows = list(reader)

    insert_sql = f"""
    INSERT INTO {TABLE} (TRACK_ID, TRACK_NAME, ARTIST_NAME, GENRE,
                         RELEASE_YEAR, DURATION_SECONDS, STREAMS, ALBUM_NAME, LABEL)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    batch_size = 200
    total = 0
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i + batch_size]
        typed_batch = [
            (int(r[0]), r[1], r[2], r[3], int(r[4]), int(r[5]), int(r[6]), r[7], r[8])
            for r in batch
        ]
        cur.executemany(insert_sql, typed_batch)
        total += len(batch)
        print(f"  Inserted {total}/{len(rows)} rows...")

    # Verify
    cur.execute(f"SELECT COUNT(*) FROM {TABLE}")
    count = cur.fetchone()[0]
    print(f"\nDone! {count} rows in {DATABASE}.{SCHEMA}.{TABLE}")

    # Show sample
    cur.execute(f"SELECT * FROM {TABLE} ORDER BY STREAMS DESC LIMIT 5")
    print("\nTop 5 tracks by streams:")
    for row in cur.fetchall():
        print(f"  {row[2]:20s} | {row[3]:12s} | {row[6]:>12,} streams")

    cur.close()
    conn.close()

if __name__ == "__main__":
    main()
