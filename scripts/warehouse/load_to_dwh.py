
import os
import glob
import csv
import io
import pandas as pd
import psycopg2

# ============================================
# 1. DATABASE CONNECTION
# ============================================

DB_CONFIG = {
    "host": os.getenv(
        "POSTGRES_HOST", "pipeline-warehouse-postgres"
    ),
    "port": 5432,
    "dbname": os.getenv(
        "POSTGRES_DB", "ecommerce_warehouse"
    ),
    "user": os.getenv(
        "POSTGRES_USER", "warehouse"
    ),
    "password": os.getenv(
        "POSTGRES_PASSWORD", "warehouse"
    ),
}

SOURCE = "etsy"
BATCH_SIZE = 5000


# ============================================
# 2. READ ETSY PARQUET FILES
# ============================================

files = sorted(
    glob.glob("data/raw/external/etsy/*.parquet")
)

print(f"📦 Etsy Parquet files: {len(files):,}")

if not files:
    raise SystemExit("❌ Etsy Parquet files not found")

df = pd.concat(
    [pd.read_parquet(file) for file in files],
    ignore_index=True
)

print(f"📊 Raw rows: {len(df):,}")

required = [
    "external_product_id",
    "product_name",
    "collected_at"
]

missing = [c for c in required if c not in df.columns]

if missing:
    raise ValueError(f"Missing columns: {missing}")


# ============================================
# 3. CLEAN AND VALIDATE DATA
# ============================================

df["external_product_id"] = (
    df["external_product_id"]
    .astype("string")
    .str.strip()
)

df["collected_at"] = pd.to_datetime(
    df["collected_at"],
    errors="coerce",
    utc=True
)

valid = (
    df["external_product_id"].notna()
    & df["external_product_id"].ne("")
    & df["collected_at"].notna()
)

invalid_count = int((~valid).sum())

df = df.loc[valid].copy()

# เก็บ timestamp ตาม UTC แบบไม่มี timezone
df["collected_at"] = (
    df["collected_at"].dt.tz_convert(None)
)

df["full_date"] = df["collected_at"].dt.date

df["product_name"] = (
    df["product_name"]
    .fillna("")
    .astype(str)
    .str.strip()
)

empty_names = df["product_name"].eq("")

df.loc[empty_names, "product_name"] = (
    "Etsy product "
    + df.loc[empty_names, "external_product_id"]
)

if "currency" not in df.columns:
    df["currency"] = None

if "price" not in df.columns:
    df["price"] = None

if "product_url" not in df.columns:
    df["product_url"] = None

df["price"] = pd.to_numeric(
    df["price"],
    errors="coerce"
)

# ตรวจสอบช่วงข้อมูลให้ตรงกับ numeric(18,4)
df.loc[
    df["price"].abs() >= 10**14,
    "price"
] = float("nan")

df["price"] = df["price"].round(4)

# เก็บ snapshot ล่าสุดของแต่ละสินค้าในแต่ละวัน
df = df.sort_values("collected_at")

df = df.drop_duplicates(
    subset=["external_product_id", "full_date"],
    keep="last"
).copy()

print(f"✨ Valid snapshot rows: {len(df):,}")
print(f"🧩 Unique products: {df['external_product_id'].nunique():,}")
print(f"⚠️ Invalid ID/date rows: {invalid_count:,}")


# ============================================
# 4. PREPARE STAGING RECORDS
# ============================================

def clean_copy_value(value):
    if value is None:
        return r"\N"

    if pd.isna(value):
        return r"\N"

    return str(value)


def load_staging(cursor, dataframe):
    total = len(dataframe)

    columns = [
        "external_product_id",
        "product_name",
        "currency",
        "price",
        "product_url",
        "collected_at",
        "full_date",
    ]

    for start in range(0, total, BATCH_SIZE):
        batch = dataframe.iloc[
            start:start + BATCH_SIZE
        ][columns]

        buffer = io.StringIO()

        writer = csv.writer(
            buffer,
            lineterminator="\n"
        )

        for record in batch.itertuples(
            index=False,
            name=None
        ):
            writer.writerow([
                clean_copy_value(v)
                for v in record
            ])

        buffer.seek(0)

        cursor.copy_expert(
            """
            COPY stg_etsy (
                external_product_id,
                product_name,
                currency,
                price,
                product_url,
                collected_at,
                full_date
            )
            FROM STDIN
            WITH (
                FORMAT CSV,
                NULL '\\N'
            )
            """,
            buffer
        )

        print(
            f"📥 Staging: "
            f"{min(start + BATCH_SIZE, total):,}"
            f"/{total:,}"
        )


# ============================================
# 5. TRANSACTION: LOAD WAREHOUSE
# ============================================

conn = psycopg2.connect(**DB_CONFIG)

try:
    with conn:
        with conn.cursor() as cur:

            # Verify platform
            cur.execute(
                """
                SELECT platform_key
                FROM dim_platform
                WHERE platform_name = %s
                """,
                (SOURCE,)
            )

            platform = cur.fetchone()

            if not platform:
                raise RuntimeError(
                    "Etsy not found in dim_platform"
                )

            platform_key = platform[0]

            # Count before load
            cur.execute(
                """
                SELECT COUNT(*)
                FROM dim_product
                WHERE source_system = %s
                """,
                (SOURCE,)
            )

            dim_before = cur.fetchone()[0]

            cur.execute(
                """
                SELECT COUNT(*)
                FROM fact_external_product
                WHERE platform_key = %s
                """,
                (platform_key,)
            )

            fact_before = cur.fetchone()[0]

            # Create isolated temporary staging table
            cur.execute(
                """
                CREATE TEMP TABLE stg_etsy (
                    external_product_id TEXT NOT NULL,
                    product_name TEXT NOT NULL,
                    currency TEXT,
                    price NUMERIC(18,4),
                    product_url TEXT,
                    collected_at TIMESTAMP NOT NULL,
                    full_date DATE NOT NULL
                ) ON COMMIT DROP
                """
            )

            load_staging(cur, df)

            # ====================================
            # 6. ADD MISSING DATES
            # ====================================

            cur.execute(
                """
                INSERT INTO dim_date (
                    date_key,
                    full_date,
                    day,
                    month,
                    month_name,
                    quarter,
                    year,
                    day_of_week,
                    day_name,
                    is_weekend
                )
                SELECT DISTINCT
                    TO_CHAR(s.full_date, 'YYYYMMDD')::INTEGER,
                    s.full_date,
                    EXTRACT(DAY FROM s.full_date)::INTEGER,
                    EXTRACT(MONTH FROM s.full_date)::INTEGER,
                    TRIM(TO_CHAR(s.full_date, 'Month')),
                    EXTRACT(QUARTER FROM s.full_date)::INTEGER,
                    EXTRACT(YEAR FROM s.full_date)::INTEGER,
                    EXTRACT(ISODOW FROM s.full_date)::INTEGER,
                    TRIM(TO_CHAR(s.full_date, 'Day')),
                    EXTRACT(ISODOW FROM s.full_date) IN (6,7)
                FROM stg_etsy s
                ON CONFLICT (full_date) DO NOTHING
                """
            )

            # Validate date mapping
            cur.execute(
                """
                SELECT COUNT(DISTINCT s.full_date)
                FROM stg_etsy s
                LEFT JOIN dim_date d
                  ON d.full_date = s.full_date
                WHERE d.date_key IS NULL
                """
            )

            if cur.fetchone()[0] > 0:
                raise RuntimeError(
                    "Some Etsy dates are missing in dim_date"
                )

            # ====================================
            # 7. UPSERT DIM_PRODUCT
            # ====================================

            cur.execute(
                """
                INSERT INTO dim_product (
                    source_system,
                    source_product_id,
                    product_name
                )
                SELECT DISTINCT ON (
                    external_product_id
                )
                    %s,
                    external_product_id,
                    product_name
                FROM stg_etsy
                ORDER BY
                    external_product_id,
                    collected_at DESC
                ON CONFLICT (
                    source_system,
                    source_product_id
                )
                DO UPDATE SET
                    product_name = EXCLUDED.product_name,
                    updated_at = CURRENT_TIMESTAMP
                """,
                (SOURCE,)
            )

            # ====================================
            # 8. UPSERT FACT_EXTERNAL_PRODUCT
            # ====================================

            cur.execute(
                """
                INSERT INTO fact_external_product (
                    date_key,
                    product_key,
                    platform_key,
                    currency,
                    price,
                    product_url,
                    collected_at,
                    processed_at
                )
                SELECT
                    d.date_key,
                    p.product_key,
                    %s,
                    s.currency,
                    s.price,
                    s.product_url,
                    s.collected_at,
                    CURRENT_TIMESTAMP
                FROM stg_etsy s
                JOIN dim_product p
                  ON p.source_system = %s
                 AND p.source_product_id =
                     s.external_product_id
                JOIN dim_date d
                  ON d.full_date = s.full_date
                ON CONFLICT (
                    date_key,
                    product_key,
                    platform_key
                )
                DO UPDATE SET
                    currency = EXCLUDED.currency,
                    price = EXCLUDED.price,
                    product_url = EXCLUDED.product_url,
                    collected_at = EXCLUDED.collected_at,
                    processed_at = CURRENT_TIMESTAMP
                """,
                (platform_key, SOURCE)
            )

            # ====================================
            # 9. VERIFY RESULTS
            # ====================================

            cur.execute(
                """
                SELECT COUNT(*)
                FROM dim_product
                WHERE source_system = %s
                """,
                (SOURCE,)
            )

            dim_after = cur.fetchone()[0]

            cur.execute(
                """
                SELECT COUNT(*)
                FROM fact_external_product
                WHERE platform_key = %s
                """,
                (platform_key,)
            )

            fact_after = cur.fetchone()[0]

            print("\n" + "=" * 55)
            print("ETSY WAREHOUSE LOAD COMPLETED")
            print("=" * 55)
            print(f"Parquet files     : {len(files):,}")
            print(f"Snapshot rows     : {len(df):,}")
            print(f"Products before   : {dim_before:,}")
            print(f"Products after    : {dim_after:,}")
            print(f"Fact rows before  : {fact_before:,}")
            print(f"Fact rows after   : {fact_after:,}")
            print("=" * 55)

    print("✅ Etsy warehouse transaction committed")

except Exception:
    print("❌ Load failed. Transaction rolled back.")
    raise

finally:
    conn.close()
