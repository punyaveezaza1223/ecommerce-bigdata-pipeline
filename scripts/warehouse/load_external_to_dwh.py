from pathlib import Path
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values


# ============================================================
# PATH
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]

PROCESSED_PATH = (
    PROJECT_ROOT
    / "data"
    / "processed"
    / "products"
    / "external_products"
)


# ============================================================
# DATA WAREHOUSE CONNECTION
# ============================================================

DB_CONFIG = {
    "host": "127.0.0.1",
    "port": 5452,
    "database": "ecommerce_warehouse",
    "user": "warehouse",
    "password": "warehouse123",
}


# ============================================================
# HELPER
# ============================================================

def clean_value(value):
    """Convert pandas/NaN values to PostgreSQL-friendly values."""

    if pd.isna(value):
        return None

    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime()

    return value


def read_processed_data():
    print("=" * 60)
    print("READING SPARK PROCESSED DATA")
    print("=" * 60)

    if not PROCESSED_PATH.exists():
        raise FileNotFoundError(
            f"Processed data not found: {PROCESSED_PATH}"
        )

    df = pd.read_parquet(PROCESSED_PATH)

    print(f"Rows found : {len(df):,}")

    if df.empty:
        raise ValueError("Processed dataset is empty.")

    return df


# ============================================================
# LOAD DATE DIMENSION
# ============================================================

def load_dim_date(cursor, df):
    print("\nLoading dim_date...")

    dates = pd.to_datetime(
        df["collected_at"],
        errors="coerce"
    ).dropna().dt.date.unique()

    rows = []

    for date_value in dates:
        timestamp = pd.Timestamp(date_value)

        date_key = int(timestamp.strftime("%Y%m%d"))

        rows.append(
            (
                date_key,
                timestamp.date(),
                timestamp.day,
                timestamp.month,
                timestamp.strftime("%B"),
                timestamp.quarter,
                timestamp.year,
                timestamp.dayofweek + 1,
                timestamp.strftime("%A"),
                timestamp.dayofweek >= 5,
            )
        )

    execute_values(
        cursor,
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
        VALUES %s
        ON CONFLICT (date_key)
        DO NOTHING
        """,
        rows,
    )

    print(f"dim_date prepared : {len(rows):,}")


# ============================================================
# LOAD PRODUCT DIMENSION
# ============================================================

def load_dim_product(cursor, df):
    print("Loading dim_product...")

    products = (
        df[
            [
                "platform",
                "external_product_id",
                "product_name",
                "category",
                "brand",
            ]
        ]
        .drop_duplicates(
            subset=["platform", "external_product_id"]
        )
    )

    rows = []

    for row in products.itertuples(index=False):
        rows.append(
            (
                clean_value(row.platform),
                str(row.external_product_id),
                clean_value(row.product_name),
                clean_value(row.category),
                clean_value(row.brand),
            )
        )

    execute_values(
        cursor,
        """
        INSERT INTO dim_product (
            source_system,
            source_product_id,
            product_name,
            category,
            brand
        )
        VALUES %s
        ON CONFLICT (
            source_system,
            source_product_id
        )
        DO UPDATE SET
            product_name = EXCLUDED.product_name,
            category = EXCLUDED.category,
            brand = EXCLUDED.brand,
            updated_at = CURRENT_TIMESTAMP
        """,
        rows,
        page_size=5000,
    )

    print(f"dim_product prepared : {len(rows):,}")


# ============================================================
# LOAD EXTERNAL FACT
# ============================================================

def load_fact_external_product(cursor, df):
    print("Loading fact_external_product...")

    platform_map = {}

    cursor.execute(
        """
        SELECT platform_key, platform_name
        FROM dim_platform
        """
    )

    for platform_key, platform_name in cursor.fetchall():
        platform_map[platform_name] = platform_key

    cursor.execute(
        """
        SELECT
            product_key,
            source_system,
            source_product_id
        FROM dim_product
        WHERE source_system IN (
            'shopee',
            'lazada',
            'amazon'
        )
        """
    )

    product_map = {}

    for product_key, source_system, source_product_id in cursor.fetchall():
        product_map[
            (source_system, str(source_product_id))
        ] = product_key

    rows = []

    for row in df.itertuples(index=False):

        collected_at = pd.to_datetime(
            row.collected_at,
            errors="coerce"
        )

        if pd.isna(collected_at):
            continue

        platform = str(row.platform).lower()
        external_product_id = str(row.external_product_id)

        platform_key = platform_map.get(platform)

        product_key = product_map.get(
            (platform, external_product_id)
        )

        if platform_key is None or product_key is None:
            continue

        date_key = int(
            collected_at.strftime("%Y%m%d")
        )

        processed_at = getattr(
            row,
            "processed_at",
            None
        )

        rows.append(
            (
                date_key,
                product_key,
                platform_key,
                clean_value(getattr(row, "currency", None)),
                clean_value(getattr(row, "price", None)),
                clean_value(getattr(row, "original_price", None)),
                clean_value(getattr(row, "discount_percent", None)),
                clean_value(getattr(row, "rating", None)),
                clean_value(getattr(row, "review_count", None)),
                clean_value(getattr(row, "sold_count", None)),
                clean_value(getattr(row, "seller_name", None)),
                clean_value(getattr(row, "product_url", None)),
                clean_value(collected_at),
                clean_value(processed_at),
            )
        )

    execute_values(
        cursor,
        """
        INSERT INTO fact_external_product (
            date_key,
            product_key,
            platform_key,
            currency,
            price,
            original_price,
            discount_percent,
            rating,
            review_count,
            sold_count,
            seller_name,
            product_url,
            collected_at,
            processed_at
        )
        VALUES %s
        ON CONFLICT (
            date_key,
            product_key,
            platform_key
        )
        DO UPDATE SET
            currency = EXCLUDED.currency,
            price = EXCLUDED.price,
            original_price = EXCLUDED.original_price,
            discount_percent = EXCLUDED.discount_percent,
            rating = EXCLUDED.rating,
            review_count = EXCLUDED.review_count,
            sold_count = EXCLUDED.sold_count,
            seller_name = EXCLUDED.seller_name,
            product_url = EXCLUDED.product_url,
            collected_at = EXCLUDED.collected_at,
            processed_at = EXCLUDED.processed_at,
            loaded_at = CURRENT_TIMESTAMP
        """,
        rows,
        page_size=5000,
    )

    print(f"fact_external_product prepared : {len(rows):,}")


# ============================================================
# MAIN
# ============================================================

def main():
    df = read_processed_data()

    connection = None

    try:
        print("\nConnecting to Data Warehouse...")

        connection = psycopg2.connect(**DB_CONFIG)
        connection.autocommit = False

        cursor = connection.cursor()

        load_dim_date(cursor, df)
        load_dim_product(cursor, df)
        load_fact_external_product(cursor, df)

        connection.commit()

        print("\n" + "=" * 60)
        print("EXTERNAL DATA WAREHOUSE LOAD COMPLETED")
        print("=" * 60)

        cursor.execute(
            """
            SELECT platform_name, COUNT(*)
            FROM fact_external_product f
            JOIN dim_platform p
                ON f.platform_key = p.platform_key
            GROUP BY platform_name
            ORDER BY platform_name
            """
        )

        print("\nWarehouse rows by platform:")

        for platform, count in cursor.fetchall():
            print(f"{platform:10} : {count:,}")

        cursor.close()

    except Exception as error:

        if connection:
            connection.rollback()

        print("\nLOAD FAILED")
        print(error)

        raise

    finally:

        if connection:
            connection.close()


if __name__ == "__main__":
    main()