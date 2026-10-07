import psycopg2
import os
from psycopg2.extras import execute_values


# ============================================================
# INTERNAL DATABASE
# ============================================================

INTERNAL_DB = {
    "host": os.getenv("INTERNAL_POSTGRES_HOST", "localhost").strip(),
    "port": int(os.getenv("INTERNAL_POSTGRES_PORT", "5451")),
    "database": os.getenv("INTERNAL_POSTGRES_DB", "ecommerce_internal").strip(),
    "user": os.getenv("INTERNAL_POSTGRES_USER", "ecommerce").strip(),
    "password": required_env("INTERNAL_POSTGRES_PASSWORD"),
}


# ============================================================
# DATA WAREHOUSE
# ============================================================

WAREHOUSE_DB = {
    "host": os.getenv("POSTGRES_HOST", "localhost").strip(),
    "port": int(os.getenv("POSTGRES_PORT", "5452")),
    "database": required_env("POSTGRES_DB"),
    "user": required_env("POSTGRES_USER"),
    "password": required_env("POSTGRES_PASSWORD"),
}


# ============================================================
# LOAD DIM_DATE
# ============================================================

def load_dim_date(source_cursor, warehouse_cursor):

    print("\nLoading dim_date...")

    source_cursor.execute("""
        SELECT DISTINCT order_date::date
        FROM orders
        WHERE order_date IS NOT NULL
        ORDER BY order_date::date
    """)

    dates = source_cursor.fetchall()

    rows = []

    for (date_value,) in dates:

        date_key = int(date_value.strftime("%Y%m%d"))

        rows.append(
            (
                date_key,
                date_value,
                date_value.day,
                date_value.month,
                date_value.strftime("%B"),
                ((date_value.month - 1) // 3) + 1,
                date_value.year,
                date_value.isoweekday(),
                date_value.strftime("%A"),
                date_value.weekday() >= 5,
            )
        )

    execute_values(
        warehouse_cursor,
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
        page_size=5000,
    )

    print(f"dim_date prepared : {len(rows):,}")


# ============================================================
# LOAD DIM_PRODUCT
# ============================================================

def load_dim_product(source_cursor, warehouse_cursor):

    print("Loading dim_product...")

    source_cursor.execute("""
        SELECT
            product_id,
            sku,
            product_name,
            category,
            brand
        FROM products
        ORDER BY product_id
    """)

    products = source_cursor.fetchall()

    rows = []

    for product_id, sku, product_name, category, brand in products:

        rows.append(
            (
                "internal",
                str(product_id),
                product_name,
                category,
                brand,
                sku,
            )
        )

    execute_values(
        warehouse_cursor,
        """
        INSERT INTO dim_product (
            source_system,
            source_product_id,
            product_name,
            category,
            brand,
            sku
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
            sku = EXCLUDED.sku,
            updated_at = CURRENT_TIMESTAMP
        """,
        rows,
        page_size=5000,
    )

    print(f"dim_product prepared : {len(rows):,}")


# ============================================================
# LOAD DIM_CUSTOMER
# ============================================================

def load_dim_customer(source_cursor, warehouse_cursor):

    print("Loading dim_customer...")

    source_cursor.execute("""
        SELECT
            customer_id,
            customer_code,
            gender,
            age,
            province,
            region,
            registered_at
        FROM customers
        ORDER BY customer_id
    """)

    customers = source_cursor.fetchall()

    rows = []

    for row in customers:
        rows.append(row)

    execute_values(
        warehouse_cursor,
        """
        INSERT INTO dim_customer (
            source_customer_id,
            customer_code,
            gender,
            age,
            province,
            region,
            registered_at
        )
        VALUES %s
        ON CONFLICT (source_customer_id)
        DO UPDATE SET
            customer_code = EXCLUDED.customer_code,
            gender = EXCLUDED.gender,
            age = EXCLUDED.age,
            province = EXCLUDED.province,
            region = EXCLUDED.region,
            registered_at = EXCLUDED.registered_at
        """,
        rows,
        page_size=5000,
    )

    print(f"dim_customer prepared : {len(rows):,}")


# ============================================================
# LOAD FACT_SALES
# ============================================================

def load_fact_sales(source_cursor, warehouse_cursor):

    print("Loading fact_sales...")

    # Get internal platform key
    warehouse_cursor.execute("""
        SELECT platform_key
        FROM dim_platform
        WHERE platform_name = 'internal'
    """)

    result = warehouse_cursor.fetchone()

    if result is None:
        raise ValueError(
            "Platform 'internal' not found in dim_platform."
        )

    platform_key = result[0]

    # Product mapping
    warehouse_cursor.execute("""
        SELECT
            product_key,
            source_product_id
        FROM dim_product
        WHERE source_system = 'internal'
    """)

    product_map = {
        str(source_product_id): product_key
        for product_key, source_product_id
        in warehouse_cursor.fetchall()
    }

    # Customer mapping
    warehouse_cursor.execute("""
        SELECT
            customer_key,
            source_customer_id
        FROM dim_customer
    """)

    customer_map = {
        source_customer_id: customer_key
        for customer_key, source_customer_id
        in warehouse_cursor.fetchall()
    }

    # Read order item grain
    source_cursor.execute("""
        SELECT
            o.order_id,
            o.order_number,
            o.customer_id,
            o.order_date,
            o.payment_method,
            o.sales_channel,
            o.order_status,

            oi.product_id,
            oi.quantity,
            oi.unit_price,
            oi.discount_amount,
            oi.line_total

        FROM orders o

        JOIN order_items oi
            ON o.order_id = oi.order_id

        ORDER BY
            o.order_id,
            oi.order_item_id
    """)

    sales = source_cursor.fetchall()

    rows = []

    skipped = 0

    for (
        order_id,
        order_number,
        customer_id,
        order_date,
        payment_method,
        sales_channel,
        order_status,
        product_id,
        quantity,
        unit_price,
        discount_amount,
        line_total,
    ) in sales:

        product_key = product_map.get(str(product_id))
        customer_key = customer_map.get(customer_id)

        if product_key is None:
            skipped += 1
            continue

        date_key = int(order_date.strftime("%Y%m%d"))

        rows.append(
            (
                order_id,
                order_number,
                date_key,
                product_key,
                customer_key,
                platform_key,
                quantity,
                unit_price,
                discount_amount,
                line_total,
                payment_method,
                sales_channel,
                order_status,
            )
        )

    execute_values(
        warehouse_cursor,
        """
        INSERT INTO fact_sales (
            order_id,
            order_number,
            date_key,
            product_key,
            customer_key,
            platform_key,
            quantity,
            unit_price,
            discount_amount,
            line_total,
            payment_method,
            sales_channel,
            order_status
        )
        VALUES %s

        ON CONFLICT (
            order_id,
            product_key
        )
        DO UPDATE SET
            order_number = EXCLUDED.order_number,
            date_key = EXCLUDED.date_key,
            customer_key = EXCLUDED.customer_key,
            platform_key = EXCLUDED.platform_key,
            quantity = EXCLUDED.quantity,
            unit_price = EXCLUDED.unit_price,
            discount_amount = EXCLUDED.discount_amount,
            line_total = EXCLUDED.line_total,
            payment_method = EXCLUDED.payment_method,
            sales_channel = EXCLUDED.sales_channel,
            order_status = EXCLUDED.order_status,
            loaded_at = CURRENT_TIMESTAMP
        """,
        rows,
        page_size=5000,
    )

    print(f"fact_sales prepared : {len(rows):,}")
    print(f"fact_sales skipped  : {skipped:,}")


# ============================================================
# SHOW SUMMARY
# ============================================================

def show_summary(cursor):

    print("\n" + "=" * 60)
    print("WAREHOUSE SUMMARY")
    print("=" * 60)

    tables = [
        "dim_date",
        "dim_platform",
        "dim_product",
        "dim_customer",
        "fact_sales",
        "fact_external_product",
    ]

    for table in tables:

        cursor.execute(
            f"SELECT COUNT(*) FROM {table}"
        )

        count = cursor.fetchone()[0]

        print(f"{table:25} : {count:,}")


# ============================================================
# MAIN
# ============================================================

def main():

    source_connection = None
    warehouse_connection = None

    try:

        print("=" * 60)
        print("INTERNAL DATA WAREHOUSE LOAD")
        print("=" * 60)

        print("\nConnecting to Internal PostgreSQL...")

        source_connection = psycopg2.connect(
            **INTERNAL_DB
        )

        print("Connecting to Warehouse PostgreSQL...")

        warehouse_connection = psycopg2.connect(
            **WAREHOUSE_DB
        )

        warehouse_connection.autocommit = False

        source_cursor = source_connection.cursor()
        warehouse_cursor = warehouse_connection.cursor()

        load_dim_date(
            source_cursor,
            warehouse_cursor
        )

        load_dim_product(
            source_cursor,
            warehouse_cursor
        )

        load_dim_customer(
            source_cursor,
            warehouse_cursor
        )

        load_fact_sales(
            source_cursor,
            warehouse_cursor
        )

        warehouse_connection.commit()

        print("\n" + "=" * 60)
        print("INTERNAL DATA WAREHOUSE LOAD COMPLETED")
        print("=" * 60)

        show_summary(warehouse_cursor)

        source_cursor.close()
        warehouse_cursor.close()

    except Exception as error:

        if warehouse_connection:
            warehouse_connection.rollback()

        print("\nLOAD FAILED")
        print(error)

        raise

    finally:

        if source_connection:
            source_connection.close()

        if warehouse_connection:
            warehouse_connection.close()


if __name__ == "__main__":
    main()
