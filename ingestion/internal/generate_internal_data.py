import random
import os
from datetime import datetime, timedelta

import psycopg2
from faker import Faker
from psycopg2.extras import execute_values


fake = Faker("en_US")

DB_CONFIG = {
    "host": os.getenv("INTERNAL_POSTGRES_HOST", "localhost").strip(),
    "port": int(os.getenv("INTERNAL_POSTGRES_PORT", "5451")),
    "database": os.getenv("INTERNAL_POSTGRES_DB", "ecommerce_internal").strip(),
    "user": os.getenv("INTERNAL_POSTGRES_USER", "ecommerce").strip(),
    "password": required_env("INTERNAL_POSTGRES_PASSWORD"),
}

# ------------------------------------------------------------
# จำนวนข้อมูลสำหรับ TEST ก่อน
# ------------------------------------------------------------
NUM_PRODUCTS = 1000
NUM_CUSTOMERS = 5000
NUM_ORDERS = 10000

BATCH_SIZE = 5000


CATEGORIES = [
    "Electronics",
    "Fashion",
    "Beauty",
    "Home",
    "Sports",
    "Books",
    "Toys",
    "Food",
]

BRANDS = [
    "Brand_A",
    "Brand_B",
    "Brand_C",
    "Brand_D",
    "Brand_E",
    "Brand_F",
    "Brand_G",
    "Brand_H",
]

PROVINCES = [
    ("Bangkok", "Central"),
    ("Chiang Mai", "North"),
    ("Chiang Rai", "North"),
    ("Khon Kaen", "Northeast"),
    ("Nakhon Ratchasima", "Northeast"),
    ("Chonburi", "East"),
    ("Rayong", "East"),
    ("Phuket", "South"),
    ("Songkhla", "South"),
    ("Nakhon Pathom", "Central"),
]

PAYMENT_METHODS = [
    "credit_card",
    "bank_transfer",
    "e_wallet",
    "cash_on_delivery",
]

SALES_CHANNELS = [
    "website",
    "mobile_app",
]

ORDER_STATUSES = [
    "completed",
    "completed",
    "completed",
    "completed",
    "cancelled",
]


def get_connection():
    return psycopg2.connect(**DB_CONFIG)


def generate_products(conn):
    print("Generating products...")

    rows = []

    for i in range(1, NUM_PRODUCTS + 1):
        cost = round(random.uniform(50, 5000), 2)
        selling_price = round(cost * random.uniform(1.10, 1.80), 2)

        rows.append(
            (
                f"SKU-{i:07d}",
                f"Product {i}",
                random.choice(CATEGORIES),
                random.choice(BRANDS),
                cost,
                selling_price,
                random.randint(0, 1000),
            )
        )

    sql = """
        INSERT INTO products (
            sku,
            product_name,
            category,
            brand,
            cost_price,
            selling_price,
            stock_quantity
        )
        VALUES %s
        ON CONFLICT (sku) DO NOTHING
    """

    with conn.cursor() as cursor:
        execute_values(cursor, sql, rows, page_size=BATCH_SIZE)

    conn.commit()

    print(f"Products completed: {NUM_PRODUCTS:,}")


def generate_customers(conn):
    print("Generating customers...")

    rows = []

    for i in range(1, NUM_CUSTOMERS + 1):
        province, region = random.choice(PROVINCES)

        rows.append(
            (
                f"CUST-{i:08d}",
                random.choice(["Male", "Female", "Other"]),
                random.randint(18, 70),
                province,
                region,
                fake.date_time_between(
                    start_date="-5y",
                    end_date="now",
                ),
            )
        )

    sql = """
        INSERT INTO customers (
            customer_code,
            gender,
            age,
            province,
            region,
            registered_at
        )
        VALUES %s
        ON CONFLICT (customer_code) DO NOTHING
    """

    with conn.cursor() as cursor:
        execute_values(cursor, sql, rows, page_size=BATCH_SIZE)

    conn.commit()

    print(f"Customers completed: {NUM_CUSTOMERS:,}")


def get_ids(conn):
    with conn.cursor() as cursor:
        cursor.execute("SELECT product_id, selling_price FROM products")
        products = cursor.fetchall()

        cursor.execute("SELECT customer_id FROM customers")
        customers = [row[0] for row in cursor.fetchall()]

    return products, customers


def generate_orders(conn):
    print("Generating orders and order items...")

    products, customers = get_ids(conn)

    if not products:
        raise RuntimeError("No products found.")

    if not customers:
        raise RuntimeError("No customers found.")

    start_date = datetime.now() - timedelta(days=730)

    for batch_start in range(1, NUM_ORDERS + 1, BATCH_SIZE):

        batch_end = min(
            batch_start + BATCH_SIZE,
            NUM_ORDERS + 1,
        )

        order_rows = []

        # ----------------------------------------------------
        # สร้าง Orders
        # ----------------------------------------------------
        for i in range(batch_start, batch_end):

            order_date = start_date + timedelta(
                seconds=random.randint(0, 730 * 24 * 60 * 60)
            )

            order_rows.append(
                (
                    f"ORD-{i:010d}",
                    random.choice(customers),
                    order_date,
                    random.choice(PAYMENT_METHODS),
                    random.choice(SALES_CHANNELS),
                    random.choice(ORDER_STATUSES),
                    0,
                )
            )

        order_sql = """
            INSERT INTO orders (
                order_number,
                customer_id,
                order_date,
                payment_method,
                sales_channel,
                order_status,
                total_amount
            )
            VALUES %s
            ON CONFLICT (order_number) DO NOTHING
            RETURNING order_id, order_number
        """

        with conn.cursor() as cursor:

            inserted_orders = execute_values(
                cursor,
                order_sql,
                order_rows,
                page_size=BATCH_SIZE,
                fetch=True,
            )

            item_rows = []
            order_totals = {}

            # ------------------------------------------------
            # สร้าง Order Items
            # ------------------------------------------------
            for order_id, order_number in inserted_orders:

                number_of_items = random.randint(1, 5)

                selected_products = random.sample(
                    products,
                    k=min(number_of_items, len(products)),
                )

                total = 0

                for product_id, selling_price in selected_products:

                    quantity = random.randint(1, 3)

                    unit_price = float(selling_price)

                    discount = round(
                        unit_price * random.choice(
                            [0, 0, 0, 0.05, 0.10]
                        ),
                        2,
                    )

                    line_total = round(
                        (unit_price - discount) * quantity,
                        2,
                    )

                    total += line_total

                    item_rows.append(
                        (
                            order_id,
                            product_id,
                            quantity,
                            unit_price,
                            discount,
                            line_total,
                        )
                    )

                order_totals[order_id] = round(total, 2)

            item_sql = """
                INSERT INTO order_items (
                    order_id,
                    product_id,
                    quantity,
                    unit_price,
                    discount_amount,
                    line_total
                )
                VALUES %s
            """

            if item_rows:
                execute_values(
                    cursor,
                    item_sql,
                    item_rows,
                    page_size=BATCH_SIZE,
                )

            # ------------------------------------------------
            # Update total_amount ของแต่ละ Order
            # ------------------------------------------------
            if order_totals:

                update_rows = [
                    (total, order_id)
                    for order_id, total in order_totals.items()
                ]

                execute_values(
                    cursor,
                    """
                    UPDATE orders AS o
                    SET total_amount = v.total_amount
                    FROM (VALUES %s)
                        AS v(total_amount, order_id)
                    WHERE o.order_id = v.order_id
                    """,
                    update_rows,
                    template="(%s::numeric, %s::bigint)",
                    page_size=BATCH_SIZE,
                )

        conn.commit()

        print(
            f"Orders processed: "
            f"{batch_end - 1:,}/{NUM_ORDERS:,}"
        )

    print("Orders and order items completed.")


def show_summary(conn):

    print("\n==============================")
    print("INTERNAL DATA SUMMARY")
    print("==============================")

    tables = [
        "products",
        "customers",
        "orders",
        "order_items",
    ]

    with conn.cursor() as cursor:

        for table in tables:

            cursor.execute(
                f"SELECT COUNT(*) FROM {table}"
            )

            count = cursor.fetchone()[0]

            print(
                f"{table:<15} : {count:,}"
            )


def main():

    conn = get_connection()

    try:
        generate_products(conn)
        generate_customers(conn)
        generate_orders(conn)
        show_summary(conn)

    finally:
        conn.close()


if __name__ == "__main__":
    main()
