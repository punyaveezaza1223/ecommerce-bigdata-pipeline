EXTERNAL_COLUMNS = [
    "platform",
    "external_product_id",
    "product_name",
    "category",
    "brand",
    "currency",
    "price",
    "original_price",
    "discount_percent",
    "rating",
    "review_count",
    "sold_count",
    "seller_name",
    "product_url",
    "image_url",
    "collected_at",
    "source_updated_at",
    "ingestion_timestamp",
    "raw_source",
]


def normalize_product(data):
    """
    Convert external marketplace data into the common external product schema.
    """

    return {
        column: data.get(column)
        for column in EXTERNAL_COLUMNS
    }


def validate_product(data):
    """
    Validate external marketplace product data.

    Price is optional because some marketplace APIs,
    such as StockX Catalog Search, provide product
    metadata without current market prices.
    """

    required_fields = [
        "platform",
        "external_product_id",
        "product_name",
    ]

    for field in required_fields:
        value = data.get(field)

        if value is None:
            return False

        if isinstance(value, str) and not value.strip():
            return False

    # Validate price only when a price is available.
    price = data.get("price")

    if price is not None:
        try:
            if float(price) < 0:
                return False
        except (TypeError, ValueError, OverflowError):
            return False

    return True