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
        "platform": data.get("platform"),
        "external_product_id": data.get("external_product_id"),
        "product_name": data.get("product_name"),
        "category": data.get("category"),
        "brand": data.get("brand"),
        "currency": data.get("currency"),
        "price": data.get("price"),
        "original_price": data.get("original_price"),
        "discount_percent": data.get("discount_percent"),
        "rating": data.get("rating"),
        "review_count": data.get("review_count"),
        "sold_count": data.get("sold_count"),
        "seller_name": data.get("seller_name"),
        "product_url": data.get("product_url"),
        "image_url": data.get("image_url"),
        "collected_at": data.get("collected_at"),
        "source_updated_at": data.get("source_updated_at"),
        "ingestion_timestamp": data.get("ingestion_timestamp"),
        "raw_source": data.get("raw_source"),
    }


def validate_product(data):
    """
    Basic validation before writing product data to Raw Data Lake.
    """

    required_fields = [
        "platform",
        "external_product_id",
        "product_name",
        "price",
    ]

    for field in required_fields:
        if data.get(field) is None:
            return False

    try:
        if float(data["price"]) < 0:
            return False
    except (TypeError, ValueError):
        return False

    return True
