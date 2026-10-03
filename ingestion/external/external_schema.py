EXTERNAL_COLUMNS = [
    "platform",
    "external_product_id",
    "product_name",
    "category",
    "brand",
    "price",
    "original_price",
    "discount_percent",
    "rating",
    "review_count",
    "sold_count",
    "seller_name",
    "product_url",
    "collected_at",
]


def normalize_product(data):
    """
    Convert product data from Shopee / Lazada / Amazon
    into the common external product schema.
    """

    return {
        "platform": data.get("platform"),
        "external_product_id": data.get("external_product_id"),
        "product_name": data.get("product_name"),
        "category": data.get("category"),
        "brand": data.get("brand"),
        "price": data.get("price"),
        "original_price": data.get("original_price"),
        "discount_percent": data.get("discount_percent"),
        "rating": data.get("rating"),
        "review_count": data.get("review_count"),
        "sold_count": data.get("sold_count"),
        "seller_name": data.get("seller_name"),
        "product_url": data.get("product_url"),
        "collected_at": data.get("collected_at"),
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