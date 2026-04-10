"""Cart discount logic — ported from prediosChile."""

DISCOUNT_TIERS = [
    (20, 0.15),  # 15% off for 20+ items
    (10, 0.10),  # 10% off for 10+ items
    (3, 0.05),   # 5% off for 3+ items
]


def calculate_cart_total(items: list[dict]) -> dict:
    """Calculate cart total with volume discount.

    Each item must have a 'precio' key.
    Returns: {subtotal, discount, discount_amount, total}
    """
    subtotal = sum(item.get("precio", 0) for item in items)
    count = len(items)

    discount = None
    for min_items, pct in DISCOUNT_TIERS:
        if count >= min_items:
            discount = {"min": min_items, "pct": pct}
            break

    discount_amount = int(subtotal * discount["pct"]) if discount else 0
    total = subtotal - discount_amount

    return {
        "subtotal": subtotal,
        "discount": discount,
        "discount_amount": discount_amount,
        "total": total,
    }
