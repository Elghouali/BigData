from decimal import Decimal
import pytest
from src.pipeline import RAW_SCHEMA, create_spark, classify_orders, aggregate_revenue


@pytest.fixture(scope="module")
def spark():
    session = create_spark()
    yield session
    session.stop()


def test_rejects_and_deduplicates(spark):
    rows = [
        ("1", "2026-11-01", "A", "Rabat", "Books", "2", "10.00", "paid"),
        ("1", "2026-11-01", "A", "Rabat", "Books", "2", "10.00", "paid"),
        ("2", "2026-11-01", "B", "Rabat", "Books", "1", "6.00", "cancelled"),
        ("3", "2026-11-01", None, "Rabat", "Books", "1", "6.00", "paid"),
        ("4", "2026-11-01", "D", "Rabat", "Books", "-1", "3.00", "paid"),
    ]
    valid, rejected = classify_orders(spark.createDataFrame(rows, RAW_SCHEMA))
    assert valid.count() == 2
    assert rejected.count() == 2
    assert {x.reject_reason for x in rejected.collect()} == {"missing_customer", "invalid_quantity"}


def test_cancelled_orders_excluded(spark):
    rows = [
        ("1", "2026-11-01", "A", "Rabat", "Books", "2", "10.00", "paid"),
        ("2", "2026-11-01", "B", "Rabat", "Books", "5", "100.00", "cancelled"),
    ]
    valid, rejected = classify_orders(spark.createDataFrame(rows, RAW_SCHEMA))
    result = aggregate_revenue(valid).collect()
    assert rejected.count() == 0
    assert len(result) == 1
    assert Decimal(str(result[0].revenue_mad)) == Decimal("20.00")
    assert result[0].paid_orders == 1


def test_invalid_status(spark):
    rows = [("9", "2026-11-01", "A", "Rabat", "Books", "2", "5.00", "refunded")]
    valid, rejected = classify_orders(spark.createDataFrame(rows, RAW_SCHEMA))
    assert valid.count() == 0
    assert rejected.first().reject_reason == "invalid_status"
