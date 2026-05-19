"""C-level data engineering case: Online retail order reconciliation.

Title: Online Retail Order Reconciliation and Customer Revenue Mart
Level: C-level engineering case
Suggested duration: 2-4 hours
Difficulty: Senior / Lead data engineering practice

Suggested public dataset:
- Dataset: UCI Online Retail
- Source: https://archive.ics.uci.edu/dataset/352/online+retail
- Why it fits: It contains real transactional e-commerce invoice lines with
  cancellations, returns, missing customer IDs, product descriptions, countries,
  quantities, prices, and invoice timestamps.
- Usage caveat: UCI datasets have dataset-specific citation and usage terms.
  Check the UCI page before using the full dataset in a portfolio or shared
  work. This challenge uses a compact simulated extract inspired by the schema.

Scenario:
You support the analytics platform for a small online retailer. Finance and
Growth currently disagree on monthly customer revenue because the source export
contains duplicate invoice lines, cancellation invoices, returns, missing
customer IDs, inconsistent product descriptions, and ambiguous order statuses.

The business wants a deterministic daily batch transform that turns raw invoice
line extracts into:
1. clean invoice line records,
2. rejected rows with reasons,
3. one order-level reconciliation table,
4. one customer-month revenue table.

This is hands-on engineering practice, not a trivia exercise. You should make
reasonable assumptions, implement the TODOs, and document tradeoffs where the
source data is ambiguous.

How to use the real dataset later:
After solving the embedded extract, download the UCI Online Retail Excel file,
map its columns to the field names used below, and run the same logic over the
full file. The main adaptations should be file loading, larger-data performance,
and extra profiling for unexpected values.
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any, Literal, TypedDict


# =========================
# CHALLENGE DESCRIPTION
# =========================

# Business context:
# - Finance needs recognized net revenue by customer and month.
# - Growth needs order counts and active customer revenue for segmentation.
# - Operations needs rejected rows and questionable orders to investigate.
#
# Source system:
# - A daily CSV-like export from an e-commerce platform.
# - Each row is an invoice line.
# - Cancellation invoices usually start with "C" and have negative quantities.
# - Returns may also appear as negative quantities on the original invoice.
# - The same invoice line can appear more than once after upstream retries.
#
# Business problem:
# Build a deterministic pure-Python transform that normalizes messy invoice
# lines, applies business rules, and produces reconciled order and customer
# revenue outputs suitable for downstream reporting.
#
# Keep the implementation in this file. Do not add a solution file.


# =========================
# SOURCE DATA
# =========================


class RawInvoiceLine(TypedDict, total=False):
    invoice_no: Any
    stock_code: Any
    description: Any
    quantity: Any
    invoice_date: Any
    unit_price: Any
    customer_id: Any
    country: Any
    source_file: Any
    source_row_number: Any


class CleanInvoiceLine(TypedDict):
    invoice_no: str
    stock_code: str
    description: str | None
    quantity: int
    invoice_date: str
    unit_price: Decimal
    customer_id: str | None
    country: str
    line_amount: Decimal
    line_type: Literal["sale", "return", "cancellation"]
    source_file: str
    source_row_number: int


class RejectedRow(TypedDict):
    row: RawInvoiceLine
    reason: str


class OrderSummary(TypedDict):
    invoice_no: str
    customer_id: str | None
    country: str
    order_date: str
    order_status: Literal["sale", "return", "cancellation", "mixed_review"]
    gross_sales_amount: Decimal
    return_amount: Decimal
    cancellation_amount: Decimal
    net_amount: Decimal
    line_count: int
    distinct_sku_count: int
    has_missing_customer: bool
    review_reason: str | None


class CustomerMonthRevenue(TypedDict):
    customer_id: str
    revenue_month: str
    country: str
    net_revenue: Decimal
    gross_sales_amount: Decimal
    return_amount: Decimal
    cancellation_amount: Decimal
    order_count: int
    return_order_count: int
    cancellation_order_count: int


RAW_INVOICE_LINES: list[RawInvoiceLine] = [
    {
        "invoice_no": " 536365 ",
        "stock_code": "85123A",
        "description": "WHITE HANGING HEART T-LIGHT HOLDER",
        "quantity": "6",
        "invoice_date": "2026-01-05 09:15",
        "unit_price": "2.55",
        "customer_id": "17850",
        "country": " United Kingdom ",
        "source_file": "retail_2026_01_05.csv",
        "source_row_number": 1,
    },
    {
        "invoice_no": "536365",
        "stock_code": "71053",
        "description": "WHITE METAL LANTERN",
        "quantity": 6,
        "invoice_date": "2026-01-05 09:15",
        "unit_price": "3.39",
        "customer_id": "17850",
        "country": "United Kingdom",
        "source_file": "retail_2026_01_05.csv",
        "source_row_number": 2,
    },
    {
        # Duplicate retry of row 2. The transform should keep one copy.
        "invoice_no": "536365",
        "stock_code": "71053",
        "description": "White metal lantern",
        "quantity": "6",
        "invoice_date": "2026-01-05 09:15",
        "unit_price": "3.390",
        "customer_id": "17850",
        "country": "United Kingdom",
        "source_file": "retail_2026_01_05_retry.csv",
        "source_row_number": 2,
    },
    {
        "invoice_no": "536366",
        "stock_code": "84406B",
        "description": "CREAM CUPID HEARTS COAT HANGER",
        "quantity": "8",
        "invoice_date": "2026-01-05 10:03",
        "unit_price": "2.75",
        "customer_id": "",
        "country": "United Kingdom",
        "source_file": "retail_2026_01_05.csv",
        "source_row_number": 3,
    },
    {
        "invoice_no": "C536367",
        "stock_code": "85123A",
        "description": "WHITE HANGING HEART T-LIGHT HOLDER",
        "quantity": "-2",
        "invoice_date": "2026-01-06 11:20",
        "unit_price": "2.55",
        "customer_id": "17850",
        "country": "United Kingdom",
        "source_file": "retail_2026_01_06.csv",
        "source_row_number": 1,
    },
    {
        "invoice_no": "536368",
        "stock_code": "POST",
        "description": "POSTAGE",
        "quantity": "1",
        "invoice_date": "2026-01-06 12:00",
        "unit_price": "18.00",
        "customer_id": "13047",
        "country": "Germany",
        "source_file": "retail_2026_01_06.csv",
        "source_row_number": 2,
    },
    {
        "invoice_no": "536368",
        "stock_code": "22752",
        "description": "SET 7 BABUSHKA NESTING BOXES",
        "quantity": "-1",
        "invoice_date": "2026-01-06 12:00",
        "unit_price": "7.65",
        "customer_id": "13047",
        "country": "Germany",
        "source_file": "retail_2026_01_06.csv",
        "source_row_number": 3,
    },
    {
        "invoice_no": "536369",
        "stock_code": "21730",
        "description": "GLASS STAR FROSTED T-LIGHT HOLDER",
        "quantity": "bad",
        "invoice_date": "2026-01-06 13:10",
        "unit_price": "4.25",
        "customer_id": "12583",
        "country": "France",
        "source_file": "retail_2026_01_06.csv",
        "source_row_number": 4,
    },
    {
        "invoice_no": "536370",
        "stock_code": "22728",
        "description": "ALARM CLOCK BAKELIKE PINK",
        "quantity": "24",
        "invoice_date": "not-a-date",
        "unit_price": "3.75",
        "customer_id": "12583",
        "country": "France",
        "source_file": "retail_2026_01_06.csv",
        "source_row_number": 5,
    },
    {
        "invoice_no": "536371",
        "stock_code": "22727",
        "description": "ALARM CLOCK BAKELIKE RED",
        "quantity": "12",
        "invoice_date": "2026-02-01 08:30",
        "unit_price": "0",
        "customer_id": "12583",
        "country": "France",
        "source_file": "retail_2026_02_01.csv",
        "source_row_number": 1,
    },
    {
        "invoice_no": "536372",
        "stock_code": "22726",
        "description": "ALARM CLOCK BAKELIKE GREEN",
        "quantity": "12",
        "invoice_date": "2026-02-01 08:45",
        "unit_price": "3.75",
        "customer_id": "12583",
        "country": "France",
        "source_file": "retail_2026_02_01.csv",
        "source_row_number": 2,
    },
]


# =========================
# BUSINESS RULES
# =========================

# Normalization rules:
# - Strip string fields.
# - invoice_no, stock_code, quantity, invoice_date, unit_price, and country are
#   required.
# - customer_id may be blank; normalize blank customer_id to None.
# - description may be blank; normalize blank description to None.
# - quantity must parse as an integer and cannot be zero.
# - unit_price must parse as Decimal and must be greater than zero.
# - invoice_date must parse from "YYYY-MM-DD HH:MM".
# - Store canonical invoice_date as "YYYY-MM-DDTHH:MM:00".
# - Store country in title case, for example "United Kingdom".
# - line_amount = quantity * unit_price.
#
# Classification rules:
# - line_type is "cancellation" when invoice_no starts with "C".
# - line_type is "return" when quantity is negative and invoice_no does not
#   start with "C".
# - line_type is "sale" when quantity is positive and invoice_no does not start
#   with "C".
# - A cancellation invoice with positive quantity is invalid.
# - A sale invoice with positive and negative lines is valid but should produce
#   an order_status of "mixed_review".
#
# Deduplication rules:
# - Exact source retries should collapse before aggregation.
# - Treat rows as duplicates when invoice_no, stock_code, quantity, invoice_date,
#   unit_price, customer_id, and country match after normalization.
# - If duplicates differ only by description/source metadata, keep the row with
#   the highest source_file lexicographically, then highest source_row_number.
# - Output order must be deterministic.
#
# Aggregation rules:
# - Gross sales amount is the sum of positive sale line amounts.
# - Return amount is the absolute value of return line amounts.
# - Cancellation amount is the absolute value of cancellation line amounts.
# - Net amount = gross_sales_amount - return_amount - cancellation_amount.
# - Order date is the calendar date of the earliest invoice_date in the invoice.
# - distinct_sku_count counts unique stock_code values after deduplication.
# - has_missing_customer is true when any line in the order has customer_id None.
# - Customer-month revenue excludes orders with missing customer_id.


# =========================
# MESSY DATA CONDITIONS
# =========================

# This sample intentionally includes:
# - whitespace around business keys,
# - duplicate retry lines with slightly different descriptions,
# - missing customer IDs,
# - cancellation invoices,
# - a negative return line on a non-cancellation invoice,
# - invalid quantity,
# - invalid timestamp,
# - zero price,
# - mixed order status requiring review.


# =========================
# IMPLEMENTATION AREA
# =========================


def parse_invoice_datetime(value: Any) -> datetime:
    """Parse invoice_date from the source contract.

    TODO:
    - Accept only the "YYYY-MM-DD HH:MM" format after stripping.
    - Raise ValueError("invalid_invoice_date") for invalid values.
    """
    raise NotImplementedError


def parse_decimal_price(value: Any) -> Decimal:
    """Parse and validate unit_price.

    TODO:
    - Strip and parse with Decimal.
    - Reject missing, invalid, zero, or negative prices.
    - Raise ValueError("invalid_unit_price") for invalid values.
    """
    raise NotImplementedError


def parse_quantity(value: Any) -> int:
    """Parse and validate quantity.

    TODO:
    - Strip and parse as int.
    - Reject missing, non-integer, and zero quantities.
    - Raise ValueError("invalid_quantity") for invalid values.
    """
    raise NotImplementedError


def normalize_line(raw: RawInvoiceLine) -> CleanInvoiceLine:
    """Normalize one raw source row.

    TODO:
    - Apply all normalization and classification rules.
    - Preserve source_file and source_row_number for traceability.
    - Raise ValueError with a clear reason, such as:
      "missing_invoice_no", "missing_stock_code", "invalid_quantity",
      "invalid_invoice_date", "invalid_unit_price", or
      "invalid_cancellation_quantity".
    """
    raise NotImplementedError


def normalize_batch(
    raw_rows: list[RawInvoiceLine],
) -> tuple[list[CleanInvoiceLine], list[RejectedRow]]:
    """Normalize the batch and collect rejected rows.

    TODO:
    - Continue after bad rows instead of failing the whole batch.
    - Return valid rows and rejected rows.
    - Keep the original raw row in RejectedRow.
    """
    raise NotImplementedError


def deduplicate_lines(rows: list[CleanInvoiceLine]) -> list[CleanInvoiceLine]:
    """Remove duplicate source retry lines.

    TODO:
    - Implement the deduplication contract from BUSINESS RULES.
    - Return rows in deterministic order by invoice_no, stock_code,
      invoice_date, source_file, and source_row_number.
    """
    raise NotImplementedError


def build_order_summaries(rows: list[CleanInvoiceLine]) -> list[OrderSummary]:
    """Aggregate clean, deduplicated lines to one row per invoice_no.

    TODO:
    - Apply the aggregation rules.
    - Mark mixed positive/negative non-cancellation orders as "mixed_review".
    - Use review_reason "contains_sale_and_return_lines" for mixed_review.
    - Return deterministic order by invoice_no.
    """
    raise NotImplementedError


def build_customer_month_revenue(
    orders: list[OrderSummary],
) -> list[CustomerMonthRevenue]:
    """Aggregate order summaries into customer-month revenue rows.

    TODO:
    - Exclude orders with missing customer_id.
    - Derive revenue_month as "YYYY-MM".
    - Count orders by final order_status.
    - Return deterministic order by customer_id and revenue_month.
    """
    raise NotImplementedError


def run_pipeline(
    raw_rows: list[RawInvoiceLine],
) -> tuple[
    list[CleanInvoiceLine],
    list[RejectedRow],
    list[OrderSummary],
    list[CustomerMonthRevenue],
]:
    """Run the full case pipeline."""
    normalized, rejected = normalize_batch(raw_rows)
    deduped = deduplicate_lines(normalized)
    orders = build_order_summaries(deduped)
    customer_month = build_customer_month_revenue(orders)
    return deduped, rejected, orders, customer_month


# =========================
# EXPECTED OUTPUTS
# =========================

# Your completed implementation should produce:
# - 7 clean deduplicated invoice lines.
# - 3 rejected rows.
# - 5 order summaries.
# - 3 customer-month revenue rows.
#
# Key expected business results:
# - Invoice 536365 net_amount is Decimal("35.64").
# - Invoice C536367 is a cancellation with net_amount Decimal("-5.10").
# - Invoice 536368 is mixed_review with net_amount Decimal("10.35").
# - Customer 17850 has January net_revenue Decimal("30.54").
# - Customer 12583 has February net_revenue Decimal("45.00").


# =========================
# ACCEPTANCE CRITERIA
# =========================

# A correct solution should:
# - Reject bad rows without stopping the whole batch.
# - Use Decimal for money, not float.
# - Preserve line-level traceability fields.
# - Produce deterministic output order.
# - Deduplicate retry rows before aggregation.
# - Separate sales, returns, cancellations, and mixed-review orders correctly.
# - Exclude missing-customer orders from customer-month revenue.
# - Include tests or assertions for the important edge cases.
# - Keep assumptions explicit in the WRITTEN ANSWERS section.


# =========================
# WRITTEN ANSWERS
# =========================

# Answer these after implementing:
#
# 1. Which assumptions did you make about cancellations versus returns?
#
# 2. How would you validate that the customer-month revenue agrees with Finance?
#
# 3. What data quality metrics would you publish for this pipeline?
#
# 4. If the upstream source later adds currency, how would your model change?
#
# 5. Which parts of this pure-Python implementation would map to Spark SQL or
#    PySpark transformations on the full UCI dataset?


# =========================
# PRODUCTION FOLLOW-UP QUESTIONS
# =========================

# Be ready to discuss:
#
# 1. Scale: How would you partition the full dataset and avoid skew on large
#    customers or invoice numbers?
#
# 2. Reliability: How would you make the daily job retry-safe and idempotent?
#
# 3. Observability: Which counters, sample rejects, and reconciliation totals
#    should be emitted on every run?
#
# 4. Backfills: How would you rerun six months of data without double-counting?
#
# 5. Schema evolution: How would you handle new columns, renamed columns, or
#    changed timestamp formats?
#
# 6. Lineage and ownership: Who owns definitions like net revenue, return
#    amount, and cancellation amount?


# =========================
# RELATED A-LEVEL DRILLS
# =========================

# Useful micro-drills before or after this case:
#
# - Parse strict datetime strings with datetime.strptime.
# - Parse Decimal values and reject invalid money fields.
# - Group dictionaries by a composite key.
# - Deduplicate records with a deterministic tie-breaker.
# - Aggregate signed amounts into gross, return, cancellation, and net amounts.
# - Write pytest assertions for rejected-row reasons.


# =========================
# REVIEW RUBRIC
# =========================

# Strict review dimensions:
#
# 1. Domain understanding: correctly explains sales, returns, cancellations,
#    and missing-customer impact.
# 2. Correctness: implements all business rules exactly and handles edge cases.
# 3. Data modeling: produces useful line, order, and customer-month outputs.
# 4. Data quality handling: rejects bad records with clear, actionable reasons.
# 5. Determinism: stable deduplication and output ordering.
# 6. Money handling: uses Decimal and avoids float rounding errors.
# 7. Traceability: keeps enough source metadata to investigate records.
# 8. Tests: covers happy paths, rejects, duplicates, mixed orders, and revenue.
# 9. Production reasoning: discusses idempotency, backfills, monitoring, and
#    schema evolution pragmatically.
# 10. Communication: written answers are concise, explicit, and business-aware.


# =========================
# SELF-CHECK / OPTIONAL TESTS
# =========================


def test_pipeline_shape() -> None:
    deduped, rejected, orders, customer_month = run_pipeline(RAW_INVOICE_LINES)

    assert len(deduped) == 7
    assert len(rejected) == 3
    assert len(orders) == 5
    assert len(customer_month) == 3


def test_rejection_reasons() -> None:
    _, rejected, _, _ = run_pipeline(RAW_INVOICE_LINES)
    reasons = sorted(row["reason"] for row in rejected)

    assert reasons == [
        "invalid_invoice_date",
        "invalid_quantity",
        "invalid_unit_price",
    ]


def test_order_reconciliation_amounts() -> None:
    _, _, orders, _ = run_pipeline(RAW_INVOICE_LINES)
    by_invoice = {row["invoice_no"]: row for row in orders}

    assert by_invoice["536365"]["net_amount"] == Decimal("35.64")
    assert by_invoice["C536367"]["order_status"] == "cancellation"
    assert by_invoice["C536367"]["net_amount"] == Decimal("-5.10")
    assert by_invoice["536368"]["order_status"] == "mixed_review"
    assert by_invoice["536368"]["review_reason"] == "contains_sale_and_return_lines"
    assert by_invoice["536368"]["net_amount"] == Decimal("10.35")


def test_customer_month_revenue() -> None:
    _, _, _, customer_month = run_pipeline(RAW_INVOICE_LINES)
    by_key = {
        (row["customer_id"], row["revenue_month"]): row
        for row in customer_month
    }

    assert by_key[("17850", "2026-01")]["net_revenue"] == Decimal("30.54")
    assert by_key[("13047", "2026-01")]["net_revenue"] == Decimal("10.35")
    assert by_key[("12583", "2026-02")]["net_revenue"] == Decimal("45.00")


if __name__ == "__main__":
    clean_lines, rejected_rows, order_rows, customer_rows = run_pipeline(
        RAW_INVOICE_LINES
    )
    print("Clean lines:", len(clean_lines))
    print("Rejected rows:", len(rejected_rows))
    print("Orders:", len(order_rows))
    print("Customer-month rows:", len(customer_rows))
