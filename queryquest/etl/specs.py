"""Declarative description of every load.

One `TableSpec` per target table. The loader in `loader.py` is generic:
it reads these specs and builds the staging DDL, the dedup statement,
the data-quality checks, the natural-key -> surrogate-key resolution and
the UPSERT from them. Adding a new table is a spec, not new code.

Three things every spec has to answer:

  * what does the source look like       -> stage_columns
  * what makes a row the *same* row      -> stage_dedup_key / conflict_key
  * what makes a row *changed*           -> hash_columns or compare_columns
"""

from __future__ import annotations

from dataclasses import dataclass, field


# ---------------------------------------------------------------------
# Data quality rules
# ---------------------------------------------------------------------
@dataclass(frozen=True)
class Rule:
    """A boolean SQL expression that must hold for a row to be accepted.

    `predicate` is evaluated against the staging alias `s`. NULL counts
    as a failure: a rule that cannot be evaluated has not passed.
    """

    name: str
    predicate: str
    description: str
    severity: str = "error"  # 'error' rejects the row, 'warn' only records


# ---------------------------------------------------------------------
# Natural key -> surrogate key resolution
# ---------------------------------------------------------------------
@dataclass(frozen=True)
class Lookup:
    """Resolve a business key in the feed to our surrogate key.

    This is the step that turns a flat CSV of business identifiers into
    a normalized, referentially-sound row. A row whose lookup fails is a
    referential-integrity reject, not a crash -- the upstream system is
    allowed to be late, we are not allowed to lose the record.
    """

    stage_col: str      # column in the staging table, e.g. 'customer_uid'
    ref_table: str      # table to resolve against,     e.g. 'customers'
    ref_key: str        # its natural key column,       e.g. 'customer_uid'
    ref_id: str         # its surrogate column,         e.g. 'customer_id'
    target_col: str     # destination column,           e.g. 'customer_id'
    required: bool = True
    # Additional join predicate; `{ref}` is substituted with whatever
    # alias the reference table is given in the statement being built.
    extra_on: str = ""

    @property
    def alias(self) -> str:
        return f"r_{self.target_col}"


@dataclass(frozen=True)
class TableSpec:
    table: str
    stage_columns: tuple[tuple[str, str], ...]   # (name, pg type)
    stage_dedup_key: tuple[str, ...]
    conflict_key: tuple[str, ...]
    insert_columns: tuple[str, ...]
    update_columns: tuple[str, ...]
    lookups: tuple[Lookup, ...] = ()
    hash_columns: tuple[str, ...] = ()
    compare_columns: tuple[str, ...] = ()
    rules: tuple[Rule, ...] = ()
    # Ordering column used to pick the winner among in-batch duplicates.
    dedup_order: str = "source_updated_at DESC NULLS LAST"
    # Optional statement run after the UPSERT, in the same transaction.
    post_sql: str = ""
    source_file: str = ""
    # Fact tables carry no updated_at column -- they are immutable once
    # written, so there is nothing to stamp.
    touch_updated_at: bool = True

    def __post_init__(self) -> None:
        if not self.hash_columns and not self.compare_columns:
            raise ValueError(
                f"{self.table}: need hash_columns or compare_columns to detect change"
            )

    @property
    def stage_column_names(self) -> tuple[str, ...]:
        return tuple(name for name, _ in self.stage_columns)


# =====================================================================
# Reusable rule fragments
# =====================================================================
def not_blank(col: str) -> Rule:
    return Rule(
        name=f"{col}_not_blank",
        predicate=f"s.{col} IS NOT NULL AND btrim(s.{col}) <> ''",
        description=f"{col} must be present and non-empty",
    )


def positive(col: str) -> Rule:
    return Rule(
        name=f"{col}_positive",
        predicate=f"s.{col} > 0",
        description=f"{col} must be greater than zero",
    )


def non_negative(col: str) -> Rule:
    return Rule(
        name=f"{col}_non_negative",
        predicate=f"s.{col} >= 0",
        description=f"{col} must not be negative",
    )


def in_range(col: str, lo: float, hi: float) -> Rule:
    return Rule(
        name=f"{col}_in_range",
        predicate=f"s.{col} BETWEEN {lo} AND {hi}",
        description=f"{col} must be between {lo} and {hi}",
    )


def not_future(col: str, slack: str = "1 day") -> Rule:
    return Rule(
        name=f"{col}_not_future",
        predicate=f"s.{col} <= now() + interval '{slack}'",
        description=f"{col} must not be dated in the future",
    )


# =====================================================================
# The specs, in dependency order. `LOAD_ORDER` is what pipeline.py walks.
# =====================================================================

REGIONS = TableSpec(
    table="regions",
    source_file="regions.csv",
    stage_columns=(
        ("region_code", "text"),
        ("region_name", "text"),
        ("country_code", "char(2)"),
        ("currency_code", "char(3)"),
        ("source_updated_at", "timestamptz"),
    ),
    stage_dedup_key=("region_code",),
    conflict_key=("region_code",),
    insert_columns=("region_code", "region_name", "country_code", "currency_code"),
    update_columns=("region_name", "country_code", "currency_code"),
    compare_columns=("region_name", "country_code", "currency_code"),
    rules=(
        not_blank("region_code"),
        not_blank("region_name"),
        Rule(
            name="country_code_iso",
            predicate="s.country_code ~ '^[A-Z]{2}$'",
            description="country_code must be a 2-letter uppercase ISO code",
        ),
    ),
)

CATEGORIES = TableSpec(
    table="categories",
    source_file="categories.csv",
    stage_columns=(
        ("category_slug", "text"),
        ("category_name", "text"),
        ("parent_slug", "text"),
        ("depth", "smallint"),
        ("source_updated_at", "timestamptz"),
    ),
    stage_dedup_key=("category_slug",),
    conflict_key=("category_slug",),
    insert_columns=("category_slug", "category_name", "depth"),
    update_columns=("category_name", "depth"),
    compare_columns=("category_name", "depth"),
    rules=(
        not_blank("category_slug"),
        not_blank("category_name"),
        in_range("depth", 0, 4),
    ),
    # Self-references cannot be resolved in the same INSERT that creates
    # the parents, so the hierarchy is stitched up in a second pass once
    # every node exists. Set-based, one statement, still idempotent.
    post_sql="""
        UPDATE {schema}.categories c
           SET parent_id = p.category_id
          FROM stg_dedup s
          JOIN {schema}.categories p ON p.category_slug = s.parent_slug
         WHERE c.category_slug = s.category_slug
           AND s.parent_slug IS NOT NULL
           AND c.parent_id IS DISTINCT FROM p.category_id
    """,
)

CUSTOMERS = TableSpec(
    table="customers",
    source_file="customers.csv",
    stage_columns=(
        ("customer_uid", "text"),
        ("email", "text"),
        ("full_name", "text"),
        ("region_code", "text"),
        ("status", "text"),
        ("signup_at", "timestamptz"),
        ("lifetime_value", "numeric(12,2)"),
        ("source_updated_at", "timestamptz"),
    ),
    stage_dedup_key=("customer_uid",),
    conflict_key=("customer_uid",),
    insert_columns=(
        "customer_uid", "email", "full_name", "region_id",
        "status", "signup_at", "lifetime_value", "source_hash", "source_batch",
    ),
    update_columns=(
        "email", "full_name", "region_id", "status", "lifetime_value",
        "source_hash", "source_batch",
    ),
    hash_columns=("email", "full_name", "region_code", "status", "lifetime_value"),
    lookups=(
        Lookup("region_code", "regions", "region_code", "region_id", "region_id"),
    ),
    rules=(
        not_blank("customer_uid"),
        Rule(
            name="email_shape",
            predicate=r"s.email ~ '^[^@\s]+@[^@\s]+\.[^@\s]+$'",
            description="email must look like an address",
        ),
        not_blank("full_name"),
        non_negative("lifetime_value"),
        not_future("signup_at"),
        Rule(
            name="status_known",
            predicate="s.status IN ('active','dormant','churned','blocked')",
            description="status must be one of the modelled customer states",
        ),
    ),
)

ADDRESSES = TableSpec(
    table="customer_addresses",
    source_file="addresses.csv",
    stage_columns=(
        ("customer_uid", "text"),
        ("kind", "text"),
        ("line1", "text"),
        ("line2", "text"),
        ("city", "text"),
        ("postal_code", "text"),
        ("region_code", "text"),
        ("is_default", "boolean"),
        ("source_updated_at", "timestamptz"),
    ),
    stage_dedup_key=("customer_uid", "kind"),
    conflict_key=("customer_id", "kind"),
    insert_columns=(
        "customer_id", "kind", "line1", "line2", "city",
        "postal_code", "region_id", "is_default",
    ),
    update_columns=("line1", "line2", "city", "postal_code", "region_id", "is_default"),
    compare_columns=("line1", "line2", "city", "postal_code", "region_id", "is_default"),
    lookups=(
        Lookup("customer_uid", "customers", "customer_uid", "customer_id", "customer_id"),
        Lookup("region_code", "regions", "region_code", "region_id", "region_id"),
    ),
    rules=(
        not_blank("line1"),
        not_blank("city"),
        not_blank("postal_code"),
        Rule(
            name="kind_known",
            predicate="s.kind IN ('billing','shipping')",
            description="address kind must be billing or shipping",
        ),
    ),
)

SELLERS = TableSpec(
    table="sellers",
    source_file="sellers.csv",
    stage_columns=(
        ("seller_uid", "text"),
        ("display_name", "text"),
        ("region_code", "text"),
        ("rating", "numeric(3,2)"),
        ("is_active", "boolean"),
        ("onboarded_at", "timestamptz"),
        ("source_updated_at", "timestamptz"),
    ),
    stage_dedup_key=("seller_uid",),
    conflict_key=("seller_uid",),
    insert_columns=(
        "seller_uid", "display_name", "region_id", "rating",
        "is_active", "onboarded_at", "source_hash",
    ),
    update_columns=("display_name", "region_id", "rating", "is_active", "source_hash"),
    hash_columns=("display_name", "region_code", "rating", "is_active"),
    lookups=(
        Lookup("region_code", "regions", "region_code", "region_id", "region_id"),
    ),
    rules=(
        not_blank("seller_uid"),
        not_blank("display_name"),
        in_range("rating", 0, 5),
        not_future("onboarded_at"),
    ),
)

PRODUCTS = TableSpec(
    table="products",
    source_file="products.csv",
    stage_columns=(
        ("product_uid", "text"),
        ("title", "text"),
        ("seller_uid", "text"),
        ("category_slug", "text"),
        ("base_price", "numeric(10,2)"),
        ("is_active", "boolean"),
        ("launched_at", "timestamptz"),
        ("source_updated_at", "timestamptz"),
    ),
    stage_dedup_key=("product_uid",),
    conflict_key=("product_uid",),
    insert_columns=(
        "product_uid", "title", "seller_id", "category_id",
        "base_price", "is_active", "launched_at", "source_hash",
    ),
    update_columns=("title", "seller_id", "category_id", "base_price", "is_active", "source_hash"),
    hash_columns=("title", "seller_uid", "category_slug", "base_price", "is_active"),
    lookups=(
        Lookup("seller_uid", "sellers", "seller_uid", "seller_id", "seller_id"),
        Lookup("category_slug", "categories", "category_slug", "category_id", "category_id"),
    ),
    rules=(
        not_blank("product_uid"),
        not_blank("title"),
        positive("base_price"),
        Rule(
            name="base_price_sane",
            predicate="s.base_price < 1000000",
            description="base_price above 1,000,000 is almost certainly a units error",
            severity="warn",
        ),
    ),
)

VARIANTS = TableSpec(
    table="product_variants",
    source_file="variants.csv",
    stage_columns=(
        ("sku", "text"),
        ("product_uid", "text"),
        ("variant_label", "text"),
        ("price_delta", "numeric(10,2)"),
        ("weight_grams", "integer"),
        ("is_active", "boolean"),
        ("source_updated_at", "timestamptz"),
    ),
    stage_dedup_key=("sku",),
    conflict_key=("sku",),
    insert_columns=(
        "sku", "product_id", "variant_label", "price_delta",
        "weight_grams", "is_active", "source_hash",
    ),
    update_columns=("product_id", "variant_label", "price_delta", "weight_grams", "is_active", "source_hash"),
    hash_columns=("product_uid", "variant_label", "price_delta", "weight_grams", "is_active"),
    lookups=(
        Lookup("product_uid", "products", "product_uid", "product_id", "product_id"),
    ),
    rules=(
        not_blank("sku"),
        not_blank("variant_label"),
        non_negative("weight_grams"),
    ),
)

INVENTORY = TableSpec(
    table="inventory",
    source_file="inventory.csv",
    stage_columns=(
        ("sku", "text"),
        ("qty_on_hand", "integer"),
        ("qty_reserved", "integer"),
        ("reorder_point", "integer"),
        ("source_updated_at", "timestamptz"),
    ),
    stage_dedup_key=("sku",),
    conflict_key=("variant_id",),
    insert_columns=("variant_id", "qty_on_hand", "qty_reserved", "reorder_point"),
    update_columns=("qty_on_hand", "qty_reserved", "reorder_point"),
    compare_columns=("qty_on_hand", "qty_reserved", "reorder_point"),
    lookups=(
        Lookup("sku", "product_variants", "sku", "variant_id", "variant_id"),
    ),
    rules=(
        non_negative("qty_on_hand"),
        non_negative("qty_reserved"),
        non_negative("reorder_point"),
        # The table's CHECK constraint would abort the whole batch on a
        # violation. Catching it as a DQ rule turns that into one
        # quarantined row plus a load that still succeeds.
        Rule(
            name="reserved_within_on_hand",
            predicate="s.qty_reserved <= s.qty_on_hand",
            description="reserved stock cannot exceed stock on hand",
        ),
    ),
)

ORDERS = TableSpec(
    table="orders",
    source_file="orders.csv",
    stage_columns=(
        ("order_uid", "text"),
        ("customer_uid", "text"),
        ("region_code", "text"),
        ("placed_at", "timestamptz"),
        ("status", "text"),
        ("currency_code", "char(3)"),
        ("subtotal", "numeric(12,2)"),
        ("shipping_fee", "numeric(10,2)"),
        ("tax_amount", "numeric(10,2)"),
        ("total_amount", "numeric(12,2)"),
        ("item_count", "smallint"),
        ("source_updated_at", "timestamptz"),
    ),
    stage_dedup_key=("order_uid", "placed_at"),
    conflict_key=("order_uid", "placed_at"),
    insert_columns=(
        "order_uid", "placed_at", "customer_id", "region_id", "status",
        "currency_code", "subtotal", "shipping_fee", "tax_amount",
        "total_amount", "item_count", "source_hash", "source_batch",
    ),
    update_columns=(
        "customer_id", "region_id", "status", "currency_code", "subtotal",
        "shipping_fee", "tax_amount", "total_amount", "item_count",
        "source_hash", "source_batch",
    ),
    hash_columns=(
        "customer_uid", "region_code", "status", "currency_code",
        "subtotal", "shipping_fee", "tax_amount", "total_amount", "item_count",
    ),
    lookups=(
        Lookup("customer_uid", "customers", "customer_uid", "customer_id", "customer_id"),
        Lookup("region_code", "regions", "region_code", "region_id", "region_id"),
    ),
    rules=(
        not_blank("order_uid"),
        not_future("placed_at"),
        positive("item_count"),
        non_negative("subtotal"),
        non_negative("total_amount"),
        Rule(
            name="status_known",
            predicate="s.status IN ('pending','paid','fulfilled','cancelled','refunded')",
            description="status must be one of the modelled order states",
        ),
        # The financial identity. This is the rule that actually catches
        # upstream bugs: totals that do not add up mean a broken
        # promotion engine or a currency mix-up, and they poison every
        # revenue number downstream.
        Rule(
            name="total_reconciles",
            predicate=(
                "abs(s.total_amount - (s.subtotal + s.shipping_fee + s.tax_amount)) <= 0.01"
            ),
            description="total_amount must equal subtotal + shipping + tax (1c tolerance)",
        ),
        Rule(
            name="currency_iso",
            predicate="s.currency_code ~ '^[A-Z]{3}$'",
            description="currency_code must be a 3-letter uppercase ISO code",
        ),
    ),
)

ORDER_ITEMS = TableSpec(
    table="order_items",
    source_file="order_items.csv",
    stage_columns=(
        ("order_uid", "text"),
        ("placed_at", "timestamptz"),
        ("line_no", "smallint"),
        ("sku", "text"),
        ("seller_uid", "text"),
        ("quantity", "smallint"),
        ("unit_price", "numeric(10,2)"),
        ("discount", "numeric(10,2)"),
        ("line_total", "numeric(12,2)"),
    ),
    stage_dedup_key=("order_uid", "line_no", "placed_at"),
    conflict_key=("order_id", "line_no", "placed_at"),
    insert_columns=(
        "order_id", "placed_at", "line_no", "variant_id", "seller_id",
        "quantity", "unit_price", "discount", "line_total",
    ),
    update_columns=("variant_id", "seller_id", "quantity", "unit_price", "discount", "line_total"),
    # No source_hash column on this table: at tens of millions of rows a
    # 32-byte digest per row is real storage for no extra signal, so
    # change detection compares the value columns directly instead.
    compare_columns=("variant_id", "seller_id", "quantity", "unit_price", "discount", "line_total"),
    dedup_order="line_no",
    touch_updated_at=False,
    lookups=(
        # The join carries placed_at as well as the uid: on a partitioned
        # parent, matching the partition key lets the planner prune, and
        # it is also what the composite unique constraint requires.
        Lookup(
            "order_uid", "orders", "order_uid", "order_id", "order_id",
            extra_on="AND {ref}.placed_at = s.placed_at",
        ),
        Lookup("sku", "product_variants", "sku", "variant_id", "variant_id"),
        Lookup("seller_uid", "sellers", "seller_uid", "seller_id", "seller_id"),
    ),
    rules=(
        positive("quantity"),
        non_negative("unit_price"),
        non_negative("discount"),
        Rule(
            name="line_total_reconciles",
            predicate="abs(s.line_total - (s.quantity * s.unit_price - s.discount)) <= 0.01",
            description="line_total must equal quantity * unit_price - discount",
        ),
        Rule(
            name="discount_not_above_gross",
            predicate="s.discount <= s.quantity * s.unit_price",
            description="a discount larger than the line's gross value is a pricing bug",
        ),
    ),
)

PAYMENTS = TableSpec(
    table="payments",
    source_file="payments.csv",
    stage_columns=(
        ("payment_uid", "text"),
        ("order_uid", "text"),
        ("placed_at", "timestamptz"),
        ("status", "text"),
        ("method", "text"),
        ("amount", "numeric(12,2)"),
        ("processed_at", "timestamptz"),
        ("source_updated_at", "timestamptz"),
    ),
    stage_dedup_key=("payment_uid",),
    conflict_key=("payment_uid",),
    insert_columns=(
        "payment_uid", "order_id", "placed_at", "status",
        "method", "amount", "processed_at", "source_hash",
    ),
    update_columns=("status", "method", "amount", "processed_at", "source_hash"),
    hash_columns=("order_uid", "status", "method", "amount", "processed_at"),
    touch_updated_at=False,
    lookups=(
        Lookup(
            "order_uid", "orders", "order_uid", "order_id", "order_id",
            extra_on="AND {ref}.placed_at = s.placed_at",
        ),
    ),
    rules=(
        not_blank("payment_uid"),
        non_negative("amount"),
        Rule(
            name="status_known",
            predicate="s.status IN ('authorized','captured','failed','refunded','chargeback')",
            description="status must be one of the modelled payment states",
        ),
        Rule(
            name="processed_after_placed",
            predicate="s.processed_at >= s.placed_at",
            description="a payment cannot be processed before its order was placed",
        ),
    ),
)

SHIPMENTS = TableSpec(
    table="shipments",
    source_file="shipments.csv",
    stage_columns=(
        ("shipment_uid", "text"),
        ("order_uid", "text"),
        ("placed_at", "timestamptz"),
        ("carrier", "text"),
        ("status", "text"),
        ("shipped_at", "timestamptz"),
        ("delivered_at", "timestamptz"),
        ("tracking_code", "text"),
        ("source_updated_at", "timestamptz"),
    ),
    stage_dedup_key=("shipment_uid",),
    conflict_key=("shipment_uid",),
    insert_columns=(
        "shipment_uid", "order_id", "placed_at", "carrier",
        "status", "shipped_at", "delivered_at", "tracking_code",
    ),
    update_columns=("carrier", "status", "shipped_at", "delivered_at", "tracking_code"),
    compare_columns=("carrier", "status", "shipped_at", "delivered_at", "tracking_code"),
    touch_updated_at=False,
    lookups=(
        Lookup(
            "order_uid", "orders", "order_uid", "order_id", "order_id",
            extra_on="AND {ref}.placed_at = s.placed_at",
        ),
    ),
    rules=(
        not_blank("shipment_uid"),
        not_blank("carrier"),
        Rule(
            name="status_known",
            predicate="s.status IN ('label_created','in_transit','delivered','lost','returned')",
            description="status must be one of the modelled shipment states",
        ),
        Rule(
            name="delivery_after_shipment",
            predicate=(
                "s.delivered_at IS NULL OR s.shipped_at IS NULL "
                "OR s.delivered_at >= s.shipped_at"
            ),
            description="delivered_at cannot precede shipped_at",
        ),
    ),
)


LOAD_ORDER: tuple[TableSpec, ...] = (
    REGIONS,
    CATEGORIES,
    CUSTOMERS,
    ADDRESSES,
    SELLERS,
    PRODUCTS,
    VARIANTS,
    INVENTORY,
    ORDERS,
    ORDER_ITEMS,
    PAYMENTS,
    SHIPMENTS,
)

BY_NAME: dict[str, TableSpec] = {spec.table: spec for spec in LOAD_ORDER}
