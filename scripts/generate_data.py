#!/usr/bin/env python3
"""Generate the source feeds the ETL consumes.

Writes twelve CSVs into `data/`. Deterministic for a given seed, so a
benchmark run is reproducible.

Two things it does on purpose that a naive generator would not:

  * Skewed distributions. Orders per customer, revenue per seller and
    sales per SKU all follow a power law. Uniform synthetic data makes
    every index look equally good and hides exactly the cases -- hot
    keys, fat partitions -- where index choice matters.

  * Deliberately dirty rows. A configurable fraction of records carry
    real defects: duplicate natural keys, malformed emails, totals that
    do not reconcile, references to customers that do not exist. Without
    them the data-quality layer is untested decoration.

Usage:
    python scripts/generate_data.py --scale demo
    python scripts/generate_data.py --scale full --dirty-ratio 0.005
"""

from __future__ import annotations

import argparse
import csv
import math
import random
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from queryquest.config import settings  # noqa: E402

# Orders is the driver; everything else is sized off it.
SCALES = {
    "tiny":   2_000,
    "demo":   60_000,
    "medium": 400_000,
    "full":   2_500_000,
}

REGIONS = [
    ("na-east", "North America East", "US", "USD"),
    ("na-west", "North America West", "US", "USD"),
    ("na-cent", "North America Central", "CA", "CAD"),
    ("eu-west", "Europe West", "IE", "EUR"),
    ("eu-cent", "Europe Central", "DE", "EUR"),
    ("eu-nord", "Europe Nordics", "SE", "SEK"),
    ("uk-main", "United Kingdom", "GB", "GBP"),
    ("apac-in", "India", "IN", "INR"),
    ("apac-sg", "Singapore", "SG", "SGD"),
    ("apac-au", "Australia", "AU", "AUD"),
    ("apac-jp", "Japan", "JP", "JPY"),
    ("latam-b", "Brazil", "BR", "BRL"),
]

TOP_CATEGORIES = [
    "electronics", "home-kitchen", "apparel", "sports-outdoors",
    "books-media", "beauty-health", "toys-games", "automotive",
]
SUB_CATEGORIES = [
    "accessories", "essentials", "premium", "clearance", "seasonal", "bundles",
]

FIRST = ["Ana", "Ben", "Chen", "Dara", "Eli", "Fatima", "Gio", "Hana", "Idris",
         "Jae", "Kira", "Luca", "Mira", "Nikhil", "Omar", "Priya", "Quinn",
         "Rosa", "Sami", "Tara", "Uma", "Viktor", "Wen", "Yara", "Zane"]
LAST = ["Alvarez", "Baptiste", "Chowdhury", "Duarte", "Eriksen", "Fontaine",
        "Gruber", "Haddad", "Ivanov", "Jensen", "Kaur", "Lindqvist", "Moreau",
        "Nakamura", "Okafor", "Pereira", "Rahman", "Silva", "Tanaka", "Uddin",
        "Volkov", "Whitfield", "Xu", "Yilmaz", "Zhang"]
ADJ = ["Compact", "Rugged", "Featherweight", "Insulated", "Adjustable",
       "Cordless", "Reversible", "Stackable", "Weatherproof", "Ergonomic"]
NOUN = ["Kettle", "Backpack", "Monitor Stand", "Headphones", "Desk Lamp",
        "Jacket", "Water Bottle", "Keyboard", "Yoga Mat", "Toolkit",
        "Blender", "Sneakers", "Notebook", "Router", "Cookware Set"]
CITIES = ["Aurora", "Belmont", "Carlisle", "Dover", "Elmwood", "Fairview",
          "Granton", "Harrow", "Inglewood", "Jarrow", "Kingsway", "Lakeport"]
CARRIERS = ["swiftpost", "aeroline", "meridian", "cartago", "northbound"]
PAY_METHODS = ["card", "wallet", "bank_transfer", "cod", "gift_card"]

ORDER_STATUS = ["pending", "paid", "fulfilled", "cancelled", "refunded"]
ORDER_STATUS_W = [6, 22, 62, 7, 3]
CUSTOMER_STATUS = ["active", "dormant", "churned", "blocked"]
CUSTOMER_STATUS_W = [72, 17, 9, 2]
SHIP_STATUS = ["label_created", "in_transit", "delivered", "lost", "returned"]
SHIP_STATUS_W = [5, 9, 82, 1, 3]
PAY_STATUS = ["authorized", "captured", "failed", "refunded", "chargeback"]
PAY_STATUS_W = [8, 84, 4, 3, 1]


def iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S+00")


def zipf_index(rng: random.Random, n: int, skew: float = 1.15) -> int:
    """Draw from a power law over [0, n). Cheap approximation -- exact
    Zipf sampling is not worth the cost at ten million draws."""
    u = rng.random()
    return min(n - 1, int(n * (u ** skew)))


class Generator:
    def __init__(self, scale: str, out_dir: Path, seed: int, dirty_ratio: float,
                 months: int = 24) -> None:
        self.rng = random.Random(seed)
        self.out = out_dir
        self.out.mkdir(parents=True, exist_ok=True)
        self.dirty = dirty_ratio
        self.months = months

        self.n_orders = SCALES[scale]
        self.n_customers = max(50, self.n_orders // 4)
        self.n_sellers = max(10, self.n_orders // 200)
        self.n_products = max(40, self.n_sellers * 8)
        self.n_variants = max(60, int(self.n_products * 2.5))

        self.end = datetime.now(timezone.utc).replace(
            hour=0, minute=0, second=0, microsecond=0
        ) - timedelta(days=1)
        self.start = self.end - timedelta(days=30 * months)

        self.region_codes = [r[0] for r in REGIONS]
        self.category_slugs: list[str] = []
        self.stats: dict[str, int] = {}

    # -- helpers -------------------------------------------------------
    def _w(self, path: str):
        fh = (self.out / path).open("w", encoding="utf-8", newline="")
        return fh, csv.writer(fh, lineterminator="\n")

    def _is_dirty(self) -> bool:
        return self.dirty > 0 and self.rng.random() < self.dirty

    def _ts_between(self, lo: datetime, hi: datetime) -> datetime:
        span = int((hi - lo).total_seconds())
        return lo + timedelta(seconds=self.rng.randrange(max(1, span)))

    # -- dimensions ----------------------------------------------------
    def regions(self) -> None:
        fh, w = self._w("regions.csv")
        w.writerow(["region_code", "region_name", "country_code", "currency_code",
                    "source_updated_at"])
        for code, name, country, currency in REGIONS:
            w.writerow([code, name, country, currency, iso(self.start)])
        fh.close()
        self.stats["regions"] = len(REGIONS)

    def categories(self) -> None:
        fh, w = self._w("categories.csv")
        w.writerow(["category_slug", "category_name", "parent_slug", "depth",
                    "source_updated_at"])
        n = 0
        for top in TOP_CATEGORIES:
            w.writerow([top, top.replace("-", " ").title(), "", 0, iso(self.start)])
            n += 1
            for sub in SUB_CATEGORIES:
                slug = f"{top}-{sub}"
                w.writerow([slug, f"{top.title()} {sub.title()}", top, 1, iso(self.start)])
                self.category_slugs.append(slug)
                n += 1
        fh.close()
        self.stats["categories"] = n

    def customers(self) -> None:
        fh, w = self._w("customers.csv")
        w.writerow(["customer_uid", "email", "full_name", "region_code", "status",
                    "signup_at", "lifetime_value", "source_updated_at"])
        fh_a, wa = self._w("addresses.csv")
        wa.writerow(["customer_uid", "kind", "line1", "line2", "city", "postal_code",
                     "region_code", "is_default", "source_updated_at"])

        n_addr = 0
        for i in range(self.n_customers):
            uid = f"CUST-{i:09d}"
            first = self.rng.choice(FIRST)
            last = self.rng.choice(LAST)
            name = f"{first} {last}"
            email = f"{first.lower()}.{last.lower()}{i}@example.com"
            region = self.rng.choice(self.region_codes)
            status = self.rng.choices(CUSTOMER_STATUS, CUSTOMER_STATUS_W)[0]
            signup = self._ts_between(self.start - timedelta(days=365), self.end)
            ltv = round(self.rng.lognormvariate(4.2, 1.0), 2)
            updated = iso(self.end)

            if self._is_dirty():
                # A malformed address the DQ layer must quarantine.
                email = email.replace("@", "_at_")

            w.writerow([uid, email, name, region, status, iso(signup), ltv, updated])

            if self._is_dirty():
                # Same natural key, later timestamp: an in-batch duplicate
                # that dedup must collapse to exactly one surviving row.
                w.writerow([uid, email, name, region, status, iso(signup),
                            round(ltv + 1, 2), iso(self.end + timedelta(minutes=5))])

            for kind in ("billing", "shipping"):
                if kind == "shipping" and self.rng.random() < 0.35:
                    continue
                wa.writerow([
                    uid, kind,
                    f"{self.rng.randrange(1, 999)} {self.rng.choice(NOUN)} Street", "",
                    self.rng.choice(CITIES),
                    f"{self.rng.randrange(10000, 99999)}",
                    region, "true" if kind == "billing" else "false", updated,
                ])
                n_addr += 1

        fh.close()
        fh_a.close()
        self.stats["customers"] = self.n_customers
        self.stats["addresses"] = n_addr

    def sellers(self) -> None:
        fh, w = self._w("sellers.csv")
        w.writerow(["seller_uid", "display_name", "region_code", "rating",
                    "is_active", "onboarded_at", "source_updated_at"])
        for i in range(self.n_sellers):
            w.writerow([
                f"SELL-{i:07d}",
                f"{self.rng.choice(ADJ)} {self.rng.choice(NOUN)} Co.",
                self.rng.choice(self.region_codes),
                round(self.rng.uniform(3.0, 5.0), 2),
                "true" if self.rng.random() < 0.94 else "false",
                iso(self._ts_between(self.start - timedelta(days=730), self.end)),
                iso(self.end),
            ])
        fh.close()
        self.stats["sellers"] = self.n_sellers

    def products_and_variants(self) -> None:
        fh_p, wp = self._w("products.csv")
        wp.writerow(["product_uid", "title", "seller_uid", "category_slug",
                     "base_price", "is_active", "launched_at", "source_updated_at"])
        fh_v, wv = self._w("variants.csv")
        wv.writerow(["sku", "product_uid", "variant_label", "price_delta",
                     "weight_grams", "is_active", "source_updated_at"])
        fh_i, wi = self._w("inventory.csv")
        wi.writerow(["sku", "qty_on_hand", "qty_reserved", "reorder_point",
                     "source_updated_at"])

        per_product = max(1, self.n_variants // self.n_products)
        n_var = 0
        for i in range(self.n_products):
            puid = f"PROD-{i:08d}"
            price = round(self.rng.lognormvariate(3.1, 0.75) + 4, 2)
            wp.writerow([
                puid,
                f"{self.rng.choice(ADJ)} {self.rng.choice(NOUN)} {self.rng.randrange(100, 999)}",
                f"SELL-{zipf_index(self.rng, self.n_sellers):07d}",
                self.rng.choice(self.category_slugs),
                price,
                "true" if self.rng.random() < 0.9 else "false",
                iso(self._ts_between(self.start - timedelta(days=400), self.end)),
                iso(self.end),
            ])
            for v in range(per_product):
                sku = f"SKU-{i:08d}-{v}"
                wv.writerow([
                    sku, puid, f"variant-{v}",
                    round(self.rng.uniform(-3, 12), 2),
                    self.rng.randrange(50, 5000),
                    "true" if self.rng.random() < 0.93 else "false",
                    iso(self.end),
                ])
                on_hand = self.rng.randrange(0, 400)
                reserved = self.rng.randrange(0, max(1, on_hand // 3 + 1))
                if self._is_dirty():
                    reserved = on_hand + self.rng.randrange(1, 20)  # oversold: must reject
                wi.writerow([sku, on_hand, reserved, self.rng.randrange(5, 60),
                             iso(self.end)])
                n_var += 1

        fh_p.close()
        fh_v.close()
        fh_i.close()
        self.stats["products"] = self.n_products
        self.stats["variants"] = n_var
        self.n_variants_actual = n_var

    # -- facts ---------------------------------------------------------
    def orders(self) -> None:
        fh_o, wo = self._w("orders.csv")
        wo.writerow(["order_uid", "customer_uid", "region_code", "placed_at",
                     "status", "currency_code", "subtotal", "shipping_fee",
                     "tax_amount", "total_amount", "item_count",
                     "source_updated_at"])
        fh_l, wl = self._w("order_items.csv")
        wl.writerow(["order_uid", "placed_at", "line_no", "sku", "seller_uid",
                     "quantity", "unit_price", "discount", "line_total"])
        fh_pay, wpay = self._w("payments.csv")
        wpay.writerow(["payment_uid", "order_uid", "placed_at", "status", "method",
                       "amount", "processed_at", "source_updated_at"])
        fh_s, ws = self._w("shipments.csv")
        ws.writerow(["shipment_uid", "order_uid", "placed_at", "carrier", "status",
                     "shipped_at", "delivered_at", "tracking_code",
                     "source_updated_at"])

        currency_of = {r[0]: r[3] for r in REGIONS}
        n_items = n_pay = n_ship = 0
        span = int((self.end - self.start).total_seconds())
        per_prod = max(1, self.n_variants // self.n_products)

        for i in range(self.n_orders):
            ouid = f"ORD-{i:010d}"
            # Recency bias: more orders in recent months, as real traffic has.
            frac = 1 - math.sqrt(self.rng.random())
            placed = self.end - timedelta(seconds=int(frac * span))
            cust_idx = zipf_index(self.rng, self.n_customers)
            cust = f"CUST-{cust_idx:09d}"
            region = self.region_codes[cust_idx % len(self.region_codes)]
            status = self.rng.choices(ORDER_STATUS, ORDER_STATUS_W)[0]

            n_lines = self.rng.choices([1, 2, 3, 4, 5, 6], [30, 26, 18, 12, 9, 5])[0]
            subtotal = 0.0
            lines = []
            for line_no in range(1, n_lines + 1):
                pidx = zipf_index(self.rng, self.n_products)
                vidx = self.rng.randrange(per_prod)
                sku = f"SKU-{pidx:08d}-{vidx}"
                seller = f"SELL-{zipf_index(self.rng, self.n_sellers):07d}"
                qty = self.rng.choices([1, 2, 3, 4], [66, 21, 9, 4])[0]
                unit = round(self.rng.lognormvariate(3.0, 0.7) + 3, 2)
                disc = round(unit * qty * self.rng.choice([0, 0, 0, 0.05, 0.1, 0.2]), 2)
                line_total = round(qty * unit - disc, 2)
                if self._is_dirty():
                    line_total = round(line_total * 1.35, 2)   # fails reconciliation
                subtotal += line_total
                lines.append((line_no, sku, seller, qty, unit, disc, line_total))

            subtotal = round(subtotal, 2)
            shipping = 0.0 if subtotal >= 50 else 4.99
            tax = round(subtotal * 0.08, 2)
            total = round(subtotal + shipping + tax, 2)
            if self._is_dirty():
                total = round(total * 1.2, 2)                  # fails reconciliation

            cust_out = cust
            if self._is_dirty():
                cust_out = f"CUST-{self.n_customers + 999999:09d}"  # unresolvable FK

            wo.writerow([ouid, cust_out, region, iso(placed), status,
                         currency_of[region], subtotal, shipping, tax, total,
                         n_lines, iso(placed)])

            for line_no, sku, seller, qty, unit, disc, lt in lines:
                wl.writerow([ouid, iso(placed), line_no, sku, seller, qty, unit, disc, lt])
                n_items += 1

            if status in ("paid", "fulfilled", "refunded"):
                processed = placed + timedelta(seconds=self.rng.randrange(5, 7200))
                wpay.writerow([
                    f"PAY-{i:010d}", ouid, iso(placed),
                    self.rng.choices(PAY_STATUS, PAY_STATUS_W)[0],
                    self.rng.choice(PAY_METHODS), total, iso(processed), iso(processed),
                ])
                n_pay += 1

            if status == "fulfilled":
                shipped = placed + timedelta(hours=self.rng.randrange(4, 96))
                sstatus = self.rng.choices(SHIP_STATUS, SHIP_STATUS_W)[0]
                delivered = (
                    shipped + timedelta(hours=self.rng.randrange(12, 240))
                    if sstatus == "delivered" else None
                )
                ws.writerow([
                    f"SHP-{i:010d}", ouid, iso(placed),
                    self.rng.choice(CARRIERS), sstatus, iso(shipped),
                    iso(delivered) if delivered else "",
                    f"TRK{self.rng.randrange(10**9, 10**10)}",
                    iso(shipped),
                ])
                n_ship += 1

        for fh in (fh_o, fh_l, fh_pay, fh_s):
            fh.close()

        self.stats["orders"] = self.n_orders
        self.stats["order_items"] = n_items
        self.stats["payments"] = n_pay
        self.stats["shipments"] = n_ship

    def run(self) -> dict[str, int]:
        t0 = time.perf_counter()
        self.regions()
        self.categories()
        self.customers()
        self.sellers()
        self.products_and_variants()
        self.orders()
        self.stats["_seconds"] = round(time.perf_counter() - t0, 1)
        return self.stats


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--scale", choices=sorted(SCALES), default="demo")
    ap.add_argument("--out", type=Path, default=None)
    ap.add_argument("--seed", type=int, default=20260822)
    ap.add_argument("--dirty-ratio", type=float, default=0.002,
                    help="fraction of records given a deliberate defect. "
                         "Defects cascade: a rejected order orphans its line "
                         "items and its payment, so the effective reject rate "
                         "downstream is several times this number.")
    ap.add_argument("--months", type=int, default=24)
    args = ap.parse_args()

    out = args.out or settings.data_dir
    gen = Generator(args.scale, out, args.seed, args.dirty_ratio, args.months)
    stats = gen.run()

    total = sum(v for k, v in stats.items() if not k.startswith("_"))
    print(f"generated into {out}")
    for k, v in stats.items():
        if not k.startswith("_"):
            print(f"  {k:<14} {v:>12,}")
    print(f"  {'TOTAL':<14} {total:>12,}  in {stats['_seconds']}s")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
