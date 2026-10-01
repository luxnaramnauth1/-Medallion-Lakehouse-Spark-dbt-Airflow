"""Generate a deterministic, intentionally messy e-commerce batch for one date.

Writes CSVs to  <LAKE_ROOT>/raw/<entity>/ds=<YYYY-MM-DD>/  (the "landing zone").
Dirty data included on purpose so the Silver layer has something to fix:
duplicates, blank IDs, mixed-case/whitespace values, invalid emails, bad quantities.
"""
import argparse
import csv
import os
import random
from datetime import date, datetime, timedelta

LAKE_ROOT = os.getenv("LAKE_ROOT", "/opt/lakehouse")
N_CUSTOMERS, N_PRODUCTS, N_ORDERS = 500, 50, 300
COUNTRIES = ["US", "GB", "DE", "FR", "IN", "MU", "ZA", "BR", "JP", "AU"]
CATEGORIES = ["electronics", "books", "home", "toys", "fashion", "sports"]
STATUSES = ["placed", "shipped", "completed", "completed", "completed", "cancelled", "returned"]
DISCOUNTS = [0, 0, 0, 5, 10, 20]


def write(entity: str, ds: str, header: list, rows: list) -> None:
    out_dir = f"{LAKE_ROOT}/raw/{entity}/ds={ds}"
    os.makedirs(out_dir, exist_ok=True)
    with open(f"{out_dir}/{entity}_{ds}.csv", "w", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(header)
        writer.writerows(rows)
    print(f"wrote {len(rows):>5} rows -> {out_dir}")


def messy_case(rng, value: str) -> str:
    return rng.choice([value, value.lower(), f" {value} ", value.upper()])


def product_price(pid: int) -> float:
    return round(random.Random(pid).uniform(5, 500), 2)


def customers(ds: str, rng: random.Random):
    rows = []
    for cid in range(1, N_CUSTOMERS + 1):
        base = random.Random(cid)  # stable attributes per customer
        email = f"user{cid}@example.com"
        if rng.random() < 0.05:
            email = rng.choice(["", "not-an-email", f"user{cid}(at)example.com"])
        if rng.random() < 0.10:  # a customer "moves" -> new country, newer updated_at
            country = rng.choice(COUNTRIES)
        else:
            country = COUNTRIES[cid % len(COUNTRIES)]
        signup = date(2023, 1, 1) + timedelta(days=cid % 365)
        cust_id = "" if rng.random() < 0.01 else f"C{cid:05d}"
        rows.append([cust_id, base.choice(["ana", "li", "sam", "max", "zoe"]), f"Lastname{cid}",
                     email, messy_case(rng, country), signup.isoformat(), f"{ds} 02:00:00"])
        if rng.random() < 0.03:  # duplicate row
            rows.append(rows[-1])
    return ["customer_id", "first_name", "last_name", "email", "country", "signup_date", "updated_at"], rows


def products(ds: str, rng: random.Random):
    rows = []
    for pid in range(1, N_PRODUCTS + 1):
        cat = CATEGORIES[pid % len(CATEGORIES)]
        rows.append([f"P{pid:03d}", f"Product {pid}", messy_case(rng, cat), f"{product_price(pid):.2f}"])
    return ["product_id", "name", "category", "unit_price"], rows


def orders_and_items(ds: str, rng: random.Random):
    o_rows, i_rows = [], []
    day = datetime.fromisoformat(ds)
    for n in range(1, N_ORDERS + 1):
        oid = f"O{ds.replace('-', '')}{n:04d}"
        ts = day + timedelta(seconds=rng.randint(0, 86399))
        cust = "" if rng.random() < 0.01 else f"C{rng.randint(1, N_CUSTOMERS):05d}"
        o_rows.append([oid, cust, ts.strftime("%Y-%m-%d %H:%M:%S"),
                       messy_case(rng, rng.choice(STATUSES)), "usd"])
        if rng.random() < 0.02:
            o_rows.append(o_rows[-1])
        for k in range(1, rng.randint(1, 4) + 1):
            pid = rng.randint(1, N_PRODUCTS)
            qty = -1 if rng.random() < 0.01 else rng.randint(1, 5)
            i_rows.append([f"{oid}-{k}", oid, f"P{pid:03d}", qty,
                           f"{product_price(pid):.2f}", rng.choice(DISCOUNTS)])
    return (["order_id", "customer_id", "order_ts", "status", "currency"], o_rows,
            ["order_item_id", "order_id", "product_id", "quantity", "unit_price", "discount_pct"], i_rows)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--ds", required=True, help="batch date YYYY-MM-DD")
    ds = parser.parse_args().ds
    rng = random.Random(ds)  # reproducible per date
    write("customers", ds, *customers(ds, rng))
    write("products", ds, *products(ds, rng))
    oh, orows, ih, irows = orders_and_items(ds, rng)
    write("orders", ds, oh, orows)
    write("order_items", ds, ih, irows)


if __name__ == "__main__":
    main()
