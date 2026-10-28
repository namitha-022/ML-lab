"""A small, deterministic batch pipeline using only the Python standard library."""

import argparse
import csv
import json
import sqlite3
from datetime import date, datetime, timezone
from pathlib import Path

FIELDS = ("order_id", "customer_id", "order_date", "amount_cents", "status", "updated_at")
STATUSES = {"paid", "pending", "cancelled", "refunded"}


def read_orders(path: Path) -> list[tuple[str, str, str, int, str, str]]:
    """Parse and validate the entire input before opening a write transaction."""
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames is None or set(reader.fieldnames) != set(FIELDS):
            raise ValueError(f"CSV must contain exactly these columns: {', '.join(FIELDS)}")
        rows = []
        for number, raw in enumerate(reader, start=2):
            try:
                if None in raw:
                    raise ValueError(f"expected {len(FIELDS)} fields, found more")
                order_id = raw["order_id"].strip()
                customer_id = raw["customer_id"].strip()
                order_date = date.fromisoformat(raw["order_date"].strip()).isoformat()
                amount_text = raw["amount_cents"].strip()
                if not amount_text.isascii() or not amount_text.isdecimal():
                    raise ValueError("amount_cents must be a non-negative integer")
                amount_cents = int(amount_text)
                status = raw["status"].strip().lower()
                timestamp = datetime.fromisoformat(raw["updated_at"].strip())
                if timestamp.tzinfo is None:
                    raise ValueError("updated_at must include a timezone")
                updated_at = timestamp.astimezone(timezone.utc).isoformat()
                if not order_id or not customer_id or status not in STATUSES:
                    raise ValueError("missing ID or unsupported status")
                if timestamp.date() < date.fromisoformat(order_date):
                    raise ValueError("updated_at precedes order_date")
                rows.append((order_id, customer_id, order_date, amount_cents, status, updated_at))
            except (KeyError, AttributeError, TypeError, ValueError) as exc:
                raise ValueError(f"Invalid CSV row {number}: {exc}") from exc
    if not rows:
        raise ValueError("CSV contains no orders")
    return rows


def initialize(connection: sqlite3.Connection) -> None:
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS bronze_orders (
            order_id TEXT NOT NULL,
            customer_id TEXT NOT NULL,
            order_date TEXT NOT NULL,
            amount_cents INTEGER NOT NULL CHECK (amount_cents >= 0),
            status TEXT NOT NULL CHECK (status IN ('paid', 'pending', 'cancelled', 'refunded')),
            updated_at TEXT NOT NULL,
            PRIMARY KEY (order_id, updated_at)
        );
        CREATE TABLE IF NOT EXISTS silver_orders (
            order_id TEXT PRIMARY KEY,
            customer_id TEXT NOT NULL,
            order_date TEXT NOT NULL,
            amount_cents INTEGER NOT NULL,
            status TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS daily_revenue (
            order_date TEXT PRIMARY KEY,
            paid_orders INTEGER NOT NULL,
            revenue_cents INTEGER NOT NULL
        );
        """
    )


def run(input_path: Path, database_path: Path) -> dict[str, int]:
    try:
        rows = read_orders(input_path)
    except FileNotFoundError as exc:
        raise FileNotFoundError(
            f"Input CSV not found: {input_path}. Check the --input path "
            f"(the Lab 01 sample is labs/01-orders/orders.csv). Original error: {exc}"
        ) from exc
    database_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with sqlite3.connect(database_path) as connection:
            initialize(connection)
            with connection:
                for row in rows:
                    existing = connection.execute(
                        "SELECT order_id, customer_id, order_date, amount_cents, status, updated_at "
                        "FROM bronze_orders WHERE order_id = ? AND updated_at = ?",
                        (row[0], row[5]),
                    ).fetchone()
                    if existing is not None:
                        if existing != row:
                            raise ValueError(f"Conflicting version for order {row[0]} at {row[5]}")
                        continue
                    connection.execute(
                        "INSERT INTO bronze_orders VALUES (?, ?, ?, ?, ?, ?)", row
                    )

                connection.execute("DELETE FROM silver_orders")
                connection.execute(
                    """
                    INSERT INTO silver_orders
                    SELECT order_id, customer_id, order_date, amount_cents, status, updated_at
                    FROM (
                        SELECT *, ROW_NUMBER() OVER (
                            PARTITION BY order_id ORDER BY updated_at DESC
                        ) AS rank FROM bronze_orders
                    ) WHERE rank = 1
                    """
                )
                connection.execute("DELETE FROM daily_revenue")
                connection.execute(
                    """
                    INSERT INTO daily_revenue
                    SELECT order_date, COUNT(*), SUM(amount_cents)
                    FROM silver_orders WHERE status = 'paid'
                    GROUP BY order_date
                    """
                )
                bronze = connection.execute("SELECT COUNT(*) FROM bronze_orders").fetchone()[0]
                silver = connection.execute("SELECT COUNT(*) FROM silver_orders").fetchone()[0]
                gold = connection.execute("SELECT COUNT(*) FROM daily_revenue").fetchone()[0]
                if silver > bronze or gold > silver:
                    raise RuntimeError("Data quality check failed: invalid table counts")
    except sqlite3.DatabaseError as exc:
        raise type(exc)(
            f"Cannot use database {database_path} for run. Use a SQLite file path "
            f"created by the pipeline. Original error: {exc}"
        ) from exc
    return {"input_rows": len(rows), "bronze_versions": bronze, "current_orders": silver, "revenue_days": gold}


def report(database_path: Path) -> list[dict[str, int | str]]:
    if not database_path.is_file():
        raise FileNotFoundError(
            f"Database not found: {database_path}. Check the --db path, or create it first "
            "by running `python3 -m launchpad.pipeline run` with a valid CSV and this "
            "database path."
        )
    try:
        with sqlite3.connect(database_path) as connection:
            return [
                {"order_date": day, "paid_orders": count, "revenue_cents": cents}
                for day, count, cents in connection.execute(
                    "SELECT order_date, paid_orders, revenue_cents FROM daily_revenue ORDER BY order_date"
                )
            ]
    except sqlite3.DatabaseError as exc:
        raise type(exc)(
            f"Cannot read report database {database_path}. Use a SQLite file created by "
            f"`python3 -m launchpad.pipeline run`. Original error: {exc}"
        ) from exc


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    run_command = subparsers.add_parser("run", help="ingest and transform CSV orders")
    run_command.add_argument("--input", type=Path, required=True)
    run_command.add_argument("--db", type=Path, required=True)
    report_command = subparsers.add_parser("report", help="show daily paid revenue")
    report_command.add_argument("--db", type=Path, required=True)
    args = parser.parse_args()
    try:
        result = run(args.input, args.db) if args.command == "run" else report(args.db)
    except (FileNotFoundError, sqlite3.DatabaseError) as exc:
        parser.error(str(exc))
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
