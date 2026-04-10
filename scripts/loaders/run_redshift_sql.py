import argparse
import os
from pathlib import Path

import redshift_connector


def split_sql_statements(sql_text: str) -> list[str]:
    statements = []
    for statement in sql_text.split(";"):
        stmt = statement.strip()
        if stmt:
            statements.append(stmt)
    return statements


def run_sql_file(sql_file: str) -> None:
    host = os.environ["REDSHIFT_HOST"]
    database = os.environ["REDSHIFT_DATABASE"]
    user = os.environ["REDSHIFT_USER"]
    password = os.environ["REDSHIFT_PASSWORD"]
    port = int(os.getenv("REDSHIFT_PORT", "5439"))

    sql_path = Path(sql_file)
    if not sql_path.exists():
        raise FileNotFoundError(f"SQL file not found: {sql_path}")

    sql_text = sql_path.read_text(encoding="utf-8")
    statements = split_sql_statements(sql_text)
    if not statements:
        raise ValueError(f"No SQL statements found in: {sql_path}")

    conn = redshift_connector.connect(
        host=host,
        database=database,
        user=user,
        password=password,
        port=port,
    )

    try:
        with conn:
            with conn.cursor() as cursor:
                for stmt in statements:
                    cursor.execute(stmt)
        print(f"Successfully executed: {sql_path}")
    finally:
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run a SQL file against Redshift")
    parser.add_argument("--sql-file", required=True, help="Path to the SQL file")
    args = parser.parse_args()

    run_sql_file(args.sql_file)
