"""Provision the Unity Catalog Delta table ``test_catalog_azure.py`` reads.

Creates (or refills) a *managed* Delta table holding ``ROWS_PER_COMMIT *
COMMIT_COUNT`` rows -- ``id`` 0..N-1 and ``v`` = ``"r{id}"`` -- written as
``COMMIT_COUNT`` separate ``INSERT``s so the table ends up with several
``_delta_log`` versions and several Parquet data files. A one-file one-version
table would let the tests pass with a single credential reaching a single
process, which is the bug they exist to catch.

A managed table is used on purpose: credential vending works on
Databricks-managed storage, which on Azure is ADLS Gen2, so no storage account,
access connector, role assignment, storage credential or external location has
to be provisioned.

Everything runs through the SQL Statement Execution API, so the only dependency
is ``databricks-sdk`` -- no SQL driver, no cluster.

Usage::

    export RAY_TEST_AZURE_DATABRICKS_HOST=https://adb-XXXX.NN.azuredatabricks.net
    export RAY_TEST_AZURE_DATABRICKS_TOKEN=dapi...
    export RAY_TEST_AZURE_UC_DELTA_TABLE=main.ray_test.ray_azure_delta
    python load_azure_uc_test_table.py --warehouse-id <sql-warehouse-id>

    # then
    pytest python/ray/data/tests/datasource/test_catalog_azure.py

``--warehouse-id`` comes from the workspace UI (SQL Warehouses -> your
warehouse -> Connection details), or ``--list-warehouses`` to print them.

Grants needed by the principal that will *read* the table (the tests' token):

    GRANT USE CATALOG ON CATALOG <catalog> TO `<principal>`;
    GRANT USE SCHEMA  ON SCHEMA  <catalog>.<schema> TO `<principal>`;
    GRANT SELECT      ON TABLE   <catalog>.<schema>.<table> TO `<principal>`;
    GRANT EXTERNAL USE SCHEMA ON SCHEMA <catalog>.<schema> TO `<principal>`;

``EXTERNAL USE SCHEMA`` is the credential-vending grant, and it is only half the
gate: the metastore also needs ``external_access_enabled``, which is an
*account*-level setting (accounts.azuredatabricks.net -> Catalog -> metastore),
not reachable from the workspace API. Without both,
``generate_temporary_table_credentials`` refuses instead of vending a SAS.

Pass ``--verify`` alone to check an existing table without rewriting it.
"""

import argparse
import os
import sys

# Keep in sync with test_catalog_azure.py.
ROWS_PER_COMMIT = 4_000
COMMIT_COUNT = 3
EXPECTED_COUNT = ROWS_PER_COMMIT * COMMIT_COUNT

_WAIT = "50s"  # max the API allows to block before returning a statement id


def _client(host, token):
    from databricks.sdk import WorkspaceClient

    return WorkspaceClient(host=host, token=token)


def _run(w, warehouse_id, statement, catalog=None, schema=None):
    """Execute one statement and return its rows, raising on failure."""
    from databricks.sdk.service.sql import StatementState

    resp = w.statement_execution.execute_statement(
        statement=statement,
        warehouse_id=warehouse_id,
        catalog=catalog,
        schema=schema,
        wait_timeout=_WAIT,
    )

    # Anything not finished inside `_WAIT` comes back PENDING/RUNNING with an id
    # to poll. The API has no blocking-forever mode, so poll it here.
    while resp.status is not None and resp.status.state in (
        StatementState.PENDING,
        StatementState.RUNNING,
    ):
        import time

        time.sleep(2)
        resp = w.statement_execution.get_statement(resp.statement_id)

    state = resp.status.state if resp.status else None
    if state is not StatementState.SUCCEEDED:
        error = resp.status.error if resp.status else None
        message = error.message if error else "no error detail"
        raise RuntimeError(
            f"statement failed ({state}): {message}\n  statement: {statement}"
        )

    if resp.result is None or resp.result.data_array is None:
        return []
    return resp.result.data_array


def list_warehouses(w):
    print("SQL warehouses visible to this principal:")
    for wh in w.warehouses.list():
        print(f"  {wh.id}  {wh.name}  state={wh.state}")


def load(w, warehouse_id, catalog, schema, table):
    full = f"{catalog}.{schema}.{table}"

    print(f"creating schema {catalog}.{schema} (if absent)", flush=True)
    _run(w, warehouse_id, f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

    # Dropped rather than truncated so a rerun starts from version 0 and the
    # commit count is exactly COMMIT_COUNT.
    print(f"dropping and recreating {full}", flush=True)
    _run(w, warehouse_id, f"DROP TABLE IF EXISTS {full}")
    _run(w, warehouse_id, f"CREATE TABLE {full} (id BIGINT, v STRING) USING DELTA")

    for commit in range(COMMIT_COUNT):
        start = commit * ROWS_PER_COMMIT
        end = start + ROWS_PER_COMMIT
        print(
            f"  commit {commit + 1}/{COMMIT_COUNT}: ids {start}..{end - 1}",
            flush=True,
        )
        _run(
            w,
            warehouse_id,
            f"INSERT INTO {full} "
            f"SELECT id, concat('r', id) AS v FROM range({start}, {end})",
        )

    return full


def verify(w, warehouse_id, catalog, schema, table):
    full = f"{catalog}.{schema}.{table}"
    print(f"\nverifying {full}", flush=True)

    rows = _run(
        w,
        warehouse_id,
        f"SELECT count(*), min(id), max(id), count(DISTINCT id) FROM {full}",
    )
    count, min_id, max_id, distinct = (int(v) for v in rows[0])
    print(f"  count={count} min={min_id} max={max_id} distinct={distinct}")

    ok = True
    for label, actual, expected in (
        ("count", count, EXPECTED_COUNT),
        ("min(id)", min_id, 0),
        ("max(id)", max_id, EXPECTED_COUNT - 1),
        ("distinct(id)", distinct, EXPECTED_COUNT),
    ):
        if actual != expected:
            print(f"  MISMATCH {label}: expected {expected}, got {actual}")
            ok = False

    # `v` must match the tests' reference formula on every row, not just in
    # aggregate -- a wrong `v` would otherwise only surface as a test failure.
    bad = _run(
        w, warehouse_id, f"SELECT count(*) FROM {full} WHERE v <> concat('r', id)"
    )
    bad_v = int(bad[0][0])
    print(f"  rows with unexpected v: {bad_v}")
    if bad_v:
        ok = False

    # DESCRIBE HISTORY exposes the log versions; several means several
    # `_delta_log/*.json` files, which is the shape the tests assert.
    history = _run(w, warehouse_id, f"DESCRIBE HISTORY {full}")
    print(f"  delta log versions: {len(history)}")
    if len(history) < COMMIT_COUNT:
        print(f"  MISMATCH: expected >= {COMMIT_COUNT} versions")
        ok = False

    files = _run(
        w, warehouse_id, f"SELECT count(DISTINCT _metadata.file_path) FROM {full}"
    )
    n_files = int(files[0][0])
    print(f"  parquet data files: {n_files}")
    if n_files < 2:
        print("  NOTE: table spans a single Parquet file. The read will not fan")
        print("        out across tasks, so it exercises less than intended.")
        print("        Databricks decides file layout; raise ROWS_PER_COMMIT to")
        print("        push it over the threshold.")

    print("\nOK" if ok else "\nFAILED", flush=True)
    return ok


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--warehouse-id", help="SQL warehouse to execute against (required to load)"
    )
    parser.add_argument(
        "--list-warehouses",
        action="store_true",
        help="print available SQL warehouses and exit",
    )
    parser.add_argument(
        "--verify",
        action="store_true",
        help="only check the existing table; do not rewrite it",
    )
    args = parser.parse_args(argv)

    host = os.environ.get("RAY_TEST_AZURE_DATABRICKS_HOST")
    token = os.environ.get("RAY_TEST_AZURE_DATABRICKS_TOKEN")
    table_fqn = os.environ.get("RAY_TEST_AZURE_UC_DELTA_TABLE")
    missing = [
        name
        for name, value in (
            ("RAY_TEST_AZURE_DATABRICKS_HOST", host),
            ("RAY_TEST_AZURE_DATABRICKS_TOKEN", token),
            ("RAY_TEST_AZURE_UC_DELTA_TABLE", table_fqn),
        )
        if not value
    ]
    if missing:
        parser.error(f"missing environment variables: {', '.join(missing)}")

    w = _client(host, token)
    print(f"authenticated as {w.current_user.me().user_name}", flush=True)

    if args.list_warehouses:
        list_warehouses(w)
        return 0

    if not args.warehouse_id:
        parser.error("--warehouse-id is required (see --list-warehouses)")

    parts = table_fqn.split(".")
    if len(parts) != 3:
        parser.error(
            f"RAY_TEST_AZURE_UC_DELTA_TABLE must be catalog.schema.table, "
            f"got {table_fqn!r}"
        )
    catalog, schema, table = parts

    if not args.verify:
        load(w, args.warehouse_id, catalog, schema, table)

    return 0 if verify(w, args.warehouse_id, catalog, schema, table) else 1


if __name__ == "__main__":
    sys.exit(main())
