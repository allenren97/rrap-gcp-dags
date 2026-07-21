#!/usr/bin/env python3
"""
compare_parquet.py — benchmark a generated parquet file against a production one.

Only the columns that exist in the GENERATED file are compared (columns you
skipped are ignored). Rows are aligned by a key you supply (--keys); for each
shared column the tool reports how many values match, as a percentage.

Pure DuckDB + Python stdlib — no pandas / numpy required. All comparison work
is pushed into DuckDB SQL, so it scales to large files (aggregated in-engine).

Usage
-----
    python compare_parquet.py \
        --prod /path/to/production.parquet \
        --gen  /path/to/generated.parquet \
        --keys BASEL_ACCT_ID,OBSN_DT

Common options
--------------
    --keys COL[,COL...]     Join key(s) that identify a row on both sides.
                            If omitted, rows are compared by scan position
                            (only meaningful if both files are ordered
                            identically -- keys are strongly recommended).
    --atol 0.01             Absolute tolerance for numeric columns (e.g. cents).
    --rtol 0.0              Relative tolerance for numeric columns.
    --trim-strings          Strip surrounding whitespace before comparing text.
    --ci-strings            Case-insensitive text comparison.
    --columns A,B,C         Restrict comparison to these columns only.
    --ignore-columns X,Y    Exclude these columns from comparison.
    --sample 5              Print up to N example mismatches per column.
    --out-csv report.csv    Write the per-column benchmark to CSV.
    --out-mismatches m.pq   Write mismatching rows (keys + both values) to parquet.
"""
import argparse
import csv
import sys

import duckdb

NUMERIC_PREFIXES = (
    "TINYINT", "SMALLINT", "INTEGER", "BIGINT", "HUGEINT",
    "UTINYINT", "USMALLINT", "UINTEGER", "UBIGINT", "UHUGEINT",
    "FLOAT", "DOUBLE", "REAL", "DECIMAL", "NUMERIC",
)


def parse_args():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--prod", required=True, help="production / reference parquet (the baseline)")
    p.add_argument("--gen", required=True, help="generated parquet (the one you produced)")
    p.add_argument("--keys", default="", help="comma-separated join key column(s)")
    p.add_argument("--columns", default="", help="only compare these columns")
    p.add_argument("--ignore-columns", default="", help="exclude these columns")
    p.add_argument("--atol", type=float, default=0.0, help="absolute tolerance for numeric columns")
    p.add_argument("--rtol", type=float, default=0.0, help="relative tolerance for numeric columns")
    p.add_argument("--trim-strings", action="store_true", help="strip whitespace before comparing text")
    p.add_argument("--ci-strings", action="store_true", help="case-insensitive text comparison")
    p.add_argument("--sample", type=int, default=0, help="print up to N example mismatches per column")
    p.add_argument("--out-csv", default="", help="write per-column benchmark to this CSV")
    p.add_argument("--out-mismatches", default="", help="write mismatching rows to this parquet")
    return p.parse_args()


def csv_list(s):
    return [c.strip() for c in s.split(",") if c.strip()]


def qi(name):
    """Quote an identifier."""
    return '"' + name.replace('"', '""') + '"'


def sql_str(s):
    """Quote a string literal."""
    return "'" + s.replace("'", "''") + "'"


def is_numeric(type_str):
    return type_str.upper().startswith(NUMERIC_PREFIXES)


def schema(con, path):
    """Ordered {column_name: column_type} for a parquet path."""
    info = con.execute(f"DESCRIBE SELECT * FROM read_parquet({sql_str(path)})").fetchall()
    return {r[0]: r[1] for r in info}


def row_count(con, path):
    return con.execute(f"SELECT count(*) FROM read_parquet({sql_str(path)})").fetchone()[0]


def norm_text(expr, trim, ci):
    e = f"CAST({expr} AS VARCHAR)"
    if trim:
        e = f"trim({e})"
    if ci:
        e = f"upper({e})"
    return e


def match_expr(col, both_numeric, atol, rtol, trim, ci):
    """Boolean SQL: TRUE when prod/gen values are equal (or both NULL)."""
    pa, ga = qi(f"p_{col}"), qi(f"g_{col}")
    both_null = f"({pa} IS NULL AND {ga} IS NULL)"
    if both_numeric:
        cond = f"abs({pa} - {ga}) <= ({atol} + {rtol} * abs({ga}))"
    else:
        cond = f"{norm_text(pa, trim, ci)} = {norm_text(ga, trim, ci)}"
    return f"({both_null} OR COALESCE({cond}, FALSE))", both_null


def print_table(rows, cols, right_align):
    widths = {c: max(len(c), max((len(str(r[c])) for r in rows), default=0)) for c in cols}
    def fmt(val, c):
        s = str(val)
        return s.rjust(widths[c]) if c in right_align else s.ljust(widths[c])
    print("  ".join(fmt(c, c) for c in cols))
    for r in rows:
        print("  ".join(fmt(r[c], c) for c in cols))


def main():
    args = parse_args()
    keys = csv_list(args.keys)
    con = duckdb.connect()

    prod_types = schema(con, args.prod)
    gen_types = schema(con, args.gen)
    prod_cols, gen_cols = list(prod_types), list(gen_types)

    only = set(csv_list(args.columns))
    ignore = set(csv_list(args.ignore_columns))

    # Only compare columns present in the GENERATED file.
    comparable = [c for c in gen_cols if c in prod_cols and c not in keys]
    if only:
        comparable = [c for c in comparable if c in only]
    comparable = [c for c in comparable if c not in ignore]

    gen_only = [c for c in gen_cols if c not in prod_cols and c not in keys]
    prod_skipped = [c for c in prod_cols if c not in gen_cols and c not in keys]

    missing_keys = [k for k in keys if k not in prod_cols or k not in gen_cols]
    if missing_keys:
        sys.exit(f"ERROR: key column(s) not found in both files: {missing_keys}")
    if not comparable:
        sys.exit("ERROR: no shared columns to compare (check --keys/--columns).")

    n_prod, n_gen = row_count(con, args.prod), row_count(con, args.gen)

    print("=" * 72)
    print("PARQUET COMPARISON")
    print("=" * 72)
    print(f"  production : {args.prod}")
    print(f"               {n_prod:,} rows, {len(prod_cols)} columns")
    print(f"  generated  : {args.gen}")
    print(f"               {n_gen:,} rows, {len(gen_cols)} columns")
    print(f"  comparing  : {len(comparable)} shared column(s) on key {keys or '(scan position)'}")
    if gen_only:
        print(f"  NOTE: {len(gen_only)} generated column(s) not in production (skipped): {gen_only}")
    if prod_skipped:
        print(f"  NOTE: {len(prod_skipped)} production column(s) you did not generate: {prod_skipped}")

    # ---- build the aligned join as a temp table ---------------------------
    if keys:
        join_keys = keys
        key_sel = ", ".join(qi(k) for k in keys)
        dedup = f"QUALIFY row_number() OVER (PARTITION BY {key_sel}) = 1"
    else:
        join_keys = ["_rn"]
        key_sel = "row_number() OVER () AS _rn"
        dedup = ""

    p_cols = ", ".join(f"{qi(c)} AS {qi('p_' + c)}" for c in comparable)
    g_cols = ", ".join(f"{qi(c)} AS {qi('g_' + c)}" for c in comparable)
    using = ", ".join(qi(k) for k in join_keys)

    con.execute(f"""
        CREATE TEMP TABLE _joined AS
        WITH p AS (
            SELECT {key_sel}, {p_cols}, 1 AS _p_present
            FROM read_parquet({sql_str(args.prod)}) {dedup}
        ),
        g AS (
            SELECT {key_sel}, {g_cols}, 1 AS _g_present
            FROM read_parquet({sql_str(args.gen)}) {dedup}
        )
        SELECT * FROM p FULL OUTER JOIN g USING ({using})
    """)

    if keys:
        for label, path in (("production", args.prod), ("generated", args.gen)):
            total = row_count(con, path)
            distinct = con.execute(
                f"SELECT count(*) FROM (SELECT DISTINCT {key_sel} FROM read_parquet({sql_str(path)}))"
            ).fetchone()[0]
            if total != distinct:
                print(f"  WARNING: {total - distinct:,} duplicate-key rows in {label}; "
                      f"kept first of each (add more --keys to avoid).")

    both_present = "(_p_present IS NOT NULL AND _g_present IS NOT NULL)"
    p_only = "(_p_present IS NOT NULL AND _g_present IS NULL)"
    g_only = "(_g_present IS NOT NULL AND _p_present IS NULL)"

    # ---- one aggregate pass over the join --------------------------------
    labels, exprs, match_sql = [], [], {}
    def add(label, expr):
        labels.append(label)
        exprs.append(f"{expr} AS {qi(label)}")

    add("matched_rows", f"COUNT(*) FILTER (WHERE {both_present})")
    add("only_prod", f"COUNT(*) FILTER (WHERE {p_only})")
    add("only_gen", f"COUNT(*) FILTER (WHERE {g_only})")
    for col in comparable:
        both_num = is_numeric(prod_types[col]) and is_numeric(gen_types[col])
        m, both_null = match_expr(col, both_num, args.atol, args.rtol, args.trim_strings, args.ci_strings)
        match_sql[col] = m
        add(f"m__{col}", f"COUNT(*) FILTER (WHERE {both_present} AND ({m}))")
        add(f"bn__{col}", f"COUNT(*) FILTER (WHERE {both_present} AND ({both_null}))")
    fully = " AND ".join(f"({m})" for m in match_sql.values())
    add("fully_match", f"COUNT(*) FILTER (WHERE {both_present} AND {fully})")

    vals = dict(zip(labels, con.execute(f"SELECT {', '.join(exprs)} FROM _joined").fetchone()))

    matched = vals["matched_rows"]
    print(f"\n  row alignment: {matched:,} matched | "
          f"{vals['only_prod']:,} only in prod | {vals['only_gen']:,} only in gen")
    if matched == 0:
        sys.exit("ERROR: no overlapping rows to compare.")

    # ---- per-column benchmark --------------------------------------------
    report = []
    for col in comparable:
        m = vals[f"m__{col}"]
        report.append({
            "column": col,
            "prod_dtype": prod_types[col],
            "gen_dtype": gen_types[col],
            "rows": matched,
            "match": m,
            "mismatch": matched - m,
            "both_null": vals[f"bn__{col}"],
            "match_pct": round(100.0 * m / matched, 4),
        })
    report.sort(key=lambda r: r["match_pct"])

    print("\n" + "=" * 72)
    print("PER-COLUMN BENCHMARK  (sorted worst-first)")
    print("=" * 72)
    print_table(report,
                ["column", "prod_dtype", "gen_dtype", "rows", "match", "mismatch", "both_null", "match_pct"],
                right_align={"rows", "match", "mismatch", "both_null", "match_pct"})

    total_cells = matched * len(comparable)
    total_match = sum(r["match"] for r in report)
    print("\n" + "-" * 72)
    print(f"  OVERALL cell match : {total_match:,}/{total_cells:,} "
          f"= {100.0 * total_match / total_cells:.4f}%")
    print(f"  Fully-matching rows: {vals['fully_match']:,}/{matched:,} "
          f"= {100.0 * vals['fully_match'] / matched:.4f}%  (all compared columns equal)")
    print(f"  Key coverage       : {matched:,} matched, "
          f"{vals['only_prod']:,} missing from gen, {vals['only_gen']:,} extra in gen")
    print("-" * 72)

    # ---- optional: sample mismatches per column --------------------------
    if args.sample:
        key_show = ", ".join(qi(k) for k in join_keys)
        for col in comparable:
            if report_lookup(report, col)["mismatch"] == 0:
                continue
            rows = con.execute(f"""
                SELECT {key_show}, {qi('p_' + col)} AS prod_value, {qi('g_' + col)} AS gen_value
                FROM _joined
                WHERE {both_present} AND NOT ({match_sql[col]})
                LIMIT {args.sample}
            """).fetchall()
            cols = list(join_keys) + ["prod_value", "gen_value"]
            print(f"\n  ── mismatches in {col} (showing {len(rows)}) ──")
            print_table([dict(zip(cols, r)) for r in rows], cols, right_align=set())

    # ---- optional: dump all mismatching rows -----------------------------
    if args.out_mismatches:
        key_show = ", ".join(qi(k) for k in join_keys)
        parts = [
            f"""SELECT {key_show}, {sql_str(col)} AS column,
                       CAST({qi('p_' + col)} AS VARCHAR) AS prod_value,
                       CAST({qi('g_' + col)} AS VARCHAR) AS gen_value
                FROM _joined
                WHERE {both_present} AND NOT ({match_sql[col]})"""
            for col in comparable
        ]
        con.execute(
            f"COPY ({' UNION ALL '.join(parts)}) TO {sql_str(args.out_mismatches)} (FORMAT PARQUET)"
        )
        print(f"  wrote mismatching rows      -> {args.out_mismatches}")

    # ---- optional: CSV benchmark -----------------------------------------
    if args.out_csv:
        with open(args.out_csv, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(report[0].keys()))
            w.writeheader()
            w.writerows(report)
        print(f"  wrote per-column benchmark  -> {args.out_csv}")


def report_lookup(report, col):
    for r in report:
        if r["column"] == col:
            return r
    return {"mismatch": 0}


if __name__ == "__main__":
    main()
