#!/usr/bin/env python3
"""
compare_parquet.py — benchmark a generated parquet file against a production one.

Only the columns that exist in the GENERATED file are compared (columns you
skipped are ignored). Rows are aligned by a key you supply (--keys); for each
shared column the tool reports how many values match, as a percentage.

Usage
-----
    python compare_parquet.py \
        --prod /path/to/production.parquet \
        --gen  /path/to/generated.parquet \
        --keys BASEL_ACCT_ID,OBSN_DT

Common options
--------------
    --keys COL[,COL...]     Join key(s) that identify a row on both sides.
                            If omitted, rows are compared by position (order),
                            which is only meaningful if both files are sorted
                            identically -- keys are strongly recommended.
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
import sys

import duckdb
import numpy as np
import pandas as pd


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


def schema_cols(path):
    return duckdb.sql("SELECT * FROM read_parquet(?) LIMIT 0", params=[path]).columns


def load_parquet(path, columns):
    cols = ", ".join(f'"{c}"' for c in columns)
    return duckdb.sql(f"SELECT {cols} FROM read_parquet(?)", params=[path]).df()


def write_parquet(df, path):
    duckdb.register("_out_df", df)
    duckdb.sql(f"COPY _out_df TO '{path}' (FORMAT PARQUET)")
    duckdb.unregister("_out_df")


def is_numeric(s):
    return pd.api.types.is_numeric_dtype(s) and not pd.api.types.is_bool_dtype(s)


def norm_text(s, trim, ci):
    out = s.astype("string")
    if trim:
        out = out.str.strip()
    if ci:
        out = out.str.upper()
    return out


def compare_column(prod_s, gen_s, atol, rtol, trim, ci):
    """Return a boolean Series: True where the two values are considered equal.
    NULL == NULL is treated as a match; NULL vs value is a mismatch."""
    both_null = prod_s.isna().values & gen_s.isna().values

    if is_numeric(prod_s) and is_numeric(gen_s):
        a = pd.to_numeric(prod_s, errors="coerce").to_numpy(dtype="float64")
        b = pd.to_numeric(gen_s, errors="coerce").to_numpy(dtype="float64")
        with np.errstate(invalid="ignore"):
            close = np.isclose(a, b, atol=atol, rtol=rtol, equal_nan=False)
        return close | both_null

    # datetime columns -> compare as timestamps
    if pd.api.types.is_datetime64_any_dtype(prod_s) and pd.api.types.is_datetime64_any_dtype(gen_s):
        eq = (prod_s.values == gen_s.values)
        return eq | both_null

    # fall back to normalized text comparison
    a = norm_text(prod_s, trim, ci)
    b = norm_text(gen_s, trim, ci)
    eq = (a.values == b.values)
    return eq | both_null


def main():
    args = parse_args()
    keys = csv_list(args.keys)

    prod_cols = schema_cols(args.prod)
    gen_cols = schema_cols(args.gen)

    # Only compare columns present in the GENERATED file.
    only = set(csv_list(args.columns))
    ignore = set(csv_list(args.ignore_columns))

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

    load_cols = keys + comparable
    prod = load_parquet(args.prod, load_cols)
    gen = load_parquet(args.gen, load_cols)

    print("=" * 72)
    print("PARQUET COMPARISON")
    print("=" * 72)
    print(f"  production : {args.prod}")
    print(f"               {len(prod):,} rows, {len(prod_cols)} columns")
    print(f"  generated  : {args.gen}")
    print(f"               {len(gen):,} rows, {len(gen_cols)} columns")
    print(f"  comparing  : {len(comparable)} shared column(s) on key {keys or '(row order)'}")
    if gen_only:
        print(f"  NOTE: {len(gen_only)} generated column(s) not in production (skipped): {gen_only}")
    if prod_skipped:
        print(f"  NOTE: {len(prod_skipped)} production column(s) you did not generate: {prod_skipped}")

    # ---- align rows -------------------------------------------------------
    if keys:
        for name, df in (("production", prod), ("generated", gen)):
            dups = df.duplicated(subset=keys).sum()
            if dups:
                print(f"  WARNING: {dups:,} duplicate key rows in {name}; keeping first of each.")
                df.drop_duplicates(subset=keys, keep="first", inplace=True)
        merged = prod.merge(gen, on=keys, how="outer", suffixes=("__prod", "__gen"), indicator=True)
        only_prod = int((merged["_merge"] == "left_only").sum())
        only_gen = int((merged["_merge"] == "right_only").sum())
        matched = merged[merged["_merge"] == "both"].copy()
        print(f"\n  row alignment: {len(matched):,} matched | "
              f"{only_prod:,} only in prod | {only_gen:,} only in gen")
    else:
        n = min(len(prod), len(gen))
        if len(prod) != len(gen):
            print(f"  WARNING: row counts differ ({len(prod):,} vs {len(gen):,}); "
                  f"comparing first {n:,} by position.")
        matched = pd.concat(
            [prod.head(n).add_suffix("__prod").reset_index(drop=True),
             gen.head(n).add_suffix("__gen").reset_index(drop=True)],
            axis=1,
        )
        only_prod = len(prod) - n
        only_gen = len(gen) - n

    n_rows = len(matched)
    if n_rows == 0:
        sys.exit("ERROR: no overlapping rows to compare.")

    # ---- per-column comparison -------------------------------------------
    rows = []
    all_match_mask = np.ones(n_rows, dtype=bool)
    mismatch_frames = []
    for col in comparable:
        ps = matched[f"{col}__prod"]
        gs = matched[f"{col}__gen"]
        eq = compare_column(ps, gs, args.atol, args.rtol, args.trim_strings, args.ci_strings)
        eq = np.asarray(eq, dtype=bool)
        all_match_mask &= eq

        n_match = int(eq.sum())
        n_mis = n_rows - n_match
        both_null = int((ps.isna().values & gs.isna().values).sum())
        rows.append({
            "column": col,
            "prod_dtype": str(ps.dtype),
            "gen_dtype": str(gs.dtype),
            "rows": n_rows,
            "match": n_match,
            "mismatch": n_mis,
            "both_null": both_null,
            "match_pct": round(100.0 * n_match / n_rows, 4),
        })

        if n_mis and (args.sample or args.out_mismatches):
            bad = matched.loc[~eq, (keys + [f"{col}__prod", f"{col}__gen"])].copy()
            if args.sample:
                print(f"\n  ── mismatches in {col} (showing {min(args.sample, len(bad))} of {len(bad):,}) ──")
                print(bad.head(args.sample).to_string(index=False))
            if args.out_mismatches:
                b = bad.rename(columns={f"{col}__prod": "prod_value", f"{col}__gen": "gen_value"})
                b.insert(len(keys), "column", col)
                mismatch_frames.append(b)

    report = pd.DataFrame(rows).sort_values("match_pct")

    print("\n" + "=" * 72)
    print("PER-COLUMN BENCHMARK  (sorted worst-first)")
    print("=" * 72)
    print(report.to_string(index=False))

    total_cells = n_rows * len(comparable)
    total_match = int(report["match"].sum())
    rows_all_match = int(all_match_mask.sum())
    print("\n" + "-" * 72)
    print(f"  OVERALL cell match : {total_match:,}/{total_cells:,} "
          f"= {100.0 * total_match / total_cells:.4f}%")
    print(f"  Fully-matching rows: {rows_all_match:,}/{n_rows:,} "
          f"= {100.0 * rows_all_match / n_rows:.4f}%  (all compared columns equal)")
    if keys:
        print(f"  Key coverage       : {n_rows:,} matched, "
              f"{only_prod:,} missing from gen, {only_gen:,} extra in gen")
    print("-" * 72)

    if args.out_csv:
        report.to_csv(args.out_csv, index=False)
        print(f"  wrote per-column benchmark -> {args.out_csv}")
    if args.out_mismatches and mismatch_frames:
        write_parquet(pd.concat(mismatch_frames, ignore_index=True), args.out_mismatches)
        print(f"  wrote mismatching rows      -> {args.out_mismatches}")


if __name__ == "__main__":
    main()
