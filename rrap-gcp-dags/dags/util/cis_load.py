"""
Fixed-width CIS1..CIS8 -> Hive-partitioned Parquet.

Port of J_RRAP_MOR_SRC_0130_BNS_CIS_DATA_NEW_FINAL.sas. Phase 1 parses each CIS
file to a staging Parquet in bounded-memory batches; Phase 2 applies the SAS
per-file NODUPKEY on primaries and writes <out_root>/FILE_YR_MTH=YYMM/.
"""
from __future__ import annotations

import datetime as dt
import logging
import multiprocessing as mp
import os
import shutil
import tempfile
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

log = logging.getLogger(__name__)

LAYOUT = {
    "CIFKEY":        (1, 17),
    "PRODUCT":       (18, 20),
    "ACCOUNT_RAW":   (21, 33),
    "CID_RAW":       (34, 49),
    "PRIMARY_FLAG":  (90, 90),
    "RELATION_CODE": (91, 93),
    "CUST_TYPE":     (94, 95),
}

LEN_ACCOUNT = 23
LEN_CID     = 15
LEN_MORT_NO = 7
LEN_CAB     = 5
LEN_LOAN_NO = 7

BATCH_ROWS     = 500_000
ROW_GROUP_ROWS = 1_000_000
STAGING_COMP   = "snappy"
FINAL_COMP     = "zstd"
FINAL_COMP_LVL = 3

# SAS blank-pads the shorter literal, so RELATION_CODE='X' matches the raw 'X  '.
# Keys are stripped and the lookup strips too, otherwise the 'X' and ' ' rules
# never fire against the 3-char raw field.
_RAW_REMAP = {
    ("SOL", "S"): "Y", ("GTR", "Y"): "N", ("SOL", "C"): "Y", ("BOR", "C"): "Y",
    ("LFB", " "): "N", ("TSC", "T"): "Y", ("GTR", "C"): "N", ("COB", "C"): "N",
    ("TRT", "T"): "N", ("GTR", "A"): "N", ("BOR", "B"): "Y", ("COS", "Y"): "N",
    ("COS", "A"): "N", ("SOL", "A"): "Y", ("X",   "N"): "N", ("BOR", "X"): "Y",
    (" ",   " "): "N", ("TR1", "T"): "Y", ("SOL", " "): "Y", ("JNT", "J"): "N",
    ("BOR", "A"): "Y", ("JN1", "J"): "Y", ("COS", "C"): "N", ("TRU", "T"): "N",
    ("GTR", "G"): "N", ("SOL", "N"): "Y", ("GTR", "X"): "N", ("COB", "A"): "N",
    ("BOR", "N"): "Y", ("COB", "Y"): "N",
}
PRIMARY_FLAG_REMAP = {(rel.strip(), pf): new for (rel, pf), new in _RAW_REMAP.items()}

STAGING_SCHEMA = pa.schema([
    ("CIFKEY",        pa.string()),
    ("PRODUCT",       pa.string()),
    ("ACCOUNT",       pa.string()),
    ("CID",           pa.string()),
    ("PRIMARY_FLAG",  pa.string()),
    ("RELATION_CODE", pa.string()),
    ("CUST_TYPE",     pa.string()),
    ("EMPLOYEE_IND",  pa.string()),
    ("MORT_NO",       pa.string()),
    ("CAB",           pa.string()),
    ("LOAN_NO",       pa.string()),
    ("FILE_YR_MTH",   pa.string()),
    ("LOAD_DATE_TM",  pa.timestamp("us")),
    ("FILE_DATE",     pa.date32()),
    ("SRC_FILE",      pa.string()),
    ("SRC_ROW",       pa.int64()),
])


def _read_field(line: str, start: int, end: int) -> str:
    padded = line if len(line) >= end else line.ljust(end)
    return padded[start - 1:end]


def _left_pad_zero(value: str, width: int) -> str:
    stripped = value.strip()
    if len(stripped) >= width:
        return stripped[:width]
    return stripped.rjust(width, "0")


def derive_file_yr_mth(start_period_dt: dt.date) -> str:
    """SAS: substr(compress(year||mthchar), 3) -> 4-char YYMM."""
    return f"{start_period_dt.year % 100:02d}{start_period_dt.month:02d}"


def _parse_line(line: str):
    cifkey        = _read_field(line, *LAYOUT["CIFKEY"])
    product       = _read_field(line, *LAYOUT["PRODUCT"])
    account_raw   = _read_field(line, *LAYOUT["ACCOUNT_RAW"])
    cid_raw       = _read_field(line, *LAYOUT["CID_RAW"])[:LEN_CID]
    primary_flag  = _read_field(line, *LAYOUT["PRIMARY_FLAG"])
    relation_code = _read_field(line, *LAYOUT["RELATION_CODE"])
    cust_type     = _read_field(line, *LAYOUT["CUST_TYPE"])

    if (not account_raw.strip() and not cid_raw.strip() and not cifkey.strip()) \
            or product == "APP":
        return None

    # MORT_NO / CAB / LOAN_NO come off the left-justified raw ACCOUNT, before the
    # zero-pad, matching the SAS statement order.
    account_padded = account_raw.ljust(LEN_ACCOUNT)
    mort_no = account_padded[6:6 + LEN_MORT_NO] if product == "MOR" else ""
    cab     = account_padded[1:1 + LEN_CAB]     if product in ("SPL", "OLL", "SAV", "DDA") else ""
    loan_no = account_padded[6:6 + LEN_LOAN_NO] if product in ("SPL", "OLL", "SAV", "DDA") else ""

    primary_flag = PRIMARY_FLAG_REMAP.get((relation_code.strip(), primary_flag), primary_flag)

    return (
        cifkey, product,
        _left_pad_zero(account_raw, LEN_ACCOUNT),
        _left_pad_zero(cid_raw, LEN_CID),
        primary_flag, relation_code, cust_type,
        "", mort_no, cab, loan_no,
    )


def _stage_one_file(args):
    src_path, staging_path, file_yr_mth, load_iso, file_date_iso = args
    load_date_tm = dt.datetime.fromisoformat(load_iso)
    file_date    = dt.date.fromisoformat(file_date_iso)
    src_name     = Path(src_path).name

    cols = STAGING_SCHEMA.names
    blank = lambda: {c: [] for c in cols}
    batch = blank()
    kept = 0
    seen = 0

    writer = pq.ParquetWriter(str(staging_path), STAGING_SCHEMA, compression=STAGING_COMP)
    try:
        with open(src_path, "r", encoding="latin-1", newline="") as fh:
            for raw in fh:
                line = raw.rstrip("\r\n")
                if not line:
                    continue
                seen += 1
                r = _parse_line(line)
                if r is None:
                    continue
                (batch["CIFKEY"].append(r[0]),  batch["PRODUCT"].append(r[1]))
                batch["ACCOUNT"].append(r[2]);        batch["CID"].append(r[3])
                batch["PRIMARY_FLAG"].append(r[4]);   batch["RELATION_CODE"].append(r[5])
                batch["CUST_TYPE"].append(r[6]);      batch["EMPLOYEE_IND"].append(r[7])
                batch["MORT_NO"].append(r[8]);        batch["CAB"].append(r[9])
                batch["LOAN_NO"].append(r[10])
                batch["FILE_YR_MTH"].append(file_yr_mth)
                batch["LOAD_DATE_TM"].append(load_date_tm)
                batch["FILE_DATE"].append(file_date)
                batch["SRC_FILE"].append(src_name)
                batch["SRC_ROW"].append(seen)

                kept += 1
                if kept % BATCH_ROWS == 0:
                    writer.write_table(pa.table(batch, schema=STAGING_SCHEMA))
                    batch = blank()
        if batch["CIFKEY"]:
            writer.write_table(pa.table(batch, schema=STAGING_SCHEMA))
    finally:
        writer.close()

    return src_name, kept


def phase1_stage(incoming_dir: Path, staging_dir: Path,
                 start_period_dt: dt.date, workers: int) -> int:
    file_yr_mth   = derive_file_yr_mth(start_period_dt)
    load_iso      = dt.datetime.now().isoformat(timespec="microseconds")
    file_date_iso = start_period_dt.replace(day=1).isoformat()

    staging_dir.mkdir(parents=True, exist_ok=True)

    tasks = []
    for i in range(1, 9):
        src = incoming_dir / f"CIS{i}"
        if not src.exists():
            log.warning("missing %s (skipping)", src)
            continue
        tasks.append((str(src), str(staging_dir / f"CIS{i}.parquet"),
                      file_yr_mth, load_iso, file_date_iso))

    if not tasks:
        raise FileNotFoundError(f"no CIS1..CIS8 under {incoming_dir}")

    workers = max(1, min(workers, len(tasks), mp.cpu_count()))
    total = 0

    def _sequential():
        n = 0
        for t in tasks:
            name, c = _stage_one_file(t)
            log.info("staged %-6s rows=%d", name, c)
            n += c
        return n

    if workers == 1:
        return _sequential()

    try:
        with ProcessPoolExecutor(max_workers=workers) as pool:
            futures = [pool.submit(_stage_one_file, t) for t in tasks]
            for fut in as_completed(futures):
                name, c = fut.result()
                log.info("staged %-6s rows=%d", name, c)
                total += c
    except Exception as exc:
        # Airflow workers may run daemonic (no child processes) or the pool may
        # break outright. Retry sequentially so a real parse error surfaces there.
        log.warning("parallel staging failed (%s: %s); retrying sequentially",
                    type(exc).__name__, exc)
        return _sequential()

    return total


def phase2_write_partitioned(staging_dir: Path, out_root: Path,
                             start_period_dt: dt.date) -> int:
    file_yr_mth = derive_file_yr_mth(start_period_dt)

    partition_dir = out_root / f"FILE_YR_MTH={file_yr_mth}"
    if partition_dir.exists():
        shutil.rmtree(partition_dir)
    out_root.mkdir(parents=True, exist_ok=True)

    staging_glob = str(staging_dir / "CIS*.parquet").replace("\\", "/")
    out_root_pq  = str(out_root).replace("\\", "/")

    con = duckdb.connect(":memory:")
    con.execute(f"PRAGMA threads = {max(2, mp.cpu_count())}")

    # SAS: PROC SORT NODUPKEY BY PRODUCT ACCOUNT PRIMARY_FLAG on the PRIMARY_FLAG='Y'
    # subset, per file. NODUPKEY keeps the first row in input order -> ORDER BY SRC_ROW.
    con.execute(f"""
        COPY (
            WITH staging AS (SELECT * FROM read_parquet('{staging_glob}')),
            primary_dedup AS (
                SELECT * EXCLUDE (SRC_FILE, SRC_ROW)
                FROM staging
                WHERE PRIMARY_FLAG = 'Y'
                QUALIFY row_number() OVER (
                    PARTITION BY SRC_FILE, PRODUCT, ACCOUNT ORDER BY SRC_ROW
                ) = 1
            ),
            non_primary AS (
                SELECT * EXCLUDE (SRC_FILE, SRC_ROW)
                FROM staging WHERE PRIMARY_FLAG <> 'Y'
            )
            SELECT * FROM non_primary
            UNION ALL
            SELECT * FROM primary_dedup
        ) TO '{out_root_pq}' (
            FORMAT PARQUET,
            COMPRESSION '{FINAL_COMP}',
            COMPRESSION_LEVEL {FINAL_COMP_LVL},
            PARTITION_BY (FILE_YR_MTH),
            ROW_GROUP_SIZE {ROW_GROUP_ROWS},
            OVERWRITE_OR_IGNORE
        )
    """)

    n = con.execute(
        f"SELECT COUNT(*) FROM read_parquet('{out_root_pq}/**/*.parquet', "
        f"hive_partitioning=true) WHERE FILE_YR_MTH = ?", [file_yr_mth]
    ).fetchone()[0]
    con.close()
    return n


def run_cis_load(incoming_dir, out_root, start_period_dt: dt.date,
                 workers: int = 8, staging_dir=None,
                 force: bool = False, lock_timeout: int = 3600) -> dict:
    """
    Parse CIS1..CIS8 into <out_root>/FILE_YR_MTH=YYMM/.

    Idempotent and safe to invoke concurrently: the first caller takes an atomic
    lock directory and does the work, the others wait for the _SUCCESS marker and
    return skipped=True. This matters because the emulated task group is generated
    once per stream, so up to three DAGs can call this for the same month.
    """
    incoming_dir = Path(incoming_dir)
    out_root     = Path(out_root)
    file_yr_mth  = derive_file_yr_mth(start_period_dt)

    marker = out_root / f"FILE_YR_MTH={file_yr_mth}" / "_SUCCESS"
    lock   = out_root / f".lock_FILE_YR_MTH={file_yr_mth}"

    if marker.exists() and not force:
        log.info("FILE_YR_MTH=%s already built; skipping parse", file_yr_mth)
        return {"file_yr_mth": file_yr_mth, "rows": None, "skipped": True}

    out_root.mkdir(parents=True, exist_ok=True)
    try:
        os.mkdir(lock)
    except FileExistsError:
        waited = 0
        while waited < lock_timeout:
            if marker.exists():
                log.info("another run built FILE_YR_MTH=%s; skipping", file_yr_mth)
                return {"file_yr_mth": file_yr_mth, "rows": None, "skipped": True}
            time.sleep(10)
            waited += 10
        raise TimeoutError(
            f"lock {lock} held for >{lock_timeout}s without producing {marker}")

    own_staging = staging_dir is None
    staging = Path(tempfile.mkdtemp(prefix="cis_stage_")) if own_staging else Path(staging_dir)
    try:
        staged = phase1_stage(incoming_dir, staging, start_period_dt, workers)
        rows   = phase2_write_partitioned(staging, out_root, start_period_dt)
        marker.parent.mkdir(parents=True, exist_ok=True)
        marker.write_text(f"{dt.datetime.now().isoformat()}\nrows={rows}\nstaged={staged}\n")
        log.info("FILE_YR_MTH=%s wrote %d rows", file_yr_mth, rows)
        return {"file_yr_mth": file_yr_mth, "rows": rows, "skipped": False}
    finally:
        if own_staging:
            shutil.rmtree(staging, ignore_errors=True)
        shutil.rmtree(lock, ignore_errors=True)
