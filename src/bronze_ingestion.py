# src/bronze_ingestion.py
import os
import gc
import pandas as pd
from datetime import datetime, timezone

from src.config import FILE_CONFIG, BRONZE_PREFIX_TEMPLATE, BRONZE_BUCKET, EXPECTED_ROWS
from src.supabase_utils import get_supabase_client, upload_to_storage
from src.io_utils import normalize_columns, verify_schema, to_parquet_bytes

def _register_manifest(manifest: list, run_id: str, name: str, path: str, part: int, rows: int, parquet_bytes: int):
    """Ghi metadata 1 part vào manifest list."""
    manifest.append({
        "run_id"        : run_id,
        "table"         : name,
        "storage_path"  : path,
        "part"          : part,
        "rows"          : rows,
        "parquet_bytes" : parquet_bytes,
        "ingested_utc"  : datetime.now(timezone.utc).isoformat(),
    })

def ingest_single_table(client, dataset_dir: str, cfg: dict, run_id: str, bronze_prefix: str, manifest: list):
    """Đọc CSV -> Kiểm tra -> Parquet -> Upload."""
    name = cfg["name"]
    chunksize = cfg.get("chunksize")
    dtypes = cfg.get("dtypes")
    
    csv_path = os.path.join(dataset_dir, f"{name}.csv")
    print(f"\n[{name}]")
    if dtypes: print(f"  Strict dtypes enabled.")

    # Đọc dữ liệu (có hoặc không có chunk)
    if chunksize is None:
        chunks = [pd.read_csv(csv_path, dtype=dtypes)]
    else:
        chunks = pd.read_csv(csv_path, chunksize=chunksize, dtype=dtypes)

    total_rows = 0
    part_num = 0

    for chunk in chunks:
        part_num += 1
        chunk = normalize_columns(chunk)
        verify_schema(chunk, name, part_num)

        payload = to_parquet_bytes(chunk)
        path = f"{bronze_prefix}/{name}/part-{part_num:05d}.parquet"

        upload_to_storage(client, BRONZE_BUCKET, path, payload)
        _register_manifest(manifest, run_id, name, path, part_num, len(chunk), len(payload))

        total_rows += len(chunk)
        print(f"  part-{part_num:05d} | rows={len(chunk):>9,} | {len(payload)/1024**2:.2f} MB")
        
        del chunk, payload
        gc.collect()

    print(f"  -> Hoàn tất: {total_rows:,} rows | {part_num} part(s)")
    return total_rows

def run_bronze_pipeline(dataset_dir: str, supabase_url: str, supabase_key: str):
    """Hàm main để chạy toàn bộ pipeline Bronze"""
    
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    from src.config import BRONZE_BASE
    bronze_prefix = f"{BRONZE_BASE}/{run_id}"
    
    print(f"RUN_ID       : {run_id}")
    print(f"BRONZE_PREFIX: {bronze_prefix}")
    
    client = get_supabase_client(supabase_url, supabase_key)
    manifest = []

    # 1. Ingest từng bảng
    for cfg in FILE_CONFIG:
        ingest_single_table(client, dataset_dir, cfg, run_id, bronze_prefix, manifest)

    # 2. Tạo Summary Report
    manifest_df = pd.DataFrame(manifest)
    summary_df = (
        manifest_df.groupby("table", as_index=False)
        .agg(rows=("rows", "sum"), parts=("part", "count"), parquet_bytes=("parquet_bytes", "sum"))
    )
    summary_df["size_mb"] = (summary_df["parquet_bytes"] / (1024 * 1024)).round(2)
    summary_df["status"] = [
        "OK" if row == EXPECTED_ROWS.get(tbl, -1) else "MISMATCH"
        for tbl, row in zip(summary_df["table"], summary_df["rows"])
    ]
    
    print(f"\nBRONZE PIPELINE SUMMARY — {run_id}\n")
    print(summary_df[["table", "rows", "parts", "size_mb", "status"]].to_string(index=False))

    # 3. Upload Manifest
    manifest_path = f"{bronze_prefix}/_manifest/bronze_manifest.csv"
    try:
        upload_to_storage(client, BRONZE_BUCKET, manifest_path, manifest_df.to_csv(index=False).encode("utf-8"))
        print(f"\nManifest uploaded: {manifest_path}")
    except Exception as e:
        print(f"\n[Lỗi Upload Manifest]: {e}")
        
    return manifest_df
