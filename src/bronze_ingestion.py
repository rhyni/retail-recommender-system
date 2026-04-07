import gc
import os
import time
from datetime import datetime, timezone

import pandas as pd

from io_utils import normalize_columns, to_parquet_bytes, upload_to_storage


def verify_schema(df: pd.DataFrame, table_name: str, expected_cols: dict):
    """
    Check schema của 1 chunk.
    - thiếu cột bắt buộc: raise lỗi
    - có cột thừa: chỉ cảnh báo
    """
    expected = expected_cols[table_name]
    actual = df.columns.tolist()

    missing = [col for col in expected if col not in actual]
    if missing:
        raise ValueError(
            f"[SCHEMA ERROR] Table '{table_name}' thiếu cột bắt buộc: {missing}"
        )

    extra = [col for col in actual if col not in expected]
    return {
        "missing": missing,
        "extra": extra,
    }


def register_manifest(
    manifest: list,
    table_name: str,
    source_file: str,
    storage_path: str,
    part_num: int,
    df: pd.DataFrame,
    parquet_bytes_len: int,
):
    """
    Ghi metadata của mỗi part vào manifest.
    """
    manifest.append(
        {
            "table": table_name,
            "source_file": source_file,
            "storage_path": storage_path,
            "part": part_num,
            "rows": len(df),
            "columns": len(df.columns),
            "column_names": ",".join(df.columns.tolist()),
            "parquet_bytes": parquet_bytes_len,
            "ingested_at_utc": datetime.now(timezone.utc).isoformat(),
        }
    )


def safe_upload_parquet(
    supabase,
    bucket: str,
    storage_path: str,
    payload: bytes,
    max_retries: int = 3,
):
    """
    Upload có retry.
    - duplicate / already exists / 409: bỏ qua
    - lỗi tạm thời: retry
    """
    for attempt in range(1, max_retries + 1):
        try:
            upload_to_storage(
                supabase=supabase,
                bucket=bucket,
                storage_path=storage_path,
                payload=payload,
                content_type="application/octet-stream",
                upsert="false",
            )
            return "uploaded"
        except Exception as e:
            msg = str(e).lower()

            if any(k in msg for k in ("duplicate", "already exists", "409")):
                return "skipped_duplicate"

            if attempt < max_retries:
                time.sleep(attempt * 2)
            else:
                raise


def ingest_table(
    supabase,
    dataset_dir: str,
    bucket: str,
    bronze_prefix: str,
    table_name: str,
    chunksize,
    dtypes,
    expected_cols: dict,
    manifest: list,
):
    """
    Read CSV -> normalize -> schema check -> parquet -> upload -> manifest.
    """
    csv_path = os.path.join(dataset_dir, f"{table_name}.csv")
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"Không tìm thấy file: {csv_path}")

    print(f"\n[{table_name}]")
    print(f"CSV path: {csv_path}")

    if chunksize is None:
        chunks = [pd.read_csv(csv_path, dtype=dtypes)]
    else:
        chunks = pd.read_csv(csv_path, chunksize=chunksize, dtype=dtypes)

    total_rows = 0
    total_parts = 0

    for part_num, chunk in enumerate(chunks, start=1):
        chunk = normalize_columns(chunk)

        schema_info = verify_schema(
            df=chunk,
            table_name=table_name,
            expected_cols=expected_cols,
        )

        if schema_info["extra"]:
            print(f"  [WARN] Extra cols: {schema_info['extra']}")

        payload = to_parquet_bytes(chunk)
        storage_path = f"{bronze_prefix}/{table_name}/part-{part_num:05d}.parquet"

        status = safe_upload_parquet(
            supabase=supabase,
            bucket=bucket,
            storage_path=storage_path,
            payload=payload,
        )

        register_manifest(
            manifest=manifest,
            table_name=table_name,
            source_file=os.path.basename(csv_path),
            storage_path=storage_path,
            part_num=part_num,
            df=chunk,
            parquet_bytes_len=len(payload),
        )

        total_rows += len(chunk)
        total_parts += 1

        print(
            f"  part-{part_num:05d} | rows={len(chunk):>10,} | "
            f"size={len(payload)/1024**2:>8.2f} MB | status={status}"
        )

        del chunk, payload
        gc.collect()

    print(f"  -> done | rows={total_rows:,} | parts={total_parts}")
    return {
        "table": table_name,
        "rows": total_rows,
        "parts": total_parts,
    }


def build_summary(manifest: list, expected_rows: dict) -> pd.DataFrame:
    """
    Tạo bảng summary từ manifest.
    """
    manifest_df = pd.DataFrame(manifest)

    if manifest_df.empty:
        return pd.DataFrame(columns=["table", "rows", "parts", "size_mb", "status"])

    summary_df = (
        manifest_df.groupby("table", as_index=False)
        .agg(
            rows=("rows", "sum"),
            parts=("part", "count"),
            parquet_bytes=("parquet_bytes", "sum"),
        )
    )

    summary_df["size_mb"] = (summary_df["parquet_bytes"] / (1024 * 1024)).round(2)
    summary_df["status"] = summary_df.apply(
        lambda row: "OK" if row["rows"] == expected_rows.get(row["table"], -1) else "MISMATCH",
        axis=1,
    )

    summary_df = summary_df[["table", "rows", "parts", "size_mb", "status"]]
    return summary_df


def upload_manifest_csv(supabase, bucket: str, bronze_prefix: str, manifest_df: pd.DataFrame):
    """
    Upload manifest CSV lên storage.
    """
    manifest_path = f"{bronze_prefix}/_manifest/bronze_manifest.csv"
    payload = manifest_df.to_csv(index=False).encode("utf-8")

    try:
        upload_to_storage(
            supabase=supabase,
            bucket=bucket,
            storage_path=manifest_path,
            payload=payload,
            content_type="text/csv",
            upsert="false",
        )
        print(f"\nManifest uploaded: {manifest_path}")
    except Exception as e:
        msg = str(e).lower()
        if any(k in msg for k in ("duplicate", "already exists", "409")):
            print(f"\n[BỎ QUA] Manifest đã tồn tại: {manifest_path}")
        else:
            raise

    return manifest_path
