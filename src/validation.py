"""
Manifest Validation for Pipeline

Giải quyết:
1. Phát hiện lỗi data sớm (trước khi chạy notebook dài)
2. Ensure data quality giữa các layer
3. Trace lại data lineage

Usage:
    from validation import validate_upstream
    
    result = validate_upstream(
        current_run=run,
        upstream_layer="bronze",
        expected_tables={
            "products": {"min_rows": 40000, "cols": ["product_id", "product_name"]},
            "orders": {"min_rows": 3000000, "cols": ["order_id", "user_id"]}
        }
    )
    
    if not result["valid"]:
        raise RuntimeError(f"Validation failed: {result['errors']}")
"""

import pandas as pd
from typing import Dict, List, Optional, Any
import io

from run_config import RunConfig, get_upstream_run_config


def validate_upstream(
    current_run: RunConfig,
    upstream_layer: str,
    expected_tables: Dict[str, Dict[str, Any]],
    upstream_user_id: Optional[str] = None
) -> Dict[str, Any]:
    """
    Validate manifest từ upstream layer.
    
    Args:
        current_run: RunConfig của notebook hiện tại
        upstream_layer: Layer cần validate (e.g., "bronze")
        expected_tables: Expected tables và specs
            Format: {
                "table_name": {
                    "min_rows": 1000,  # minimum row count
                    "cols": ["col1", "col2"],  # expected columns
                }
            }
        upstream_user_id: User_id của upstream (default: same as current)
    
    Returns:
        {
            "valid": True/False,
            "errors": [...],  # list of error messages
            "summary": {...},  # summary stats if valid
            "upstream_run_id": "...",  # RUN_ID được validate
        }
    
    Example:
        >>> result = validate_upstream(
        ...     current_run=silver_run,
        ...     upstream_layer="bronze",
        ...     expected_tables={
        ...         "products": {"min_rows": 40000, "cols": ["product_id"]}
        ...     }
        ... )
        >>> if not result["valid"]:
        ...     raise RuntimeError(result["errors"])
    """
    from supabase_utils import get_supabase_client
    
    # Get upstream RunConfig
    upstream_run = get_upstream_run_config(
        current_run, 
        upstream_layer,
        upstream_user_id
    )
    
    upstream_run_id = upstream_run.get_run_id()
    
    # DRY_RUN mode → skip validation
    if upstream_run_id == "DRY_RUN":
        return {
            "valid": True,
            "errors": [],
            "summary": {"note": "DRY_RUN mode - validation skipped"},
            "upstream_run_id": "DRY_RUN"
        }
    
    supabase = get_supabase_client()
    
    # Read manifest
    manifest_path = upstream_run.get_full_path("_manifest")
    manifest_file = f"{manifest_path}/manifest.csv"
    
    try:
        raw = supabase.storage.from_(upstream_run.bucket).download(manifest_file)
        manifest = pd.read_csv(io.BytesIO(raw))
    except Exception as e:
        return {
            "valid": False,
            "errors": [f"Cannot read manifest: {manifest_file}. Error: {e}"],
            "summary": None,
            "upstream_run_id": upstream_run_id
        }
    
    errors = []
    
    # Check 1: Tables exist
    actual_tables = set(manifest["table_name"].unique())
    expected_table_names = set(expected_tables.keys())
    missing = expected_table_names - actual_tables
    
    if missing:
        errors.append(f"Missing tables: {missing}")
    
    # Check 2: Row counts
    summary = manifest.groupby("table_name")["rows"].sum()
    
    for table, spec in expected_tables.items():
        if table not in summary:
            continue
        
        actual_rows = summary[table]
        min_rows = spec.get("min_rows", 0)
        
        if actual_rows < min_rows:
            errors.append(
                f"Table '{table}': has {actual_rows:,} rows, "
                f"expected at least {min_rows:,}"
            )
    
    # Check 3: Columns (sample 1 part from each table)
    from io_utils import read_parquet_from_storage
    
    for table, spec in expected_tables.items():
        expected_cols = spec.get("cols")
        if not expected_cols:
            continue
        
        try:
            sample = read_parquet_from_storage(
                supabase,
                upstream_run.bucket,
                upstream_run.get_full_path(table),
                n_parts=1
            )
            
            actual_cols = set(sample.columns)
            missing_cols = set(expected_cols) - actual_cols
            
            if missing_cols:
                errors.append(
                    f"Table '{table}' missing columns: {missing_cols}"
                )
        
        except Exception as e:
            errors.append(f"Cannot read sample from '{table}': {e}")
    
    # Build result
    valid = len(errors) == 0
    
    return {
        "valid": valid,
        "errors": errors,
        "summary": summary.to_dict() if valid else None,
        "upstream_run_id": upstream_run_id
    }


def create_manifest_entry(
    table_name: str,
    path: str,
    df: pd.DataFrame,
    byte_size: int,
    notes: str = ""
) -> Dict[str, Any]:
    """
    Tạo 1 entry cho manifest.
    
    Args:
        table_name: Tên table
        path: Storage path
        df: DataFrame (để lấy row count và columns)
        byte_size: Size in bytes
        notes: Optional notes
    
    Returns:
        Manifest entry dict
    
    Example:
        >>> entry = create_manifest_entry(
        ...     "products",
        ...     "instacart/silver/20260408_thuy/products/part-00001.parquet",
        ...     products_df,
        ...     123456,
        ...     "cleaned and validated"
        ... )
    """
    return {
        "table_name": table_name,
        "path": path,
        "rows": len(df),
        "columns": len(df.columns),
        "byte_size": byte_size,
        "notes": notes,
        "schema": "|".join(df.columns)
    }


def save_manifest(
    run: RunConfig,
    manifest_entries: List[Dict[str, Any]]
) -> str:
    """
    Save manifest CSV lên Supabase.
    
    Args:
        run: RunConfig
        manifest_entries: List of manifest entries
    
    Returns:
        Manifest path
    
    Example:
        >>> manifest_entries = [entry1, entry2, ...]
        >>> path = save_manifest(run, manifest_entries)
        >>> print(f"Manifest saved to: {path}")
    """
    from io_utils import upload_to_storage
    from supabase_utils import get_supabase_client
    
    if not run.should_upload():
        return "DRY_RUN - manifest not saved"
    
    manifest_df = pd.DataFrame(manifest_entries)
    manifest_path = f"{run.get_full_path('_manifest')}/manifest.csv"
    
    supabase = get_supabase_client()
    
    upload_to_storage(
        supabase,
        run.bucket,
        manifest_path,
        manifest_df.to_csv(index=False).encode("utf-8")
    )
    
    return manifest_path
