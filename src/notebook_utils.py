"""
Notebook Setup and Summary Utilities

Standardize notebook structure:
1. Cell 1: notebook_setup()
2. Cell 2: RunConfig
3. Cell 3-N: business logic
4. Cell last: notebook_summary()

Usage:
    # Cell 1
    from notebook_utils import notebook_setup, notebook_summary
    notebook_setup("01-bronze-ingestion", "Load CSV to Bronze layer")
    
    # ... business logic ...
    
    # Cell last
    notebook_summary(run, manifest_df, {"total_products": 49688})
"""

import gc
import pandas as pd
from datetime import datetime
from typing import Dict, Any, Optional

from run_config import RunConfig


def notebook_setup(
    notebook_name: str,
    description: str,
    show_env_info: bool = False
) -> None:
    """
    Setup đầu notebook.
    
    Args:
        notebook_name: Tên notebook (e.g., "01-bronze-ingestion")
        description: Mô tả ngắn gọn
        show_env_info: Show Python/Pandas version info
    
    Example:
        >>> notebook_setup(
        ...     "01-bronze-ingestion",
        ...     "Load raw CSV from Kaggle to Bronze Parquet on Supabase"
        ... )
    """
    print("=" * 80)
    print(f"NOTEBOOK: {notebook_name}")
    print(f"DESCRIPTION: {description}")
    print(f"START TIME: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)
    
    if show_env_info:
        import sys
        print(f"Python: {sys.version.split()[0]}")
        print(f"Pandas: {pd.__version__}")
        print("-" * 80)
    
    gc.collect()


def notebook_summary(
    run: RunConfig,
    manifest_df: Optional[pd.DataFrame] = None,
    custom_metrics: Optional[Dict[str, Any]] = None,
    show_next_steps: bool = True
) -> None:
    """
    Summary cuối notebook.
    
    Args:
        run: RunConfig instance
        manifest_df: Manifest DataFrame (nếu có data written)
        custom_metrics: Custom metrics để show
        show_next_steps: Show suggested next steps
    
    Example:
        >>> notebook_summary(
        ...     run=run,
        ...     manifest_df=pd.DataFrame(MANIFEST),
        ...     custom_metrics={
        ...         "total_users": 206209,
        ...         "total_products": 49688,
        ...         "bronze_to_silver_ratio": 0.98
        ...     }
        ... )
    """
    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)
    
    # Pipeline config
    print(f"Mode       : {run.mode}")
    print(f"User ID    : {run.user_id}")
    print(f"Layer      : {run.layer}")
    print(f"RUN_ID     : {run.get_run_id()}")
    print(f"Bucket     : {run.bucket}")
    print(f"Path       : {run.base_path}")
    print(f"Upload     : {'YES' if run.should_upload() else 'NO (DRY_RUN)'}")
    print(f"End time   : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Data written
    if manifest_df is not None and not manifest_df.empty:
        print("\n" + "-" * 80)
        print("DATA WRITTEN:")
        summary = (
            manifest_df
            .groupby("table_name", as_index=False)
            .agg(
                total_rows=("rows", "sum"),
                parts=("table_name", "count"),
                total_mb=("byte_size", lambda x: round(x.sum() / 1024**2, 2))
            )
            .sort_values("total_rows", ascending=False)
        )
        print(summary.to_string(index=False))
    
    # Custom metrics
    if custom_metrics:
        print("\n" + "-" * 80)
        print("CUSTOM METRICS:")
        for k, v in custom_metrics.items():
            if isinstance(v, (int, float)):
                if isinstance(v, int):
                    print(f"  {k:.<50} {v:>15,}")
                else:
                    print(f"  {k:.<50} {v:>15.4f}")
            else:
                print(f"  {k:.<50} {str(v):>15}")
    
    # Next steps
    if show_next_steps and run.should_upload():
        print("\n" + "-" * 80)
        print("NEXT STEPS:")
        
        layer_next = {
            "bronze": "02-silver-transform",
            "silver": "03-statistical-inference or 04-eda",
            "gold": "07-bi-export",
            "analysis": "05-customer-clustering or 06-recommender-system",
        }
        
        next_nb = layer_next.get(run.layer)
        if next_nb:
            print(f"  → Run notebook: {next_nb}")
            print(f"  → Use RUN_ID: {run.get_run_id()}")
    
    print("=" * 80)


def print_dataframe_info(
    df: pd.DataFrame,
    name: str = "DataFrame",
    max_rows: int = 5,
    show_dtypes: bool = True,
    show_memory: bool = True
) -> None:
    """
    Print DataFrame info trong notebook (for debugging).
    
    Args:
        df: DataFrame to inspect
        name: Name để display
        max_rows: Number of rows to show
        show_dtypes: Show data types
        show_memory: Show memory usage
    
    Example:
        >>> print_dataframe_info(products_df, "Products", max_rows=3)
    """
    print(f"\n{'─' * 80}")
    print(f"{name}")
    print(f"{'─' * 80}")
    print(f"Shape: {df.shape[0]:,} rows × {df.shape[1]} columns")
    
    if show_memory:
        mem_mb = df.memory_usage(deep=True).sum() / 1024**2
        print(f"Memory: {mem_mb:.2f} MB")
    
    if show_dtypes:
        print(f"\nData types:")
        dtype_counts = df.dtypes.value_counts()
        for dtype, count in dtype_counts.items():
            print(f"  {str(dtype):15} : {count} columns")
    
    print(f"\nFirst {max_rows} rows:")
    print(df.head(max_rows).to_string())
    print(f"{'─' * 80}\n")
