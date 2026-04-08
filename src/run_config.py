"""
RUN_ID Management and Pipeline Mode Control

Giải quyết:
1. RUN_ID rác khi test → DRY_RUN mode
2. Conflict khi 2 người chạy → user_id trong RUN_ID
3. Trace version → explicit RUN_ID trong REUSE_RUN
4. Multi-layer support → layer parameter

Usage:
    # Development - test code mà không upload
    run = RunConfig(mode="DRY_RUN", user_id="thuy", layer="silver")
    
    # Production - tạo version mới
    run = RunConfig(mode="NEW_RUN", user_id="thuy", layer="silver")
    
    # Retry - dùng lại version cũ
    run = RunConfig(
        mode="REUSE_RUN", 
        user_id="thuy", 
        run_id="20260408T120000Z_thuy",
        layer="silver"
    )
"""

from datetime import datetime, timezone
from typing import Literal, Optional
import os

# Type definitions
PipelineMode = Literal["DRY_RUN", "REUSE_RUN", "NEW_RUN"]
LayerType = Literal["bronze", "silver", "gold", "analysis", "bi"]

# Layer configuration mapping
LAYER_CONFIG = {
    "bronze": {
        "bucket": "bronze-data",
        "base_path": "instacart/bronze"
    },
    "silver": {
        "bucket": "silver-data",
        "base_path": "instacart/silver"
    },
    "gold": {
        "bucket": "gold-data",
        "base_path": "instacart/gold"
    },
    "analysis": {
        "bucket": "analysis-data",
        "base_path": "instacart/analysis"
    },
    "bi": {
        "bucket": "bi-export",
        "base_path": "instacart/bi"
    }
}


class RunConfig:
    """
    Pipeline configuration với RUN_ID management.
    
    Attributes:
        mode: Pipeline mode (DRY_RUN | REUSE_RUN | NEW_RUN)
        user_id: User identifier cho teamwork
        layer: Data layer (bronze | silver | gold | analysis | bi)
        run_id: RUN_ID (auto-generated hoặc manual)
        bucket: Supabase bucket name
        base_path: Base path trong bucket
    """
    
    def __init__(
        self,
        mode: PipelineMode,
        user_id: str,
        layer: LayerType,
        run_id: Optional[str] = None,
    ):
        """
        Initialize pipeline configuration.
        
        Args:
            mode: Pipeline mode
                - DRY_RUN: test logic, không upload
                - NEW_RUN: tạo RUN_ID mới
                - REUSE_RUN: dùng lại RUN_ID cũ (cần truyền run_id)
            user_id: User identifier (thuy | partner | shared)
            layer: Data layer (bronze | silver | gold | analysis | bi)
            run_id: RUN_ID cụ thể (bắt buộc nếu mode=REUSE_RUN)
        
        Raises:
            ValueError: Nếu mode=REUSE_RUN mà không có run_id
            ValueError: Nếu layer không hợp lệ
        """
        self.mode = mode
        self.user_id = user_id
        self.layer = layer
        self._run_id = run_id
        self._supabase = None
        
        # Validate
        if mode == "REUSE_RUN" and not run_id:
            raise ValueError(
                "mode='REUSE_RUN' requires run_id parameter. "
                "Example: run_id='20260408T120000Z_thuy'"
            )
        
        if layer not in LAYER_CONFIG:
            raise ValueError(
                f"Invalid layer='{layer}'. Must be one of: "
                f"{list(LAYER_CONFIG.keys())}"
            )
        
        # Get layer config
        layer_cfg = LAYER_CONFIG[layer]
        self.bucket = layer_cfg["bucket"]
        self.base_path = layer_cfg["base_path"]
    
    def get_run_id(self) -> str:
        """
        Lấy RUN_ID theo mode.
        
        Returns:
            RUN_ID string
            - DRY_RUN: "DRY_RUN"
            - NEW_RUN: "20260408T120000Z_thuy"
            - REUSE_RUN: user-provided RUN_ID
        """
        if self.mode == "DRY_RUN":
            return "DRY_RUN"
        
        elif self.mode == "REUSE_RUN":
            return self._run_id
        
        elif self.mode == "NEW_RUN":
            timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            return f"{timestamp}_{self.user_id}"
        
        raise ValueError(f"Invalid mode: {self.mode}")
    
    def get_full_path(self, suffix: str = "") -> str:
        """
        Lấy full path trong bucket.
        
        Args:
            suffix: Optional suffix (e.g., "products", "_manifest")
        
        Returns:
            Full path: "instacart/silver/20260408T120000Z_thuy/products"
        
        Example:
            >>> run.get_full_path("products")
            'instacart/silver/20260408T120000Z_thuy/products'
            >>> run.get_full_path()
            'instacart/silver/20260408T120000Z_thuy'
        """
        path = f"{self.base_path}/{self.get_run_id()}"
        if suffix:
            path = f"{path}/{suffix}"
        return path
    
    def should_upload(self) -> bool:
        """
        Check xem có upload lên Supabase không.
        
        Returns:
            True nếu mode != DRY_RUN
        """
        return self.mode != "DRY_RUN"
    
    def get_latest_user_run_id(self) -> str:
        """
        Lấy RUN_ID mới nhất của user từ Supabase.
        
        Chỉ dùng trong REUSE_RUN mode khi không specify run_id.
        
        Returns:
            Latest RUN_ID của user
        
        Raises:
            RuntimeError: Nếu không tìm thấy RUN_ID nào
        """
        if not self._supabase:
            from supabase_utils import get_supabase_client
            self._supabase = get_supabase_client()
        
        folders = self._supabase.storage.from_(self.bucket).list(self.base_path)
        
        # Filter RUN_IDs của user này
        user_runs = sorted(
            [f["name"] for f in folders 
             if f["name"] and f["name"].endswith(f"_{self.user_id}")],
            reverse=True
        )
        
        if not user_runs:
            raise RuntimeError(
                f"Không tìm thấy RUN_ID nào của user '{self.user_id}' "
                f"tại {self.bucket}/{self.base_path}"
            )
        
        return user_runs[0]
    
    def __repr__(self) -> str:
        return (
            f"RunConfig(mode={self.mode}, user_id={self.user_id}, "
            f"layer={self.layer}, run_id={self.get_run_id()})"
        )


def get_upstream_run_config(
    current_run: RunConfig,
    upstream_layer: LayerType,
    upstream_user_id: Optional[str] = None
) -> RunConfig:
    """
    Tạo RunConfig cho upstream layer (để đọc data).
    
    Logic:
    - Nếu current là DRY_RUN → upstream cũng DRY_RUN
    - Nếu current là NEW_RUN/REUSE_RUN → upstream là REUSE_RUN với latest RUN_ID
    
    Args:
        current_run: RunConfig hiện tại
        upstream_layer: Layer upstream (e.g., "bronze" nếu đang ở "silver")
        upstream_user_id: User_id của upstream (default: same as current)
    
    Returns:
        RunConfig cho upstream layer
    
    Example:
        >>> current = RunConfig(mode="NEW_RUN", user_id="thuy", layer="silver")
        >>> upstream = get_upstream_run_config(current, "bronze")
        >>> # upstream sẽ đọc latest bronze RUN_ID của thuy
    """
    if upstream_user_id is None:
        upstream_user_id = current_run.user_id
    
    if current_run.mode == "DRY_RUN":
        # DRY_RUN mode → upstream cũng DRY_RUN
        return RunConfig(
            mode="DRY_RUN",
            user_id=upstream_user_id,
            layer=upstream_layer
        )
    
    else:
        # NEW_RUN/REUSE_RUN → lấy latest upstream
        temp_run = RunConfig(
            mode="REUSE_RUN",
            user_id=upstream_user_id,
            layer=upstream_layer,
            run_id="temp"  # placeholder
        )
        
        upstream_run_id = temp_run.get_latest_user_run_id()
        
        return RunConfig(
            mode="REUSE_RUN",
            user_id=upstream_user_id,
            layer=upstream_layer,
            run_id=upstream_run_id
        )
