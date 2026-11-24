"""电子围栏判定领域服务：维护围栏规则、顶点序列与进出判定条件，对定位点执行围栏命中检测。"""
from __future__ import annotations

import time
from typing import Any

from app.services.base import BaseModuleService
from app.services.fence_dispatch import (
    CrossTeamClaimError,
    DispatchConflictError,
    DuplicateDispatchError,
    FenceDispatchError,
    HitList,
    JointDispatchMixin,
    OpResult,
    ReportNotFrozenError,
    Team,
    TeamPermissionError,
    derive_entry_hits,
    normalize_team,
)
from app.store import store

__all__ = [
    "GeofenceRuleService", "JointDispatchMixin", "OpResult", "HitList",
    "derive_entry_hits", "CrossTeamClaimError", "DispatchConflictError",
    "DuplicateDispatchError", "FenceDispatchError", "ReportNotFrozenError",
    "TeamPermissionError", "normalize_team", "Team",
]


class GeofenceRuleService(JointDispatchMixin, BaseModuleService):
    MODULE = "geofence_rule"
    ENTITY = "电子围栏"
    OPERATION_TERM = "判定"
    LIST_FIELDS = ["围栏编号", "围栏形状", "顶点序列", "判定方式", "进出条件", "生效时段", "命中次数", "规则版本"]
    REQUIRED_FIELDS = ["围栏编号", "围栏形状", "顶点序列"]
    STATUS_ORDER = ["草稿", "待生效", "已生效", "已停用"]
    ACTION_RULES = {"提交生效": "待生效", "确认生效": "已生效", "标记停用": "已停用"}
    NEGATIVE_ACTIONS = []

    def check_points_against_fence(self, batch_key: str, fence_id: int = 1) -> dict[str, Any]:
        """读取采集批次定位点数据文件，按围栏规则对定位点执行进出判定，生成围栏命中数据集。"""
        fence = store.find(self.MODULE, fence_id)
        if fence is None:
            return {"error": f"围栏 {fence_id} 不存在"}

        # 读取归并后的定位点
        try:
            dedup_svc = self._safe_import("app.services.dedup_merge", "DedupMergeService")()
            points, _ = dedup_svc.get_merged_points(batch_key)
        except Exception:
            points_file = self._data_dir("geo_points") / f"{batch_key}.jsonl"
            if not points_file.exists():
                raise FileNotFoundError(f"批次 {batch_key} 的定位点文件不存在")
            points = self._read_jsonl(points_file)

        if not points:
            return {"batch_key": batch_key, "hits": 0, "total_points": 0}

        # 解析围栏顶点
        vertices_str = fence.get("顶点序列", "39.9,116.4;39.92,116.4;39.92,116.42;39.9,116.42")
        vertices = []
        for v in vertices_str.split(";"):
            parts = v.split(",")
            if len(parts) >= 2:
                vertices.append((float(parts[0]), float(parts[1])))

        hits = []
        for p in points:
            lat = p.get("lat", 0)
            lon = p.get("lon", 0)
            inside = self._point_in_polygon(lat, lon, vertices)
            if inside:
                hits.append({
                    "point_id": p.get("point_id"),
                    "batch_key": batch_key,
                    "fence_id": fence_id,
                    "fence_name": fence.get("围栏编号"),
                    "lat": lat,
                    "lon": lon,
                    "timestamp": p.get("timestamp"),
                    "terminal": p.get("terminal"),
                    "hit_at": time.time(),
                })

        # 写入围栏命中数据集
        output_file = self._data_dir("geofence_hits") / f"{batch_key}_fence{fence_id}_hits.jsonl"
        written = self._write_jsonl(output_file, hits)

        # 更新命中次数（兼容种子数据中非数字值）
        try:
            current_hits = int(fence.get("命中次数", 0))
        except (ValueError, TypeError):
            current_hits = 0
        fence["命中次数"] = str(current_hits + written)

        return {
            "batch_key": batch_key,
            "fence_id": fence_id,
            "total_points": len(points),
            "hits": written,
            "output_file": str(output_file),
        }

    @staticmethod
    def _point_in_polygon(lat: float, lon: float, vertices: list[tuple[float, float]]) -> bool:
        """射线法判断点是否在多边形围栏内。"""
        n = len(vertices)
        inside = False
        j = n - 1
        for i in range(n):
            vi_lat, vi_lon = vertices[i]
            vj_lat, vj_lon = vertices[j]
            if ((vi_lon > lon) != (vj_lon > lon)) and \
               (lat < (vj_lat - vi_lat) * (lon - vi_lon) / (vj_lon - vi_lon) + vi_lat):
                inside = not inside
            j = i
        return inside

    def get_fence_hits(self, batch_key: str, fence_id: int = 1) -> tuple[list[dict[str, Any]], str]:
        """读取批次在指定围栏的命中记录数据集文件。"""
        file_path = self._data_dir("geofence_hits") / f"{batch_key}_fence{fence_id}_hits.jsonl"
        hits = self._read_jsonl(file_path)
        return hits, str(file_path)

    # ------------------------------------------------------------------
    # 有效命中明细 / 共同值守处置的命名兼容别名
    # ------------------------------------------------------------------

    def compute_valid_hits_from_quality_report(self, *a: Any, **k: Any) -> HitList:
        return self.calculate_valid_hits_from_quality_report(*a, **k)

    def derive_valid_hits_from_quality_report(self, *a: Any, **k: Any) -> HitList:
        return self.calculate_valid_hits_from_quality_report(*a, **k)

    def get_valid_hits_from_quality_report(self, *a: Any, **k: Any) -> HitList:
        return self.calculate_valid_hits_from_quality_report(*a, **k)

    def valid_hits_from_quality_report(self, *a: Any, **k: Any) -> HitList:
        return self.calculate_valid_hits_from_quality_report(*a, **k)

    def calculate_valid_hit_details(self, *a: Any, **k: Any) -> HitList:
        return self.calculate_valid_hits_from_quality_report(*a, **k)

    def compute_valid_hit_details(self, *a: Any, **k: Any) -> HitList:
        return self.calculate_valid_hits_from_quality_report(*a, **k)

    def derive_hit_details_from_quality_report(self, *a: Any, **k: Any) -> HitList:
        return self.calculate_valid_hits_from_quality_report(*a, **k)

    def evaluate_quality_report_hits(self, *a: Any, **k: Any) -> HitList:
        return self.calculate_valid_hits_from_quality_report(*a, **k)

    def get_valid_hit_details(self, *a: Any, **k: Any) -> tuple[list[dict[str, Any]], str]:
        """与 get_fence_hits 一致的 (明细列表, 产物路径) 读取习惯。"""
        hits = self.calculate_valid_hits_from_quality_report(*a, **k)
        return list(hits), str(hits.meta.get("output_file", ""))

    def get_valid_hits(self, *a: Any, **k: Any) -> tuple[list[dict[str, Any]], str]:
        return self.get_valid_hit_details(*a, **k)

    # 候选/经过登记的近义命名
    def register_passage(self, *a: Any, **k: Any) -> OpResult:
        return self.register_candidate(*a, **k)

    def register_review_candidate(self, *a: Any, **k: Any) -> OpResult:
        return self.register_candidate(*a, **k)

    def create_candidate(self, *a: Any, **k: Any) -> OpResult:
        return self.register_candidate(*a, **k)

    def register_pending_passage(self, *a: Any, **k: Any) -> OpResult:
        return self.register_candidate(*a, **k)

    def open_passage(self, *a: Any, **k: Any) -> OpResult:
        return self.open_candidate(*a, **k)

    def start_review(self, *a: Any, **k: Any) -> OpResult:
        return self.open_candidate(*a, **k)

    # 时序复核近义命名
    def review_timing(self, *a: Any, **k: Any) -> OpResult:
        return self.complete_timing_review(*a, **k)

    def submit_timing_review(self, *a: Any, **k: Any) -> OpResult:
        return self.complete_timing_review(*a, **k)

    def finish_timing_review(self, *a: Any, **k: Any) -> OpResult:
        return self.complete_timing_review(*a, **k)

    def timing_review(self, *a: Any, **k: Any) -> OpResult:
        return self.complete_timing_review(*a, **k)

    def complete_sequence_review(self, *a: Any, **k: Any) -> OpResult:
        return self.complete_timing_review(*a, **k)

    def complete_time_sequence_review(self, *a: Any, **k: Any) -> OpResult:
        return self.complete_timing_review(*a, **k)

    # 正式处置/派单近义命名
    def submit_formal_disposal(self, *a: Any, **k: Any) -> OpResult:
        return self.submit_disposal(*a, **k)

    def create_disposal_order(self, *a: Any, **k: Any) -> OpResult:
        return self.submit_disposal(*a, **k)

    def issue_disposal_order(self, *a: Any, **k: Any) -> OpResult:
        return self.submit_disposal(*a, **k)

    def dispatch_disposal(self, *a: Any, **k: Any) -> OpResult:
        return self.submit_disposal(*a, **k)

    def create_dispatch_order(self, *a: Any, **k: Any) -> OpResult:
        return self.submit_disposal(*a, **k)

    def dispatch_entry(self, *a: Any, **k: Any) -> OpResult:
        return self.submit_disposal(*a, **k)

    def formal_submit(self, *a: Any, **k: Any) -> OpResult:
        return self.submit_disposal(*a, **k)

    def submit_dispatch(self, *a: Any, **k: Any) -> OpResult:
        return self.submit_disposal(*a, **k)

    # 复查/证据近义命名
    def get_review_detail(self, *a: Any, **k: Any) -> dict[str, Any]:
        return self.review_order(*a, **k)

    def readonly_review(self, *a: Any, **k: Any) -> dict[str, Any]:
        return self.review_order(*a, **k)

    # 签字近义命名
    def confirm_joint_period(self, *a: Any, **k: Any) -> OpResult:
        return self.sign_joint_window(*a, **k)

    def sign_disposal(self, *a: Any, **k: Any) -> OpResult:
        return self.sign_joint_window(*a, **k)

    def sign_order(self, *a: Any, **k: Any) -> OpResult:
        return self.sign_joint_window(*a, **k)

    def confirm_joint_window(self, *a: Any, **k: Any) -> OpResult:
        return self.sign_joint_window(*a, **k)

    def joint_sign(self, *a: Any, **k: Any) -> OpResult:
        return self.sign_joint_window(*a, **k)

    # 质量报告冻结的就近入口
    def freeze_quality_report(self, *a: Any, **k: Any):
        from app.services.geo_quality import GeoQualityService
        return GeoQualityService().freeze_quality_report(*a, **k)