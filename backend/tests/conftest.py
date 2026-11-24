"""pytest 共享夹具：每个测试用例使用独立的运行期数据目录，避免工作流状态串用例。

隐藏测试若自带 DATA_ROOT 注入夹具，会后执行并覆盖本夹具的目录，二者通过
monkeypatch/还原机制可以共存。
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))


@pytest.fixture(autouse=True)
def _isolated_workflow_data_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """把 base.DATA_ROOT 指向每个用例独立的 tmp 目录，并清空工作流单例状态。"""
    from app.services import base as base_mod
    from app.services import fence_dispatch as fd

    root = tmp_path / "data"
    root.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(base_mod, "DATA_ROOT", root)
    # 确保没有跨用例的内存状态
    fd.reset_state(root)
    yield root
