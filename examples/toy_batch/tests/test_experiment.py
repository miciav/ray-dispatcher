"""Unit tests for experiment.py — run via subprocess since the script has no __main__ guard."""
import json
import math
import subprocess
import sys
from pathlib import Path

EXPERIMENT = Path(__file__).parent.parent / "experiment" / "experiment.py"


def _run(cfg: dict, wd: Path) -> dict:
    wd.mkdir(parents=True, exist_ok=True)
    (wd / "cfg.json").write_text(json.dumps(cfg))
    subprocess.run([sys.executable, str(EXPERIMENT), "cfg.json"], check=True, cwd=wd)
    return json.loads((wd / "result.json").read_text())


def test_result_json_has_expected_fields(tmp_path):
    r = _run({"label": "test", "n_samples": 1_000, "seed": 1}, tmp_path)
    assert r["label"] == "test"
    assert r["n_samples"] == 1_000
    assert r["seed"] == 1
    assert isinstance(r["pi_estimate"], float)


def test_pi_estimate_is_reasonable(tmp_path):
    r = _run({"n_samples": 100_000, "seed": 42}, tmp_path)
    assert abs(r["pi_estimate"] - math.pi) < 0.05


def test_deterministic_with_same_seed(tmp_path):
    cfg = {"n_samples": 10_000, "seed": 7}
    r1 = _run(cfg, tmp_path / "a")
    r2 = _run(cfg, tmp_path / "b")
    assert r1["pi_estimate"] == r2["pi_estimate"]


def test_different_seeds_give_different_estimates(tmp_path):
    r1 = _run({"n_samples": 10_000, "seed": 1}, tmp_path / "a")
    r2 = _run({"n_samples": 10_000, "seed": 2}, tmp_path / "b")
    assert r1["pi_estimate"] != r2["pi_estimate"]


def test_no_args_uses_defaults(tmp_path):
    subprocess.run([sys.executable, str(EXPERIMENT)], check=True, cwd=tmp_path)
    r = json.loads((tmp_path / "result.json").read_text())
    assert r["label"] == "run"
    assert r["n_samples"] == 100_000
    assert r["seed"] == 0
