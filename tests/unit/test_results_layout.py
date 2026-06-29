import pytest

from ray_dispatcher.results import JobLayout, create_attempt_dir


def _layout(tmp_path):
    return JobLayout(str(tmp_path / "results"), "batch1", "jobA")


def test_layout_paths_match_spec_9_1(tmp_path):
    lo = _layout(tmp_path)
    base = tmp_path / "results" / "batch1" / "jobA"
    assert lo.job_dir == base
    assert lo.attempts_dir == base / "attempts"
    assert lo.outputs_dir == base / "outputs"
    assert lo.result_json == base / "result.json"
    assert lo.attempt_dir(1) == base / "attempts" / "1"
    assert lo.stdout_log(2) == base / "attempts" / "2" / "stdout.log"
    assert lo.stderr_log(2) == base / "attempts" / "2" / "stderr.log"
    assert lo.attempt_json(3) == base / "attempts" / "3" / "attempt.json"


def test_create_attempt_dir_makes_dir(tmp_path):
    lo = _layout(tmp_path)
    d = create_attempt_dir(lo, 1)
    assert d.is_dir()
    assert d == lo.attempt_dir(1)


def test_create_attempt_dir_rejects_reuse(tmp_path):
    lo = _layout(tmp_path)
    create_attempt_dir(lo, 1)
    with pytest.raises(FileExistsError):
        create_attempt_dir(lo, 1)  # attempts are never reused (§9.1)
