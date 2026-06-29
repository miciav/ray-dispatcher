import pytest

from ray_dispatcher.results import publish_job_outputs


def test_publish_renames_staging_into_outputs(tmp_path):
    staging = tmp_path / "staging"
    staging.mkdir()
    (staging / "a.json").write_text("payload")
    outputs = tmp_path / "job" / "outputs"   # parent 'job' does not exist yet
    publish_job_outputs(staging, outputs)
    assert outputs.is_dir()
    assert (outputs / "a.json").read_text() == "payload"
    assert not staging.exists()              # moved, not copied


def test_publish_refuses_to_clobber_existing_nonempty_outputs(tmp_path):
    staging = tmp_path / "staging"
    staging.mkdir()
    (staging / "new.json").write_text("new")
    outputs = tmp_path / "outputs"
    outputs.mkdir()
    (outputs / "old.json").write_text("old")  # a prior success already there
    with pytest.raises(OSError):
        publish_job_outputs(staging, outputs)
    assert (outputs / "old.json").read_text() == "old"  # untouched
