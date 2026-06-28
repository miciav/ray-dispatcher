import pytest

from ray_dispatcher.errors import ModelValidationError, PathValidationError
from ray_dispatcher.models import InputSpec, Job, OutputSpec


def test_inputspec_normalizes_destination():
    spec = InputSpec(source="/abs/local/file", destination="./config/./a.json")
    assert spec.destination == "config/a.json"


def test_inputspec_rejects_escaping_destination():
    with pytest.raises(PathValidationError):
        InputSpec(source="s", destination="../escape")


def test_outputspec_defaults_required_true():
    spec = OutputSpec(source="solutions/eval_smoke")
    assert spec.required is True
    assert spec.destination is None


def test_job_valid():
    job = Job(
        id="madea-smoke",
        command=("python", "run.py", "--config", "c.json"),
        inputs=(InputSpec("c.json", "c.json"),),
        outputs=(OutputSpec("solutions/eval_smoke"),),
    )
    assert job.cwd == "."


@pytest.mark.parametrize("bad_id", ["", "-leading", "has space", "a" * 129, "x/y"])
def test_job_rejects_bad_id(bad_id):
    with pytest.raises(ModelValidationError):
        Job(id=bad_id, command=("echo", "hi"))


def test_job_rejects_empty_command():
    with pytest.raises(ModelValidationError):
        Job(id="j", command=())


def test_job_rejects_nul_in_command():
    with pytest.raises(ModelValidationError):
        Job(id="j", command=("echo", "a\x00b"))


def test_job_rejects_bad_env_key():
    with pytest.raises(ModelValidationError):
        Job(id="j", command=("echo",), env={"1BAD": "x"})


def test_job_rejects_nonpositive_timeout():
    with pytest.raises(ModelValidationError):
        Job(id="j", command=("echo",), timeout_s=0)


def test_job_rejects_escaping_cwd():
    with pytest.raises(PathValidationError):
        Job(id="j", command=("echo",), cwd="../escape")


# FIX 1: trailing-newline bypass regression
def test_job_rejects_trailing_newline_in_id():
    with pytest.raises(ModelValidationError):
        Job(id="abc\n", command=("echo",))


def test_job_rejects_embedded_newline_in_id():
    with pytest.raises(ModelValidationError):
        Job(id="a\nb", command=("echo",))


def test_job_rejects_trailing_newline_in_env_key():
    with pytest.raises(ModelValidationError):
        Job(id="j", command=("echo",), env={"ABC\n": "x"})


# FIX 3: non-string command element
def test_job_rejects_nonstring_command_element():
    with pytest.raises(ModelValidationError):
        Job(id="j", command=("echo", 5))  # type: ignore[arg-type]


# FIX 4: InputSpec empty source
def test_inputspec_rejects_empty_source():
    with pytest.raises(ModelValidationError):
        InputSpec(source="", destination="x")
