import pytest

from ray_dispatcher import errors as e


def test_hierarchy_roots():
    assert issubclass(e.ConfigurationError, e.DispatcherError)
    assert issubclass(e.ModelValidationError, e.ConfigurationError)
    assert issubclass(e.PathValidationError, e.ConfigurationError)
    for cls in (
        e.RayRuntimeConflictError,
        e.ProvisioningError,
        e.HostInUseError,
        e.NoHealthyHostsError,
        e.BatchExistsError,
        e.BatchFailedError,
    ):
        assert issubclass(cls, e.DispatcherError)


def test_provisioning_error_carries_report():
    report = object()
    err = e.ProvisioningError(report)
    assert err.report is report
    assert isinstance(err, e.DispatcherError)


def test_batch_failed_error_carries_results():
    results = [1, 2, 3]
    err = e.BatchFailedError(results)
    assert err.results == results


def test_model_and_path_errors_are_raisable():
    with pytest.raises(e.ModelValidationError):
        raise e.ModelValidationError("bad value")
    with pytest.raises(e.PathValidationError):
        raise e.PathValidationError("bad path")
