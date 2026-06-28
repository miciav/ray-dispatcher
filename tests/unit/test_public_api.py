import ray_dispatcher as rd


def test_public_value_objects_importable():
    for name in [
        "RemoteHost", "Inventory", "SecretFile", "Project",
        "InputSpec", "OutputSpec", "Job",
        "JobStatus", "FailureKind", "RetryPolicy", "JobHandle",
        "AttemptResult", "JobResult", "HostProvisioningResult", "ProvisioningReport",
    ]:
        assert hasattr(rd, name), name


def test_public_errors_importable():
    for name in [
        "DispatcherError", "ConfigurationError", "ModelValidationError",
        "PathValidationError", "RayRuntimeConflictError", "ProvisioningError",
        "HostInUseError", "NoHealthyHostsError", "BatchExistsError",
        "BatchFailedError",
    ]:
        assert hasattr(rd, name), name


def test_all_names_resolve():
    for name in rd.__all__:
        assert hasattr(rd, name), name
