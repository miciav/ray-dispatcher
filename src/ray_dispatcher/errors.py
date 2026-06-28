"""Public exception hierarchy for ray_dispatcher (spec §9.3)."""

from __future__ import annotations

from typing import Any


class DispatcherError(Exception):
    """Base class for all ray_dispatcher errors.

    Also raised directly for unclassified backend-wide failures.
    """


class ConfigurationError(DispatcherError):
    """Raised synchronously, before submission, for invalid configuration."""


class ModelValidationError(ConfigurationError):
    """Invalid value object (RemoteHost/Inventory/Project/Job/SecretFile/...)."""


class PathValidationError(ConfigurationError):
    """A path is absolute, contains '..', or escapes the run root."""


class RayRuntimeConflictError(DispatcherError):
    """``ray.is_initialized()`` was already true at setup() (spec §3.2)."""


class ProvisioningError(DispatcherError):
    """Setup failed. Carries the ProvisioningReport."""

    def __init__(self, report: Any, message: str | None = None) -> None:
        self.report = report
        super().__init__(message or "provisioning failed")


class HostInUseError(DispatcherError):
    """A remote session lock is held by another live Dispatcher session (§3.2)."""


class NoHealthyHostsError(DispatcherError):
    """No healthy host capacity remains for pending work (§8.2)."""


class BatchExistsError(DispatcherError):
    """The local batch directory already exists (§4.5)."""


class BatchFailedError(DispatcherError):
    """raise_on_failure=True and at least one job failed; carries ordered results."""

    def __init__(self, results: Any, message: str | None = None) -> None:
        self.results = results
        super().__init__(message or "batch completed with failures")
