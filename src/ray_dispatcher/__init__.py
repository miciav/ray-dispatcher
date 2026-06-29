"""ray_dispatcher: a generic, Ray-scheduled dispatcher for subprocess jobs on VMs."""

from .backends.ssh_ray import SshRayBackend
from .dispatcher import Dispatcher
from .errors import (
    BatchExistsError,
    BatchFailedError,
    ConfigurationError,
    DispatcherError,
    HostInUseError,
    ModelValidationError,
    NoHealthyHostsError,
    PathValidationError,
    ProvisioningError,
    RayRuntimeConflictError,
)
from .models import (
    AttemptResult,
    FailureKind,
    HostProvisioningResult,
    InputSpec,
    Inventory,
    Job,
    JobHandle,
    JobResult,
    JobStatus,
    OutputSpec,
    Project,
    ProvisioningReport,
    RemoteHost,
    RetryPolicy,
    SecretFile,
)

__version__ = "0.1.0"

__all__ = [
    "__version__",
    # value objects
    "RemoteHost",
    "Inventory",
    "SecretFile",
    "Project",
    "InputSpec",
    "OutputSpec",
    "Job",
    "JobStatus",
    "FailureKind",
    "RetryPolicy",
    "JobHandle",
    "AttemptResult",
    "JobResult",
    "HostProvisioningResult",
    "ProvisioningReport",
    # high-level API
    "Dispatcher",
    "SshRayBackend",
    # errors
    "DispatcherError",
    "ConfigurationError",
    "ModelValidationError",
    "PathValidationError",
    "RayRuntimeConflictError",
    "ProvisioningError",
    "HostInUseError",
    "NoHealthyHostsError",
    "BatchExistsError",
    "BatchFailedError",
]
