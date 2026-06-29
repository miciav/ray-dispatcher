"""The backend-neutral execution interface (spec §3.3).

A backend maps opaque JobHandles to its own execution mechanism; Ray types never
cross this boundary, so a future non-Ray backend can satisfy the same contract.
"""

from __future__ import annotations

from abc import ABC, abstractmethod

from ..models import (
    Inventory,
    Job,
    JobHandle,
    JobResult,
    JobStatus,
    Project,
    ProvisioningReport,
)


class ExecutionBackend(ABC):
    @abstractmethod
    def setup(self, inventory: Inventory, project: Project) -> ProvisioningReport: ...

    @abstractmethod
    def submit(self, batch_id: str, job: Job) -> JobHandle: ...

    @abstractmethod
    def status(self, handle: JobHandle) -> JobStatus: ...

    @abstractmethod
    def cancel(self, handle: JobHandle) -> None: ...

    @abstractmethod
    def resolve(self, handle: JobHandle) -> JobResult: ...

    @abstractmethod
    def teardown(self, *, purge: bool = False) -> None: ...
