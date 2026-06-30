# ray-dispatcher — Live Host Visibility (addendum)

- **Created:** 2026-06-30
- **Status:** In review
- **Author:** Michele Ciavotta (with Claude Code)
- **Extends:** [2026-06-27-ray-dispatcher-design.md](2026-06-27-ray-dispatcher-design.md)

## Motivation

A consumer building a live progress view (a TUI showing per-VM occupancy)
needs to know which host a currently-`RUNNING` job is executing on.
`Dispatcher.status(handle)` (spec §3.3) returns only a `JobStatus` enum — no
host. The host is only known via `JobResult.host`, available after a job
reaches a terminal state (through `resolve`/`as_completed`/`run`).

The host *is* known earlier than that: `LeasePool` (§7.1) already holds it in
memory for every in-flight attempt, keyed by `attempt_id`, which `run_job`
(§8.3) sets to `job.id`. This addendum exposes that existing state through
the public API instead of adding any new tracking.

## Design

Add one read method at each layer already involved in lease tracking,
returning a snapshot `{job_id: host}` for every job currently holding a
lease (i.e. an attempt mid-flight on some VM):

1. **`LeasePool.current_hosts() -> dict[str, str]`** (§7.1, pure, sync)
   ```python
   def current_hosts(self) -> dict[str, str]:
       return {ls.attempt_id: ls.host for ls in self._leases.values()}
   ```

2. **`LeaseService.current_hosts() -> dict[str, str]`** (§7.1, async wrapper)
   ```python
   async def current_hosts(self) -> dict[str, str]:
       async with self._cond:
           return self._pool.current_hosts()
   ```
   Read-only, so it does not need to `notify_all()` like the mutating
   methods on this class.

3. **`ExecutionBackend.running_hosts() -> dict[str, str]`** (§3.3, new
   abstract method) — implemented by `SshRayBackend`:
   ```python
   def running_hosts(self) -> dict[str, str]:
       if self._actor is None:
           return {}
       return ray.get(self._actor.current_hosts.remote())
   ```
   Returns `{}` before `setup()` has run (no actor yet), matching how other
   backend methods behave pre-setup.

4. **`Dispatcher.running_hosts() -> dict[str, str]`** (§4.5, new public
   method):
   ```python
   def running_hosts(self) -> dict[str, str]:
       return self._backend.running_hosts()
   ```

One call per polling tick returns the host for every running job at once —
no per-handle round trip, no new actor calls beyond the existing `HostLease`
actor.

## Why a snapshot dict, not per-handle

A consumer's progress view polls all outstanding handles once per tick. A
`host_of(handle) -> str | None` method would cost one actor round-trip per
handle per tick; `running_hosts()` costs one round-trip per tick regardless
of job count, and the consumer does the `dict.get(job_id)` locally.

## Compatibility

- `ExecutionBackend` gains a new abstract method, so any external backend
  implementation (none exist beyond `SshRayBackend` today) must implement
  `running_hosts()` too — acceptable since the library has a single
  consumer (`DFaaSOptimizer`/`remote_experiments`) and a single backend.
- `tests/unit/test_backend_base.py::test_declares_the_spec_3_3_methods`
  asserts the exact set of abstract method names; it gains
  `"running_hosts"`.
- A job's entry disappears from `running_hosts()` the instant its lease is
  released (on success, failure, or cancellation) — slightly before
  `Dispatcher.status()` may report the terminal state on the next poll.
  Consumers should treat a missing entry as "no live host info right now",
  not as an error.

## Out of scope

- No host info for `PENDING` jobs (no lease yet — there is nothing to
  report).
- No historical/queue-position data — this is a live snapshot only.
