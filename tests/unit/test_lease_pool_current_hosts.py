from ray_dispatcher.scheduling import LeasePool


class Clock:
    def __init__(self, t: float = 1000.0) -> None:
        self.t = t
    def __call__(self) -> float:
        return self.t


def _tokens():
    n = 0
    def factory() -> str:
        nonlocal n
        t = f"tok{n}"
        n += 1
        return t
    return factory


def test_current_hosts_empty_when_no_leases():
    pool = LeasePool({"a": 1}, now=Clock(), token_factory=_tokens())
    assert pool.current_hosts() == {}


def test_current_hosts_reports_attempt_id_to_host():
    pool = LeasePool({"a": 1, "b": 1}, now=Clock(), token_factory=_tokens())
    pool.acquire("job-1")
    pool.acquire("job-2")
    assert pool.current_hosts() == {"job-1": "a", "job-2": "b"}


def test_current_hosts_drops_entry_after_release():
    pool = LeasePool({"a": 1}, now=Clock(), token_factory=_tokens())
    lease = pool.acquire("job-1")
    pool.release(lease.token)
    assert pool.current_hosts() == {}


def test_current_hosts_drops_entry_after_quarantine():
    pool = LeasePool({"a": 1}, now=Clock(), token_factory=_tokens())
    pool.acquire("job-1")
    pool.quarantine("a")
    assert pool.current_hosts() == {}
