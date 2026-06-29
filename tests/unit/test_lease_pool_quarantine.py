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


def test_quarantine_removes_host_from_capacity_and_drops_its_leases():
    pool = LeasePool({"a": 2, "b": 1}, now=Clock(), token_factory=_tokens())
    la = pool.acquire("x")  # lands on 'a' (most free)
    assert la.host == "a"
    pool.quarantine("a")
    assert pool.healthy_host_count() == 1  # only 'b'
    assert pool.acquire("y").host == "b"  # acquire avoids quarantined 'a'
    # 'a' lease is invalid; releasing it is a no-op
    assert pool.release(la.token) is False


def test_acquire_skips_quarantined_host():
    pool = LeasePool({"a": 1}, now=Clock(), token_factory=_tokens())
    pool.quarantine("a")
    assert pool.acquire("x") is None
    assert pool.free_slots() == 0


def test_mark_reconciled_restores_host():
    pool = LeasePool({"a": 1}, now=Clock(), token_factory=_tokens())
    pool.quarantine("a")
    assert pool.acquire("x") is None
    pool.mark_reconciled("a")
    assert pool.healthy_host_count() == 1
    assert pool.acquire("x") is not None  # slots usable again after reconcile
