from ray_dispatcher.scheduling import LeasePool


class Clock:
    def __init__(self, t: float = 1000.0) -> None:
        self.t = t
    def __call__(self) -> float:
        return self.t
    def advance(self, d: float) -> None:
        self.t += d


def _tokens():
    n = 0
    def factory() -> str:
        nonlocal n
        t = f"tok{n}"
        n += 1
        return t
    return factory


def _pool(**kw):
    return LeasePool({"a": 2, "b": 1}, now=Clock(), token_factory=_tokens(), **kw)


def test_initial_capacity():
    pool = _pool()
    assert pool.healthy_host_count() == 2
    assert pool.free_slots() == 3  # 2 + 1


def test_empty_inventory_has_no_capacity():
    pool = LeasePool({}, now=Clock(), token_factory=_tokens())
    assert pool.healthy_host_count() == 0
    assert pool.free_slots() == 0


def test_acquire_mints_lease_and_consumes_slot():
    clock = Clock(1000.0)
    pool = LeasePool({"a": 2, "b": 1}, lease_ttl_s=60.0, now=clock, token_factory=_tokens())
    lease = pool.acquire("batch/job/1")
    assert lease is not None
    assert lease.token == "tok0"
    assert lease.host in ("a", "b")
    assert lease.attempt_id == "batch/job/1"
    assert lease.heartbeat_s == 1000.0 and lease.expiry_s == 1060.0
    assert pool.free_slots() == 2  # one consumed


def test_acquire_prefers_most_free_host():
    pool = LeasePool({"a": 3, "b": 1}, now=Clock(), token_factory=_tokens())
    lease = pool.acquire("x")
    assert lease.host == "a"  # 3 free > 1 free


def test_acquire_returns_none_when_full():
    pool = LeasePool({"a": 1}, now=Clock(), token_factory=_tokens())
    assert pool.acquire("x") is not None
    assert pool.acquire("y") is None  # only slot taken


def test_distinct_slots_on_same_host():
    pool = LeasePool({"a": 2}, now=Clock(), token_factory=_tokens())
    l1 = pool.acquire("x")
    l2 = pool.acquire("y")
    assert {l1.slot, l2.slot} == {0, 1}
