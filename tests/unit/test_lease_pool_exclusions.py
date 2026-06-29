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


def test_excluded_host_is_skipped_when_another_is_free():
    pool = LeasePool({"a": 1, "b": 1}, now=Clock(), token_factory=_tokens())
    lease = pool.acquire("retry", exclude=["a"])
    assert lease.host == "b"  # 'a' excluded, 'b' chosen


def test_waits_rather_than_reuse_while_untried_host_exists():
    # Fill 'b' (the only non-excluded host), leave 'a' free but excluded.
    # 'b' is still untried-but-full, so the retry must wait (None), not reuse 'a'.
    pool = LeasePool({"a": 1, "b": 1}, now=Clock(), token_factory=_tokens())
    b_lease = pool.acquire("on_b", exclude=["a"])   # picks 'b' ('a' excluded)
    assert b_lease.host == "b"
    # now 'b' is full, 'a' is free but excluded and still untried -> wait (None)
    assert pool.acquire("retry", exclude=["a"]) is None


def test_reuses_excluded_host_once_all_healthy_tried():
    # single host, already tried -> retry may reuse it (it is the only healthy host).
    pool = LeasePool({"a": 2}, now=Clock(), token_factory=_tokens())
    first = pool.acquire("first")
    assert first.host == "a"
    reuse = pool.acquire("retry", exclude=["a"])  # every healthy host tried -> reuse 'a'
    assert reuse is not None and reuse.host == "a"
    assert reuse.slot != first.slot
