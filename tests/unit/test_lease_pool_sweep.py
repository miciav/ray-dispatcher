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


def test_sweep_quarantines_expired_lease_host():
    clock = Clock(1000.0)
    pool = LeasePool({"a": 1, "b": 1}, lease_ttl_s=60.0, now=clock, token_factory=_tokens())
    pool.acquire("x")           # on 'a' or 'b'; both have 1 slot -> 'a' (sorted/most-free tie)
    clock.advance(100.0)        # past expiry 1060
    affected = pool.sweep_expired()
    assert len(affected) == 1
    assert affected[0] in ("a", "b")
    assert affected[0] not in pool._live_hosts()  # quarantined
    assert pool.healthy_host_count() == 1


def test_sweep_keeps_live_leases():
    clock = Clock(1000.0)
    pool = LeasePool({"a": 1}, lease_ttl_s=60.0, now=clock, token_factory=_tokens())
    pool.acquire("x")
    clock.advance(30.0)         # not yet expired (deadline 1060)
    assert pool.sweep_expired() == []
    assert pool.healthy_host_count() == 1


def test_sweep_quarantine_invalidates_sibling_leases():
    clock = Clock(1000.0)
    pool = LeasePool({"a": 2}, lease_ttl_s=60.0, now=clock, token_factory=_tokens())
    l1 = pool.acquire("x")
    clock.advance(30.0)
    l2 = pool.acquire("y")      # heartbeat 1030, deadline 1090
    clock.advance(40.0)         # now 1070: l1 (1060) expired, l2 (1090) not — same host
    affected = pool.sweep_expired()
    assert affected == ["a"]
    # the whole host was quarantined, so BOTH leases are gone
    assert pool.release(l1.token) is False
    assert pool.release(l2.token) is False
    assert pool.healthy_host_count() == 0


def test_heartbeat_keeps_lease_alive_past_old_deadline():
    clock = Clock(1000.0)
    pool = LeasePool({"a": 1}, lease_ttl_s=60.0, now=clock, token_factory=_tokens())
    lease = pool.acquire("x")        # deadline 1060
    clock.advance(30.0)              # now 1030
    assert pool.heartbeat(lease.token) is True   # new deadline 1090
    clock.advance(45.0)              # now 1075: past old 1060, before new 1090
    assert pool.sweep_expired() == []            # heartbeat kept it alive


def test_heartbeat_after_expiry_does_not_resurrect():
    clock = Clock(1000.0)
    pool = LeasePool({"a": 1}, lease_ttl_s=60.0, now=clock, token_factory=_tokens())
    lease = pool.acquire("x")
    clock.advance(100.0)             # past expiry 1060
    pool.sweep_expired()             # lease gone, host quarantined
    assert pool.heartbeat(lease.token) is False
