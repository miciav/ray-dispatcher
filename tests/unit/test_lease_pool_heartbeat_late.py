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


def test_heartbeat_rejects_past_deadline_even_before_sweep():
    clock = Clock(1000.0)
    pool = LeasePool({"a": 1}, lease_ttl_s=60.0, now=clock, token_factory=_tokens())
    lease = pool.acquire("x")          # deadline 1060
    clock.advance(70.0)                # now 1070 >= 1060, NOT swept yet
    assert pool.heartbeat(lease.token) is False   # past deadline -> rejected
    # the lease was not extended (stored expiry is still the old deadline)
    assert pool._leases[lease.token].expiry_s == 1060.0


def test_heartbeat_still_extends_a_live_lease():
    clock = Clock(1000.0)
    pool = LeasePool({"a": 1}, lease_ttl_s=60.0, now=clock, token_factory=_tokens())
    lease = pool.acquire("x")          # deadline 1060
    clock.advance(30.0)                # now 1030 < 1060 -> live
    assert pool.heartbeat(lease.token) is True
    assert pool._leases[lease.token].expiry_s == 1090.0
