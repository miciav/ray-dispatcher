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


def test_heartbeat_extends_expiry():
    clock = Clock(1000.0)
    pool = LeasePool({"a": 1}, lease_ttl_s=60.0, now=clock, token_factory=_tokens())
    lease = pool.acquire("x")
    assert lease.expiry_s == 1060.0
    clock.advance(30.0)  # now 1030
    assert pool.heartbeat(lease.token) is True
    # the stored lease now carries a fresh deadline (now + ttl)
    assert pool._leases[lease.token].heartbeat_s == 1030.0
    assert pool._leases[lease.token].expiry_s == 1090.0


def test_heartbeat_unknown_token_false():
    pool = LeasePool({"a": 1}, now=Clock(), token_factory=_tokens())
    assert pool.heartbeat("nope") is False
