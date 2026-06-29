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
