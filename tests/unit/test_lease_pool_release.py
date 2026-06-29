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


def test_release_frees_slot():
    pool = LeasePool({"a": 1}, now=Clock(), token_factory=_tokens())
    lease = pool.acquire("x")
    assert pool.free_slots() == 0
    assert pool.release(lease.token) is True
    assert pool.free_slots() == 1
    # slot is reusable
    assert pool.acquire("y") is not None


def test_release_is_idempotent():
    pool = LeasePool({"a": 1}, now=Clock(), token_factory=_tokens())
    lease = pool.acquire("x")
    assert pool.release(lease.token) is True
    assert pool.release(lease.token) is False  # already released -> no-op
    assert pool.free_slots() == 1  # not double-freed


def test_release_unknown_token_is_noop():
    pool = LeasePool({"a": 1}, now=Clock(), token_factory=_tokens())
    assert pool.release("nope") is False
