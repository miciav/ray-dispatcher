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


def test_quarantined_hosts_lists_quarantined_sorted():
    pool = LeasePool({"a": 1, "b": 1, "c": 1}, now=Clock(), token_factory=_tokens())
    assert pool.quarantined_hosts() == []
    pool.quarantine("c")
    pool.quarantine("a")
    assert pool.quarantined_hosts() == ["a", "c"]  # sorted


def test_quarantined_hosts_drops_after_reconcile():
    pool = LeasePool({"a": 1}, now=Clock(), token_factory=_tokens())
    pool.quarantine("a")
    assert pool.quarantined_hosts() == ["a"]
    pool.mark_reconciled("a")
    assert pool.quarantined_hosts() == []
