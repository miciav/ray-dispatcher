import asyncio

from ray_dispatcher.scheduling import LeaseService


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


def test_heartbeat_delegates():
    async def scenario():
        svc = LeaseService({"a": 1}, lease_ttl_s=60.0, now=Clock(), token_factory=_tokens())
        lease = await svc.acquire("x")
        assert await svc.heartbeat(lease.token) is True
        assert await svc.heartbeat("nope") is False

    asyncio.run(scenario())


def test_sweep_returns_expired_hosts_and_wakes_waiters():
    async def scenario():
        clock = Clock(1000.0)
        svc = LeaseService({"a": 1}, lease_ttl_s=60.0, now=clock, token_factory=_tokens())
        await svc.acquire("x")           # deadline 1060
        clock.advance(100.0)             # past deadline
        hosts = await svc.sweep()
        assert hosts == ["a"]            # quarantined, reported for reconcile
        assert await svc.quarantined_hosts() == ["a"]

    asyncio.run(scenario())


def test_mark_reconciled_restores_and_lets_acquire_proceed():
    async def scenario():
        clock = Clock(1000.0)
        svc = LeaseService({"a": 1}, lease_ttl_s=60.0, now=clock, token_factory=_tokens())
        await svc.acquire("x")
        clock.advance(100.0)
        await svc.sweep()                # 'a' quarantined, lease dropped
        assert await svc.quarantined_hosts() == ["a"]
        await svc.mark_reconciled("a")
        assert await svc.quarantined_hosts() == []
        lease = await asyncio.wait_for(svc.acquire("y"), timeout=1.0)  # usable again
        assert lease.host == "a"

    asyncio.run(scenario())
