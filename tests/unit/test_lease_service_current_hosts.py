import asyncio

from ray_dispatcher.scheduling import LeaseService


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


def test_current_hosts_delegates_to_pool():
    async def scenario():
        svc = LeaseService({"a": 1}, lease_ttl_s=60.0, now=Clock(), token_factory=_tokens())
        assert await svc.current_hosts() == {}
        await svc.acquire("job-1")
        assert await svc.current_hosts() == {"job-1": "a"}

    asyncio.run(scenario())
