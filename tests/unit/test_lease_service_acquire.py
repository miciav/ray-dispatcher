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


def test_acquire_returns_lease_immediately_when_free():
    async def scenario():
        svc = LeaseService({"a": 1}, now=Clock(), token_factory=_tokens())
        lease = await svc.acquire("job/1")
        assert lease is not None and lease.host == "a"

    asyncio.run(scenario())


def test_acquire_blocks_until_release():
    async def scenario():
        svc = LeaseService({"a": 1}, now=Clock(), token_factory=_tokens())
        first = await svc.acquire("job/1")     # consumes the only slot
        waiter = asyncio.create_task(svc.acquire("job/2"))
        await asyncio.sleep(0.05)              # let the waiter block on the condition
        assert not waiter.done()               # no capacity -> still waiting
        await svc.release(first.token)         # frees the slot, notifies
        second = await asyncio.wait_for(waiter, timeout=1.0)
        assert second is not None and second.host == "a"

    asyncio.run(scenario())
