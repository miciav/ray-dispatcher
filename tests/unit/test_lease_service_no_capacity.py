import asyncio

import pytest

from ray_dispatcher.errors import NoHealthyHostsError
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


def test_acquire_raises_immediately_when_no_capacity():
    async def scenario():
        svc = LeaseService({"a": 1}, now=Clock(), token_factory=_tokens())
        await svc.acquire("x")        # consume the slot
        await svc.quarantine("a")     # now zero healthy hosts
        # wait_for so the pre-fix version (which would block on the condition)
        # fails cleanly with TimeoutError instead of hanging the whole run.
        with pytest.raises(NoHealthyHostsError):
            await asyncio.wait_for(svc.acquire("y"), timeout=1.0)

    asyncio.run(scenario())


def test_blocked_acquire_wakes_and_raises_when_last_host_lost():
    async def scenario():
        svc = LeaseService({"a": 1}, now=Clock(), token_factory=_tokens())
        await svc.acquire("x")                       # full
        waiter = asyncio.create_task(svc.acquire("y"))
        await asyncio.sleep(0.05)                     # waiter blocks (capacity remains)
        assert not waiter.done()
        await svc.quarantine("a")                     # last host lost -> notify
        with pytest.raises(NoHealthyHostsError):
            await asyncio.wait_for(waiter, timeout=1.0)

    asyncio.run(scenario())
