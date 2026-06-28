import json

import pytest

from ray_dispatcher.errors import HostInUseError
from ray_dispatcher.locking import SessionLock
from ray_dispatcher.ssh import CommandResult, FakeTransport


def _responder(create_rc, owner):
    """FakeTransport callback: mkdir ok; create returns create_rc; cat returns owner json."""
    owner_json = json.dumps(owner) if owner is not None else ""

    def results(argv):
        script = argv[2]
        if "set -C" in script:
            return CommandResult(create_rc, "", "", 0.0)
        if script.startswith("cat "):
            rc = 0 if owner_json else 1
            return CommandResult(rc, owner_json, "", 0.0)
        return CommandResult(0, "", "", 0.0)  # mkdir, mv

    return results


def _scripts(t):
    return [c[1][2] for c in t.calls if c[0] == "run"]


def test_live_lock_from_other_session_raises():
    t = FakeTransport(_responder(create_rc=1,
                                 owner={"session_id": "other", "heartbeat": 1000.0}))
    lock = SessionLock(t, "mine", ttl_s=60.0, now=lambda: 1000.0)  # age 0 < ttl
    with pytest.raises(HostInUseError):
        lock.acquire()
    assert not any("mv -f" in s for s in _scripts(t))  # never took it over


def test_stale_lock_is_taken_over():
    t = FakeTransport(_responder(create_rc=1,
                                 owner={"session_id": "other", "heartbeat": 900.0}))
    lock = SessionLock(t, "mine", ttl_s=60.0, now=lambda: 1000.0)  # age 100 > ttl
    lock.acquire()  # no raise
    assert any("mv -f" in s for s in _scripts(t))  # took it over


def test_own_lock_is_refreshed():
    t = FakeTransport(_responder(create_rc=1,
                                 owner={"session_id": "mine", "heartbeat": 1000.0}))
    lock = SessionLock(t, "mine", ttl_s=60.0, now=lambda: 1000.0)
    lock.acquire()  # no raise
    assert any("mv -f" in s for s in _scripts(t))
