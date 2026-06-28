import json

from ray_dispatcher.locking import SessionLock
from ray_dispatcher.ssh import CommandResult, FakeTransport


def _owner_responder(owner):
    owner_json = json.dumps(owner)

    def results(argv):
        if argv[2].startswith("cat "):
            return CommandResult(0, owner_json, "", 0.0)
        return CommandResult(0, "", "", 0.0)

    return results


def _scripts(t):
    return [c[1][2] for c in t.calls if c[0] == "run"]


def test_heartbeat_refreshes_when_owned():
    t = FakeTransport(_owner_responder({"session_id": "mine", "heartbeat": 1.0}))
    SessionLock(t, "mine").heartbeat()
    assert any("mv -f" in s for s in _scripts(t))


def test_heartbeat_noop_when_not_owned():
    t = FakeTransport(_owner_responder({"session_id": "other", "heartbeat": 1.0}))
    SessionLock(t, "mine").heartbeat()
    assert not any("mv -f" in s for s in _scripts(t))


def test_release_removes_when_owned():
    t = FakeTransport(_owner_responder({"session_id": "mine", "heartbeat": 1.0}))
    SessionLock(t, "mine").release()
    assert any("rm -f" in s for s in _scripts(t))


def test_release_noop_when_not_owned():
    t = FakeTransport(_owner_responder({"session_id": "other", "heartbeat": 1.0}))
    SessionLock(t, "mine").release()
    assert not any("rm -f" in s for s in _scripts(t))
