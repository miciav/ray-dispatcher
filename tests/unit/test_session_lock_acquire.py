from ray_dispatcher.ssh import CommandResult, FakeTransport
from ray_dispatcher.locking import SessionLock


def _script(call):
    # call is ("run", ("sh", "-c", script)); return the script
    return call[1][2]


def test_acquire_creates_lock_atomically():
    # the noclobber create (set -C) succeeds -> we own the lock, no takeover
    def results(argv):
        return CommandResult(0, "", "", 0.0)  # mkdir + create both succeed

    t = FakeTransport(run_results=results)
    SessionLock(t, "sess-1").acquire()
    scripts = [_script(c) for c in t.calls if c[0] == "run"]
    assert any("mkdir -p" in s for s in scripts)
    assert any("set -C" in s and "session.json" in s for s in scripts)
    # happy path does not read or overwrite an existing owner
    assert not any("mv -f" in s for s in scripts)


def test_payload_contains_session_and_heartbeat():
    t = FakeTransport()
    lock = SessionLock(t, "sess-9", now=lambda: 1234.5)
    import json
    payload = json.loads(lock._payload())
    assert payload == {"session_id": "sess-9", "heartbeat": 1234.5}
