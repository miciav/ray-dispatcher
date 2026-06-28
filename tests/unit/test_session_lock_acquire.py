import json

from ray_dispatcher.locking import SessionLock
from ray_dispatcher.ssh import CommandResult, FakeTransport


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
    payload = json.loads(lock._payload())
    assert payload == {"session_id": "sess-9", "heartbeat": 1234.5}


def test_acquire_takes_over_lock_with_corrupt_heartbeat():
    # a foreign lock whose heartbeat is non-numeric is treated as stale, not a crash
    def results(argv):
        script = argv[2]
        if "set -C" in script:
            return CommandResult(1, "", "", 0.0)  # lock exists
        if script.startswith("cat "):
            return CommandResult(
                0, '{"session_id": "other", "heartbeat": "garbage"}', "", 0.0
            )
        return CommandResult(0, "", "", 0.0)  # mkdir, mv

    t = FakeTransport(run_results=results)
    SessionLock(t, "mine", ttl_s=60.0, now=lambda: 1000.0).acquire()  # must not raise
    assert any("mv -f" in c[1][2] for c in t.calls if c[0] == "run")
