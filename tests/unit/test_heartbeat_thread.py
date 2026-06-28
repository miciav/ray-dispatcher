import json
import time

from ray_dispatcher.locking import HeartbeatThread, SessionLock
from ray_dispatcher.ssh import CommandResult, FakeTransport


def test_heartbeat_thread_beats_then_stops():
    owner = json.dumps({"session_id": "mine", "heartbeat": 1.0})

    def results(argv):
        if argv[2].startswith("cat "):
            return CommandResult(0, owner, "", 0.0)
        return CommandResult(0, "", "", 0.0)

    t = FakeTransport(run_results=results)
    hb = HeartbeatThread(SessionLock(t, "mine"), interval_s=0.01)
    hb.start()
    time.sleep(0.05)
    hb.stop()
    refreshes = sum(1 for c in t.calls if c[0] == "run" and "mv -f" in c[1][2])
    assert refreshes >= 1
    # after stop, no further beats
    settled = refreshes
    time.sleep(0.03)
    assert sum(1 for c in t.calls if c[0] == "run" and "mv -f" in c[1][2]) == settled
