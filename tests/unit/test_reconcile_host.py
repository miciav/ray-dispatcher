from ray_dispatcher.scheduling import reconcile_host
from ray_dispatcher.ssh import CommandResult, FakeTransport


def _runs(t):
    return [c[1] for c in t.calls if c[0] == "run"]


def test_reconcile_no_pid_file_is_clean():
    def results(argv):
        if argv[0] == "cat":
            return CommandResult(1, "", "no such file", 0.0)
        return CommandResult(0, "", "", 0.0)

    t = FakeTransport(run_results=results)
    assert reconcile_host(t, "/home/u/.ray_dispatcher/runs/b/j/1/pid.json") is True
    assert not any(a[0] == "kill" for a in _runs(t))  # nothing terminated


def test_reconcile_terminates_recorded_pgid():
    def results(argv):
        if argv[0] == "cat":
            return CommandResult(0, '{"pid": 1234, "pgid": 4321}', "", 0.0)
        if argv[:2] == ["kill", "-0"]:
            return CommandResult(1, "", "", 0.0)  # probe: group already gone
        return CommandResult(0, "", "", 0.0)      # TERM/KILL succeed

    t = FakeTransport(run_results=results)
    assert reconcile_host(t, "/x/pid.json") is True
    assert ("kill", "-TERM", "-4321") in _runs(t)  # SIGTERM to the process group


def test_reconcile_corrupt_pid_file_stays_quarantined():
    def results(argv):
        if argv[0] == "cat":
            return CommandResult(0, "garbage{not json", "", 0.0)
        return CommandResult(0, "", "", 0.0)

    t = FakeTransport(run_results=results)
    assert reconcile_host(t, "/x/pid.json") is False  # cannot confirm clean
    assert not any(a[0] == "kill" for a in _runs(t))
