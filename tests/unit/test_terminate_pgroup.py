from ray_dispatcher.ssh import CommandResult, FakeTransport, terminate_process_group


def _alive_for(n_checks):
    """run_results: 'kill -0' returns alive (rc 0) for the first n checks, then dead (rc 1)."""
    state = {"checks": 0}

    def results(argv):
        if argv[:2] == ["kill", "-0"]:
            state["checks"] += 1
            return CommandResult(0 if state["checks"] <= n_checks else 1, "", "", 0.0)
        return CommandResult(0, "", "", 0.0)

    return results


def test_term_is_enough_when_process_exits():
    t = FakeTransport(run_results=_alive_for(0))  # dead on first probe
    assert terminate_process_group(t, 4321, sleep=lambda s: None) is True
    sent = [c[1] for c in t.calls if c[0] == "run"]
    assert ("kill", "-TERM", "-4321") in sent
    assert ("kill", "-KILL", "-4321") not in sent  # never needed to escalate


def test_escalates_to_kill_after_grace():
    # stays alive through the grace window, then dies after KILL
    t = FakeTransport(run_results=_alive_for(100))
    fake_clock = {"t": 0.0}

    def now():
        return fake_clock["t"]

    def sleep(s):
        fake_clock["t"] += s

    # process is still "alive" on probes during grace; flip to dead after KILL:
    calls = {"kill_sent": False}

    def results(argv):
        if argv == ["kill", "-KILL", "-9"]:
            calls["kill_sent"] = True
            return CommandResult(0, "", "", 0.0)
        if argv[:2] == ["kill", "-0"]:
            return CommandResult(1 if calls["kill_sent"] else 0, "", "", 0.0)
        return CommandResult(0, "", "", 0.0)

    t = FakeTransport(run_results=results)
    assert terminate_process_group(t, 9, grace_s=1.0, poll_s=0.5, now=now, sleep=sleep) is True
    sent = [c[1] for c in t.calls if c[0] == "run"]
    assert ("kill", "-KILL", "-9") in sent
