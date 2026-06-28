from ray_dispatcher.ssh import CommandResult, FakeTransport


def test_command_result_ok():
    assert CommandResult(0, "", "", 0.1).ok is True
    assert CommandResult(1, "", "boom", 0.1).ok is False


def test_fake_records_calls_and_returns_default_ok():
    t = FakeTransport()
    r = t.run(["echo", "hi"])
    t.push("/local", "/remote", delete=True, excludes=(".git/",))
    t.pull("/remote", "/local")
    assert r.ok
    assert t.calls[0] == ("run", ("echo", "hi"))
    assert t.calls[1] == ("push", ("/local", "/remote", True, (".git/",)))
    assert t.calls[2] == ("pull", ("/remote", "/local", False, ()))


def test_fake_run_results_callback():
    def results(argv):
        return CommandResult(0 if argv[0] == "true" else 7, "", "", 0.0)

    t = FakeTransport(run_results=results)
    assert t.run(["true"]).returncode == 0
    assert t.run(["false"]).returncode == 7
