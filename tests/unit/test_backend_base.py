import inspect

import pytest

from ray_dispatcher.backends.base import ExecutionBackend


def test_cannot_instantiate_abstract_backend():
    with pytest.raises(TypeError):
        ExecutionBackend()  # abstract — has unimplemented abstract methods


def test_declares_the_spec_3_3_methods():
    expected = {"setup", "submit", "status", "cancel", "resolve", "teardown"}
    assert expected <= set(ExecutionBackend.__abstractmethods__)


def test_setup_signature_takes_inventory_and_project():
    sig = inspect.signature(ExecutionBackend.setup)
    assert list(sig.parameters)[1:] == ["inventory", "project"]
