import pytest

from ray_dispatcher.errors import ModelValidationError
from ray_dispatcher.scheduling import Lease


def test_lease_holds_all_fields():
    lease = Lease(token="t", host="ubuntu@a:22", slot=0, attempt_id="b/j/1",
                  expiry_s=1060.0, heartbeat_s=1000.0)
    assert lease.token == "t"
    assert lease.host == "ubuntu@a:22"
    assert lease.slot == 0
    assert lease.attempt_id == "b/j/1"
    assert lease.expiry_s == 1060.0 and lease.heartbeat_s == 1000.0


def test_lease_is_frozen():
    lease = Lease(token="t", host="a", slot=0, attempt_id="x", expiry_s=1.0, heartbeat_s=0.0)
    with pytest.raises(Exception):
        lease.token = "other"  # type: ignore[misc]


@pytest.mark.parametrize("kw", [
    {"token": ""},
    {"host": ""},
    {"attempt_id": ""},
    {"slot": -1},
])
def test_lease_rejects_invalid(kw):
    base = dict(token="t", host="a", slot=0, attempt_id="x", expiry_s=1.0, heartbeat_s=0.0)
    base.update(kw)
    with pytest.raises(ModelValidationError):
        Lease(**base)
