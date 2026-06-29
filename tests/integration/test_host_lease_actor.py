import ray

from ray_dispatcher.backends.ssh_ray import HostLease, _ActorLeaseHandle


def test_actor_lease_acquire_release_roundtrip():
    ray.init(address="local", namespace="test-hostlease", num_cpus=2)
    try:
        actor = HostLease.remote({"a": 2})  # 2 slots on host "a"
        handle = _ActorLeaseHandle(actor)
        l1 = handle.acquire("job/1")
        l2 = handle.acquire("job/2")
        assert {l1.host, l2.host} == {"a"} and l1.token != l2.token
        # both slots taken; free one and re-acquire
        handle.release(l1.token)
        l3 = handle.acquire("job/3")
        assert l3.host == "a"
    finally:
        ray.shutdown()
