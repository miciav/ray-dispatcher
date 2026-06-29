import time

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


def test_actor_lease_handle_heartbeats_while_held():
    ray.init(address="local", namespace="test-heartbeat", num_cpus=2)
    try:
        # Short TTL so an un-heartbeated lease would expire quickly.
        actor = HostLease.remote({"a": 1}, lease_ttl_s=1.0)
        handle = _ActorLeaseHandle(actor, heartbeat_interval_s=0.1)
        lease = handle.acquire("job/hb-test")
        time.sleep(0.5)  # let several heartbeats fire (5 × 0.1s)
        # If heartbeat fired, the lease is still alive in the actor's pool.
        alive = ray.get(actor.heartbeat.remote(lease.token))
        assert alive, "heartbeat must keep the lease alive"
        handle.release(lease.token)
        # After release, the token is gone.
        gone = not ray.get(actor.heartbeat.remote(lease.token))
        assert gone, "token must not exist after release"
    finally:
        ray.shutdown()
