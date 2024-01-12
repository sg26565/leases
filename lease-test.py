"""A simple leader election demonstration using Kubernetes Lease objects."""
import os
import time
from datetime import UTC, datetime, timedelta

from kubernetes import client, config
from kubernetes.client.exceptions import ApiException

LEASE_NAME = 'my-lease'
NAMESPACE = 'default'
HOLDER_IDENTITY = str(os.getpid())

# the number of seconds until the lease expires
LEASE_DURATION = 10
# the number of milliseconds before expiration the lease is renewed
RENEW_LEAD_TIME = 500


def get_lease():
    """Read the lease object or create it if it does not exist."""
    try:
        return api.read_namespaced_lease(name=LEASE_NAME, namespace=NAMESPACE)
    except ApiException as e:
        if e.status != 404:
            raise e

    # lease does not exist - let's create it
    return api.create_namespaced_lease(
        namespace=NAMESPACE,
        body=client.V1Lease(metadata=client.V1ObjectMeta(name=LEASE_NAME))
    )


def update_lease(lease=None):
    """Update the lease renew time. If the holder has changed, also set the acquire time and increment the lease transition count."""
    now = datetime.now(UTC)
    lease = lease or get_lease()

    # update renew time
    lease.spec.renew_time = now + timedelta(seconds=LEASE_DURATION)
    lease.spec.lease_duration_seconds = LEASE_DURATION

    # if the holder has changed, also set the acquire time and increment the lease transition count
    if HOLDER_IDENTITY != lease.spec.holder_identity:
        lease.spec.holder_identity = HOLDER_IDENTITY
        lease.spec.acquire_time = now

        if lease.spec.lease_transitions:
            lease.spec.lease_transitions += 1
        else:
            lease.spec.lease_transitions = 1

    return api.replace_namespaced_lease(
        name=LEASE_NAME,
        namespace=NAMESPACE,
        body=lease
    )


def sleep_until(ts: datetime, delta: int = 0):
    """Sleep until the specified timestamp minus the delta in milliseconds."""
    duration_ms = (ts - datetime.now(UTC))/timedelta(milliseconds=1) - delta
    time.sleep(duration_ms / 1000)


def acquire_lease():
    """Try to acquire the lease if it has expired or wait."""
    while True:
        now = datetime.now(UTC)
        lease = get_lease()

        if lease.spec.renew_time and lease.spec.renew_time > now:
            # lease is still active - wait until it exires
            print(f'follwing leader {lease.spec.holder_identity}')
            sleep_until(lease.spec.renew_time)
        else:
            # lease has expired - try to become new leader
            try:
                return update_lease(lease)
            except ApiException as e:
                if e.status != 409 or e.reason != 'Conflict':
                    raise e


if __name__ == '__main__':
    config.load_config()
    api = client.CoordinationV1Api()

    lease = acquire_lease()

    while True:
        print(f"i'm leader {lease.spec.holder_identity}")
        sleep_until(lease.spec.renew_time, RENEW_LEAD_TIME)
        lease = update_lease()
