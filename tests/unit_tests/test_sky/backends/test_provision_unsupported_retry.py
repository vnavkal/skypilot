"""ProvisionUnsupportedError vs. retry_until_up.

ProvisionUnsupportedError's docstring says "a caller that retries until
resources appear stops instead of waiting for a condition that will never
change". These tests check whether _locked_provision honors that.
"""
import contextlib
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from sky import exceptions
from sky.backends import cloud_vm_ray_backend


def _unsupported_only_error() -> exceptions.ResourcesUnavailableError:
    """What provision_with_retries raises when the only candidate is
    unsupported: drive it for a fresh launch whose _retry_zones raises
    ProvisionUnsupportedError and whose re-optimization finds nothing left."""
    provisioner = object.__new__(cloud_vm_ray_backend.RetryingVmProvisioner)
    provisioner._dag = MagicMock()  # pylint: disable=protected-access
    provisioner._optimize_target = MagicMock()  # pylint: disable=protected-access
    provisioner._requested_features = set()  # pylint: disable=protected-access
    provisioner._blocked_resources = set()  # pylint: disable=protected-access
    provisioner._is_launched_by_jobs_controller = False  # pylint: disable=protected-access
    provisioner.log_dir = '/tmp'
    provisioner._retry_zones = MagicMock(  # pylint: disable=protected-access
        side_effect=exceptions.ProvisionUnsupportedError('bad pod_config'))
    to_provision = MagicMock()
    setattr(to_provision, 'assert_launchable', lambda: to_provision)
    config = cloud_vm_ray_backend.RetryingVmProvisioner.ToProvisionConfig(
        'c1',
        to_provision,
        1,
        None,  # Fresh launch: no previous cluster.
        None,
        prev_cluster_ever_up=False,
        prev_config_hash=None)
    task = MagicMock()
    task.is_controller_task.return_value = False
    # The failure summary only formats the (mocked) resources for display.
    with patch.object(cloud_vm_ray_backend.optimizer.Optimizer,
                      'optimize',
                      side_effect=exceptions.ResourcesUnavailableError(
                          'nothing left')), patch.object(
                              cloud_vm_ray_backend,
                              '_format_provision_failure_blocks',
                              return_value=''):
        with pytest.raises(exceptions.ResourcesUnavailableError) as e:
            provisioner.provision_with_retries(
                task,
                config,
                dryrun=False,
                stream_logs=False,
                skip_unnecessary_provisioning=False)
    return e.value


def test_unsupported_only_history():
    error = _unsupported_only_error()
    assert [type(err) for err in error.failover_history
           ] == [exceptions.ProvisionUnsupportedError]


def _locked_provision(monkeypatch, retry_until_up: bool):
    error = _unsupported_only_error()
    backend = cloud_vm_ray_backend.CloudVmRayBackend()
    monkeypatch.setattr(cloud_vm_ray_backend.lock_events,
                        'DistributedLockEvent',
                        lambda *a, **k: contextlib.nullcontext())
    monkeypatch.setattr(cloud_vm_ray_backend.rich_utils, 'force_update_status',
                        lambda *a, **k: None)
    monkeypatch.setattr(
        backend, '_check_existing_cluster', lambda *a, **k: cloud_vm_ray_backend
        .RetryingVmProvisioner.ToProvisionConfig('c1',
                                                 MagicMock(),
                                                 1,
                                                 None,
                                                 None,
                                                 prev_cluster_ever_up=False,
                                                 prev_config_hash=None))
    monkeypatch.setattr(backend, '_maybe_clear_external_cluster_failures',
                        lambda *a, **k: None)
    monkeypatch.setattr(cloud_vm_ray_backend.wheel_utils, 'build_sky_wheel',
                        lambda: ('/tmp/wheel', 'hash'))
    monkeypatch.setattr(cloud_vm_ray_backend.RetryingVmProvisioner,
                        'provision_with_retries', MagicMock(side_effect=error))
    for name in ('add_cluster_event', 'remove_cluster'):
        monkeypatch.setattr(cloud_vm_ray_backend.global_user_state, name,
                            MagicMock())
    usage = cloud_vm_ray_backend.usage_lib.messages.usage
    for name in ('update_cluster_resources', 'update_cluster_status',
                 'update_final_cluster_status'):
        monkeypatch.setattr(usage, name, MagicMock())
    task = MagicMock(num_nodes=1, resources={'kubernetes'})
    return backend._locked_provision(  # pylint: disable=protected-access
        'lock-id',
        task,
        None,
        dryrun=False,
        stream_logs=False,
        cluster_name='c1',
        retry_until_up=retry_until_up)


def test_without_retry_until_up_the_launch_fails(monkeypatch):
    with pytest.raises(exceptions.ResourcesUnavailableError):
        _locked_provision(monkeypatch, retry_until_up=False)


def test_retry_until_up_stops_for_unsupported_only_history(monkeypatch):
    # Per ProvisionUnsupportedError's docstring this should not be retried.
    # If _locked_provision ignores the failover history, this raises
    # ExecutionRetryableError instead and the request is retried forever.
    with pytest.raises(exceptions.ResourcesUnavailableError):
        _locked_provision(monkeypatch, retry_until_up=True)
