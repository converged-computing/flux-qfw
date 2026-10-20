"""Reserve a real QFw QPM through DEFw.

This is the adapter from the qfw-slurm gateway, without the gateway. The job
identity comes from Flux instead of a Slurm verifier. It needs QFw activated
with qfw-activate, which puts the defw, api_qpm_common and defw_app_util
modules on the path and sets QFW_SITE_CONFIG.
"""

import os
import threading

from .base import Backend, Reservation, ReservationError

# These modules only exist when QFw is activated. Importing this file without
# them is fine. Using the backend without them raises a clear error.
try:
    import defw
    from api_qpm_common import QPMLifecycleBinding
    from defw_app_util import defw_get_directory_service
except ImportError as error:
    defw = None
    QPMLifecycleBinding = None
    defw_get_directory_service = None
    _QFW_IMPORT_ERROR = error
else:
    _QFW_IMPORT_ERROR = None


class DefwBackend(Backend):
    name = "defw"

    def __init__(self, site_config=None, timeout_seconds=10.0):
        self.site_config = site_config or os.environ.get("QFW_SITE_CONFIG")
        self.timeout_seconds = timeout_seconds
        self._defw = None
        self._binding_factory = None
        self._bindings = {}
        self._lock = threading.Lock()

    def _start(self):
        if self._binding_factory is not None:
            return
        if not self.site_config or not os.path.isfile(self.site_config):
            raise ReservationError(
                "QFW_SITE_CONFIG is unset or missing; source qfw-activate first"
            )
        if _QFW_IMPORT_ERROR is not None:
            raise ReservationError(
                "the QFw environment is not active: {}".format(_QFW_IMPORT_ERROR)
            )
        os.environ["QFW_SITE_CONFIG"] = self.site_config
        try:
            defw_get_directory_service(timeout=self.timeout_seconds)
        except Exception as error:
            raise ReservationError(
                "cannot connect to the DEFw directory service: {}".format(error)
            ) from error
        self._defw = defw
        self._binding_factory = QPMLifecycleBinding

    def _binding(self, service_id):
        self._start()
        with self._lock:
            binding = self._bindings.get(service_id)
            if binding is None:
                binding = self._binding_factory(
                    service_id,
                    directory_getter=lambda: getattr(self._defw, "dirsvc", None),
                    defw_module=self._defw,
                    recovery_timeout=self.timeout_seconds,
                )
                binding.start()
                self._bindings[service_id] = binding
        return binding

    def _admission(self, service_id):
        try:
            lifecycle = self._binding(service_id)
            identity = lifecycle.snapshot()
            if not identity.get("available") or not identity.get("runtime_id"):
                raise ReservationError("QPM {!r} is unavailable".format(service_id))
            runtime_id = str(identity["runtime_id"])
            control = lifecycle.api("control", expected_runtime_id=runtime_id)
            ready = control.is_ready()
            if not isinstance(ready, dict) or not ready.get("ready"):
                raise ReservationError("QPM {!r} is not ready".format(service_id))
            return (
                lifecycle.api("admission", expected_runtime_id=runtime_id),
                runtime_id,
            )
        except ReservationError:
            raise
        except Exception as error:
            raise ReservationError(
                "cannot resolve QPM {!r}: {}".format(service_id, error)
            ) from error

    def _payload(self, job, envelope, duration_s):
        walltime_ns = int(duration_s) * 1_000_000_000 if duration_s else 0
        return {
            "request_id": int(job.jobid) & 0xFFFFFFFF,
            "owner": {"user": job.username, "uid": job.userid, "gid": job.gid},
            "scheduler": "flux",
            "launcher": {"scheduler": "flux", "cluster_name": job.cluster},
            "job_id": str(job.jobid),
            "allocation_id": job.allocation_id,
            "scope_id": job.cluster,
            "account": None,
            "qos": None,
            "priority": 0,
            "workload_kind": envelope.workload_kind,
            "walltime_ns": walltime_ns,
            "ttl_ns": walltime_ns,
            "task_class": envelope.task_class(),
        }

    def reserve(self, job, service_id, envelope, duration_s):
        admission, runtime_id = self._admission(service_id)
        try:
            raw = admission.reserve(request=self._payload(job, envelope, duration_s))
        except Exception as error:
            raise ReservationError(
                "QPM {!r} reserve failed: {}".format(service_id, error)
            ) from error
        if not isinstance(raw, dict):
            raise ReservationError("QPM admission returned a non-mapping")
        status = str(raw.get("status", "")).lower()
        if status != "accepted":
            raise ReservationError(
                "QPM {!r} {} the request: {}".format(
                    service_id,
                    status or "did not answer",
                    raw.get("message") or raw.get("reason"),
                )
            )
        rid = raw.get("reservation_id")
        try:
            rid = int(rid)
        except (TypeError, ValueError):
            rid = 0
        if not 0 < rid < 1 << 64:
            raise ReservationError("accepted reservation lacks a valid reservation id")
        return Reservation(service_id, str(rid), {"runtime_id": runtime_id})

    def release(self, service_id, reservation_id, reason=1):
        admission, _ = self._admission(service_id)
        try:
            result = admission.release(
                reservation_id=int(reservation_id), reason=reason
            )
        except Exception as error:
            raise ReservationError(
                "QPM {!r} release failed: {}".format(service_id, error)
            ) from error
        return result if isinstance(result, dict) else {"state": "unknown"}

    def status(self, service_id):
        lifecycle = self._binding(service_id)
        return {"service_id": service_id, "backend": "defw", **lifecycle.snapshot()}
