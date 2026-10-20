"""A fake QPM that keeps its state in files, for tests and demos.

Files live under FLUX_QFW_MOCK_DIR, which defaults to /tmp/flux-qfw-mock.
Each reservation is one JSON file named by its id. Releasing it rewrites the
file with the state set to released, so tests can check what happened. Set
FLUX_QFW_MOCK_REJECT=1 to make every reserve fail.

The mock also fakes a vendor queue. A session starts as a file in the queued
state with a ready time FLUX_QFW_MOCK_SESSION_DELAY seconds ahead, five by
default. The first status check after that turns it into a normal
reservation with the same id, so release and status treat it like any other.
Set FLUX_QFW_MOCK_SESSION_REJECT=1 to make every session fail to open.
"""

import json
import os
import time
import uuid

from .base import Backend, Reservation, ReservationError, Session, SessionError

SESSION_DELAY_DEFAULT = 5.0


class MockBackend(Backend):
    name = "mock"

    def __init__(self, root=None):
        self.root = root or os.environ.get("FLUX_QFW_MOCK_DIR") or "/tmp/flux-qfw-mock"
        os.makedirs(self.root, exist_ok=True)

    def _path(self, rid):
        return os.path.join(self.root, "{}.json".format(rid))

    def _new_id(self):
        # QFw wants a decimal id. A random 62 bit number stays unique when
        # several jobs reserve at the same moment, and we never reuse a file.
        while True:
            rid = str((uuid.uuid4().int >> 66) or 1)
            if not os.path.exists(self._path(rid)):
                return rid

    def _record(self, rid, job, service_id, envelope, duration_s, state):
        return {
            "service_id": service_id,
            "reservation_id": rid,
            "jobid": job.jobid,
            "userid": job.userid,
            "username": job.username,
            "allocation_id": job.allocation_id,
            "task_class": envelope.task_class(),
            "workload_kind": envelope.workload_kind,
            "walltime_s": duration_s,
            "state": state,
        }

    def _write(self, record):
        with open(self._path(record["reservation_id"]), "w") as fh:
            json.dump(record, fh)

    def _read(self, rid):
        path = self._path(rid)
        if not os.path.exists(path):
            return None
        with open(path) as fh:
            return json.load(fh)

    def reserve(self, job, service_id, envelope, duration_s):
        if os.environ.get("FLUX_QFW_MOCK_REJECT"):
            raise ReservationError(
                "mock QPM {} rejected the request".format(service_id)
            )
        rid = self._new_id()
        self._write(
            self._record(rid, job, service_id, envelope, duration_s, "reserved")
        )
        return Reservation(service_id, rid, {"mock_path": self._path(rid)})

    # The fake vendor queue

    def open_session(self, job, service_id, envelope, duration_s):
        if os.environ.get("FLUX_QFW_MOCK_SESSION_REJECT"):
            raise SessionError(
                "mock queue for {} refused the session".format(service_id)
            )
        delay = float(
            os.environ.get("FLUX_QFW_MOCK_SESSION_DELAY") or SESSION_DELAY_DEFAULT
        )
        sid = self._new_id()
        record = self._record(sid, job, service_id, envelope, duration_s, "queued")
        record["ready_at"] = time.time() + delay
        self._write(record)
        return Session(service_id, sid, "queued", detail={"mock_path": self._path(sid)})

    def session_status(self, service_id, session_id):
        record = self._read(session_id)
        if record is None or record.get("state") == "released":
            return Session(
                service_id,
                session_id,
                "failed",
                detail={"reason": "released" if record else "unknown session"},
            )
        if record["state"] == "queued" and time.time() >= record.get("ready_at", 0):
            # The vendor got to us, so the session becomes a reservation.
            record["state"] = "reserved"
            record["session_ready_at"] = time.time()
            self._write(record)
        if record["state"] == "queued":
            return Session(
                service_id,
                session_id,
                "queued",
                detail={"ready_in_s": round(record["ready_at"] - time.time(), 1)},
            )
        return Session(
            service_id,
            session_id,
            "ready",
            reservation_id=session_id,
            detail={"mock_path": self._path(session_id)},
        )

    def close_session(self, service_id, session_id, reason=1):
        return self.release(service_id, session_id, reason)

    def release(self, service_id, reservation_id, reason=1):
        path = self._path(reservation_id)
        if not os.path.exists(path):
            return {"state": "unknown"}
        with open(path) as fh:
            record = json.load(fh)
        record["state"] = "released"
        record["release_reason"] = reason
        with open(path, "w") as fh:
            json.dump(record, fh)
        return {"state": "released"}

    def status(self, service_id):
        active = queued = 0
        for name in os.listdir(self.root):
            with open(os.path.join(self.root, name)) as fh:
                rec = json.load(fh)
            if rec.get("service_id") != service_id:
                continue
            if rec.get("state") == "reserved":
                active += 1
            elif rec.get("state") == "queued":
                queued += 1
        return {
            "service_id": service_id,
            "backend": "mock",
            "active": active,
            "queued": queued,
        }
