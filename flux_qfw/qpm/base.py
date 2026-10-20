"""What every backend provides.

A backend reserves and releases a QPM for one Flux job. For a device behind a
vendor queue it also opens a session on that queue, reports on it, and closes
it. The scout uses the session calls.
"""

import dataclasses
import json


class ReservationError(RuntimeError):
    """The QPM refused, was too slow, or could not be reached."""


class SessionError(RuntimeError):
    """The vendor queue refused the session, or lost it."""


@dataclasses.dataclass(frozen=True)
class JobIdentity:
    """The job and user a reservation is for."""

    jobid: int
    userid: int
    username: str
    cluster: str = "flux"
    gid: int = 0

    @property
    def allocation_id(self):
        return "{}:{}".format(self.cluster, self.jobid)


@dataclasses.dataclass(frozen=True)
class Reservation:
    service_id: str
    reservation_id: str  # a decimal string, which is what QFw expects
    detail: dict = dataclasses.field(default_factory=dict)


@dataclasses.dataclass(frozen=True)
class Session:
    """A place in a vendor's queue, opened for one job before the job runs.

    The state is queued while the vendor has not got to us, ready once it
    has, and failed if the queue refused or dropped it. A ready session
    carries the reservation the job uses.
    """

    service_id: str
    session_id: str
    state: str
    reservation_id: str = None
    detail: dict = dataclasses.field(default_factory=dict)

    @property
    def ready(self):
        return self.state == "ready"

    def reservation(self):
        if not self.ready:
            raise SessionError("session {} is {}".format(self.session_id, self.state))
        return Reservation(self.service_id, self.reservation_id, dict(self.detail))


def encode_reservations(reservations):
    """Return the QFW_RESERVATIONS value, a JSON list of [service_id, id] pairs.

    This is the format QFw's reservation_set.py reads.
    """
    return json.dumps(
        [[r.service_id, str(r.reservation_id)] for r in reservations],
        separators=(",", ":"),
    )


def decode_reservations(text):
    parsed = json.loads(text)
    return [Reservation(sid, str(rid)) for sid, rid in parsed]


class Backend:
    name = None

    def reserve(self, job, service_id, envelope, duration_s):
        """Return a Reservation, or raise ReservationError."""
        raise NotImplementedError

    def release(self, service_id, reservation_id, reason=1):
        """Release a reservation. Calling it twice must be safe."""
        raise NotImplementedError

    def status(self, service_id):
        """Return a dict that describes the QPM, for flux-qfw status."""
        return {"service_id": service_id}

    def open_session(self, job, service_id, envelope, duration_s):
        """Ask the vendor queue for a session and return it, usually queued."""
        raise SessionError(
            "backend {} cannot open a session on an external queue".format(self.name)
        )

    def session_status(self, service_id, session_id):
        """Return the session as the vendor queue sees it now."""
        raise SessionError(
            "backend {} cannot report a session on an external queue".format(self.name)
        )

    def close_session(self, service_id, session_id, reason=1):
        """Give the session up, ready or not. Calling it twice must be safe."""
        raise SessionError(
            "backend {} cannot close a session on an external queue".format(self.name)
        )
