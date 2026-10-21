"""Backends that talk to a QPM.

defw talks to a real QFw QPM through DEFw, the same way the qfw-slurm gateway
did. mock keeps reservations and sessions in small files, so the plugins can
be tested without QFw. The backend comes from --qfw-backend, then
FLUX_QFW_BACKEND, and defaults to defw.
"""

import os

from .base import (  # noqa: F401
    Backend,
    JobIdentity,
    Reservation,
    ReservationError,
    Session,
    SessionError,
)
from .defw import DefwBackend
from .mock import MockBackend

BACKENDS = {"defw": DefwBackend, "mock": MockBackend}


def backend_name(explicit=None):
    return explicit or os.environ.get("FLUX_QFW_BACKEND") or "defw"


def get_backend(name=None):
    name = backend_name(name)
    if name not in BACKENDS:
        raise ValueError("unknown QPM backend {!r} (mock or defw)".format(name))
    return BACKENDS[name]()
