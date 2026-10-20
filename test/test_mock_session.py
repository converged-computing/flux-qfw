import os
import sys
import tempfile
import time
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from flux_qfw.envelope import Envelope  # noqa: E402
from flux_qfw.qpm.base import (
    Backend,
    JobIdentity,
    SessionError,
    encode_reservations,
)  # noqa: E402
from flux_qfw.qpm.mock import MockBackend  # noqa: E402


class MockSessionTest(unittest.TestCase):
    def setUp(self):
        os.environ["FLUX_QFW_MOCK_SESSION_DELAY"] = "0.2"
        os.environ.pop("FLUX_QFW_MOCK_SESSION_REJECT", None)

    def tearDown(self):
        os.environ.pop("FLUX_QFW_MOCK_SESSION_DELAY", None)
        os.environ.pop("FLUX_QFW_MOCK_SESSION_REJECT", None)

    def test_session_becomes_the_reservation(self):
        with tempfile.TemporaryDirectory() as root:
            b = MockBackend(root)
            job = JobIdentity(jobid=7, userid=1000, username="user-a")
            s = b.open_session(job, "fake-iqm", Envelope(qpu="fake-iqm"), 60)
            self.assertEqual(s.state, "queued")
            self.assertFalse(s.ready)
            with self.assertRaises(SessionError):
                s.reservation()
            self.assertEqual(
                b.status("fake-iqm"),
                {"service_id": "fake-iqm", "backend": "mock", "active": 0, "queued": 1},
            )
            self.assertEqual(b.session_status("fake-iqm", s.session_id).state, "queued")
            time.sleep(0.3)
            ready = b.session_status("fake-iqm", s.session_id)
            self.assertTrue(ready.ready)
            self.assertEqual(ready.reservation_id, s.session_id)
            self.assertEqual(b.status("fake-iqm")["active"], 1)
            self.assertEqual(b.status("fake-iqm")["queued"], 0)
            r = ready.reservation()
            self.assertEqual(
                encode_reservations([r]), '[["fake-iqm","{}"]]'.format(s.session_id)
            )
            # The scout releases it like any reservation.
            self.assertEqual(
                b.release("fake-iqm", r.reservation_id)["state"], "released"
            )
            self.assertEqual(b.session_status("fake-iqm", s.session_id).state, "failed")
            self.assertEqual(b.status("fake-iqm")["active"], 0)

    def test_close_while_queued(self):
        with tempfile.TemporaryDirectory() as root:
            b = MockBackend(root)
            s = b.open_session(
                JobIdentity(1, 1, "u"), "fake-iqm", Envelope(qpu="fake-iqm"), 1
            )
            self.assertEqual(
                b.close_session("fake-iqm", s.session_id)["state"], "released"
            )
            self.assertEqual(
                b.close_session("fake-iqm", s.session_id)["state"], "released"
            )
            self.assertEqual(b.status("fake-iqm")["queued"], 0)
            self.assertEqual(b.session_status("fake-iqm", "no-such").state, "failed")

    def test_reject(self):
        with tempfile.TemporaryDirectory() as root:
            os.environ["FLUX_QFW_MOCK_SESSION_REJECT"] = "1"
            with self.assertRaises(SessionError):
                MockBackend(root).open_session(
                    JobIdentity(1, 1, "u"), "fake-iqm", Envelope(qpu="fake-iqm"), 1
                )

    def test_base_backend_has_no_queue(self):
        with self.assertRaises(SessionError):
            Backend().open_session(JobIdentity(1, 1, "u"), "x", Envelope(qpu="x"), 1)


if __name__ == "__main__":
    unittest.main()
