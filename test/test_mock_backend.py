import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from flux_qfw.envelope import Envelope  # noqa: E402
from flux_qfw.qpm.base import JobIdentity, ReservationError  # noqa: E402
from flux_qfw.qpm.mock import MockBackend  # noqa: E402


class MockBackendTest(unittest.TestCase):
    def test_reserve_release_lifecycle(self):
        with tempfile.TemporaryDirectory() as root:
            b = MockBackend(root)
            job = JobIdentity(jobid=1234, userid=1000, username="user-a")
            r = b.reserve(job, "nwqsim", Envelope(qpu="nwqsim"), 60)
            self.assertEqual(r.service_id, "nwqsim")
            self.assertEqual(b.status("nwqsim")["active"], 1)
            self.assertEqual(b.release("nwqsim", r.reservation_id)["state"], "released")
            self.assertEqual(b.status("nwqsim")["active"], 0)
            self.assertEqual(b.release("nwqsim", "999")["state"], "unknown")

    def test_reject(self):
        with tempfile.TemporaryDirectory() as root:
            os.environ["FLUX_QFW_MOCK_REJECT"] = "1"
            try:
                with self.assertRaises(ReservationError):
                    MockBackend(root).reserve(
                        JobIdentity(1, 1, "u"), "nwqsim", Envelope(qpu="nwqsim"), 1
                    )
            finally:
                del os.environ["FLUX_QFW_MOCK_REJECT"]


if __name__ == "__main__":
    unittest.main()
