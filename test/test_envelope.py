import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from flux_qfw.envelope import Envelope  # noqa: E402
from flux_qfw.qpm.base import (
    Reservation,
    decode_reservations,
    encode_reservations,
)  # noqa: E402


class EnvelopeTest(unittest.TestCase):
    def test_round_trip_and_task_class(self):
        e = Envelope(qpu="nwqsim", max_qubits=5, circuit_count=3, max_shots=16)
        attr = e.to_attr()
        attr["reserve"] = True  # extra keys the CLI plugin adds are ignored
        self.assertEqual(Envelope.from_attr(attr), e)
        tc = e.task_class()
        self.assertEqual((tc["count"], tc["qubit_count"], tc["shots"]), (3, 5, 16))

    def test_validation(self):
        with self.assertRaises(ValueError):
            Envelope(qpu="")
        with self.assertRaises(ValueError):
            Envelope(qpu="nwqsim", workload_kind="classical")
        with self.assertRaises(ValueError):
            Envelope(qpu="nwqsim", max_qubits=0)

    def test_reservation_encoding_matches_qfw(self):
        text = encode_reservations([Reservation("nwqsim", "42")])
        self.assertEqual(text, '[["nwqsim","42"]]')
        self.assertEqual(decode_reservations(text)[0].reservation_id, "42")


if __name__ == "__main__":
    unittest.main()
