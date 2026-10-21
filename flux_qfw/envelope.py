"""The workload envelope, which describes the size of a job's quantum work.

These are the same values qfw-slurm's SPANK options collected. The envelope
is stored in the jobspec under attributes.system.qfw, and becomes the
task_class of the request sent to the QPM. The field names follow QFw's
reservation format, so the QPM sees the same request it got from Slurm.
"""

import dataclasses

WORKLOAD_KINDS = ("quantum", "hybrid")


@dataclasses.dataclass(frozen=True)
class Envelope:
    qpu: str
    workload_kind: str = "hybrid"
    circuit_count: int = 1
    max_qubits: int = 1
    max_depth: int = 1
    max_shots: int = 1024
    one_q_gates: int = 0
    two_q_gates: int = 0
    measurements: int = 0
    qpus: int = 1

    def __post_init__(self):
        if not self.qpu:
            raise ValueError("an envelope needs a QPU service id")
        if self.workload_kind not in WORKLOAD_KINDS:
            raise ValueError(
                "workload kind must be one of {}".format(", ".join(WORKLOAD_KINDS))
            )
        for name in ("circuit_count", "max_qubits", "max_depth", "max_shots", "qpus"):
            if int(getattr(self, name)) < 1:
                raise ValueError("{} must be at least 1".format(name))
        for name in ("one_q_gates", "two_q_gates", "measurements"):
            if int(getattr(self, name)) < 0:
                raise ValueError("{} must not be negative".format(name))

    def to_attr(self):
        """Return the dict stored under attributes.system.qfw."""
        return dataclasses.asdict(self)

    @classmethod
    def from_attr(cls, attr):
        fields = {f.name for f in dataclasses.fields(cls)}
        return cls(**{k: v for k, v in attr.items() if k in fields})

    def task_class(self):
        """Return the task_class the QPM's admission expects."""
        return {
            "class_id": 1,
            "count": int(self.circuit_count),
            "qubit_count": int(self.max_qubits),
            "depth": int(self.max_depth),
            "shots": int(self.max_shots),
            "one_q_gate_count": int(self.one_q_gates),
            "two_q_gate_count": int(self.two_q_gates),
            "measurement_count": int(self.measurements),
        }
