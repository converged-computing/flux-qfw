"""The flux CLI plugin that adds the --qfw-* options.

    flux run -N1 -n4 --qfw-qpu=nwqsim --qfw-max-qubits=5 ./qfw_qiskit_simple.sh 5

It runs in the user's process when they submit, and only shapes the jobspec.

For a device the site owns, the classical request stays as flux built it and
a second entry asks for the exclusive qpu. Fluxion gives the job both at
once, or the job waits while the device is busy, just like a busy core.

For a device behind a vendor queue, one submit becomes two jobs. A scout job
asks for one core and the qpu. The user's job is held and asks for no device.
The scout releases it once the vendor is ready. See scout.py.

The envelope goes into attributes.system.qfw for the shell plugin, and the
shell plugin is added through the userrc shell option, so the site does not
need any shell configuration.
"""

import copy
import os
import sys

import flux
import flux.job
from flux.cli.plugin import CLIPlugin
from flux.job import JobspecV1

from . import instance, resources
from .envelope import WORKLOAD_KINDS, Envelope

USERRC = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "flux_plugins", "flux-qfw.lua"
)
_ACTIVE_PROGS = ("submit", "run", "batch", "bulksubmit", "alloc")


def envelope_from_args(args):
    """Return the envelope for these options, or None when no device was asked for."""
    qpu = getattr(args, "qpu", None)
    if not qpu:
        for name in ("max_qubits", "circ_count", "max_depth", "max_shots"):
            if getattr(args, name, None):
                raise ValueError(
                    "--qfw-{} needs --qfw-qpu".format(name.replace("_", "-"))
                )
        return None
    kwargs = {"qpu": qpu}
    for name in (
        "workload_kind",
        "circ_count",
        "max_qubits",
        "max_depth",
        "max_shots",
        "one_q_gates",
        "two_q_gates",
        "measurements",
        "qpus",
    ):
        value = getattr(args, name, None)
        if value is not None:
            kwargs["circuit_count" if name == "circ_count" else name] = value
    return Envelope(**kwargs)


def device_config(service_id):
    """Return this device's table from the instance's [qfw] config.

    Returns an empty dict outside an instance, or when there is no table.
    """
    try:
        conf = flux.Flux().conf_get("qfw", None) or {}
    except Exception:
        return {}
    spec = dict((conf.get("qpm") or {}).get(service_id) or {})
    if "launch-agent" in conf and "launch-agent" not in spec:
        spec["launch-agent"] = conf["launch-agent"]
    return spec


def instance_graph_or_none():
    """Return the instance graph, or None when it cannot be read.

    That happens with flux run --dry-run outside an instance.
    """
    try:
        return instance.load_instance_graph()
    except Exception:
        return None


def check_device_known(service_id, doc):
    """Refuse the job now when the graph has no such device.

    Fluxion would refuse it later with a bare "type must be known". This
    names the devices the graph does have.
    """
    if doc is None:
        return
    known = resources.devices_in(doc)
    if service_id not in known:
        raise ValueError(
            "no QPM {!r} in the resource graph; known: {}".format(
                service_id, ", ".join(sorted(known)) or "none"
            )
        )


class QfwCLIPlugin(CLIPlugin):
    """Ask for a QFw device along with the classical resources."""

    def __init__(self, prog):
        super().__init__(prog, prefix="qfw")
        if self.prog not in _ACTIVE_PROGS:
            return
        self.add_option(
            "--qpu",
            metavar="SERVICE_ID",
            help="the QFw device to use, for example nwqsim or iqm-ornl-20q",
        )
        self.add_option(
            "--qpus",
            metavar="N",
            type=int,
            default=1,
            help="how many leases to take on the device, default 1",
        )
        self.add_option(
            "--workload-kind",
            metavar="KIND",
            choices=WORKLOAD_KINDS,
            default="hybrid",
            help="quantum or hybrid, default hybrid",
        )
        self.add_option(
            "--circ-count", metavar="N", type=int, help="circuits the job will submit"
        )
        self.add_option(
            "--max-qubits", metavar="N", type=int, help="largest circuit width"
        )
        self.add_option(
            "--max-depth", metavar="N", type=int, help="largest circuit depth"
        )
        self.add_option(
            "--max-shots", metavar="N", type=int, help="most shots per circuit"
        )
        self.add_option(
            "--one-q-gates", metavar="N", type=int, help="most one qubit gates"
        )
        self.add_option(
            "--two-q-gates", metavar="N", type=int, help="most two qubit gates"
        )
        self.add_option(
            "--measurements", metavar="N", type=int, help="most measurements"
        )
        self.add_option(
            "--backend",
            metavar="NAME",
            help="the QPM backend, defw (default) or mock",
        )
        self.add_option(
            "--no-reserve",
            action="store_true",
            help="schedule the device but do not tell the QPM",
        )
        self.add_option(
            "--scope",
            metavar="SCOPE",
            choices=("site", "job"),
            help="site means the site runs the QPM and the job registers with it. "
            "job means the job runs the simulator on its own nodes. "
            "Defaults to the device's [qfw] config",
        )
        self.add_option(
            "--queue",
            metavar="QUEUE",
            choices=("owned", "external"),
            help="owned means the site controls the device, so nodes and device "
            "are given together. external means the device is behind a vendor "
            "queue, so a scout job holds it and this job waits until the vendor "
            "is ready. Defaults to the device's [qfw] config",
        )

    def modify_jobspec(self, args, jobspec):
        try:
            envelope = envelope_from_args(args)
        except ValueError as error:
            raise SystemExit("flux-qfw: {}".format(error))
        if envelope is None:
            return
        raw = jobspec.jobspec
        doc = instance_graph_or_none()
        try:
            check_device_known(envelope.qpu, doc)
            classical = raw["resources"]
            if doc is not None:
                # A graph with sockets needs every level named, or the flat
                # request flux builds will not match it.
                classical = resources.mirror_path(
                    classical, resources.node_to_core_path(doc)
                )
            raw["resources"] = resources.add_device(
                classical, envelope.qpu, envelope.qpus
            )
        except ValueError as error:
            raise SystemExit("flux-qfw: {}".format(error))
        attr = envelope.to_attr()

        # Scope says who runs the QPM. With site scope the site runs it and
        # the job shell registers the job with it. With job scope the job runs
        # the simulator itself on its own nodes, so there is nothing to
        # register and the shell exports the job's hosts instead.
        spec = device_config(envelope.qpu)
        scope = getattr(args, "scope", None) or spec.get("scope") or "site"
        if scope not in ("site", "job"):
            raise SystemExit(
                "flux-qfw: scope must be site or job, not {!r}".format(scope)
            )
        attr["scope"] = scope
        attr["reserve"] = scope == "site" and not getattr(args, "no_reserve", False)

        # Queue says who controls the device. An owned device is ready the
        # moment Fluxion grants it. An external device is behind a vendor
        # queue, so this submit becomes a scout job that holds the device and
        # this job, held, with no device of its own.
        queue = getattr(args, "queue", None) or spec.get("queue") or "owned"
        if queue not in ("owned", "external"):
            raise SystemExit(
                "flux-qfw: queue must be owned or external, not {!r}".format(queue)
            )
        if queue == "external" and scope == "job":
            raise SystemExit(
                "flux-qfw: {} is job-scoped (the job runs it itself) and cannot also sit "
                'behind an external queue; give it scope = "site" or --qfw-scope=site'.format(
                    envelope.qpu
                )
            )
        attr["queue"] = queue
        if spec.get("launch-agent"):
            attr["launch_agent"] = spec["launch-agent"]
        attr["backend"] = (
            getattr(args, "backend", None)
            or os.environ.get("FLUX_QFW_BACKEND")
            or "defw"
        )

        # The shell runs the helper with the same python and PYTHONPATH used
        # here, so it finds the same flux_qfw.
        attr["python"] = sys.executable
        attr["pythonpath"] = os.environ.get("PYTHONPATH", "")
        attr["hold"] = queue == "external" and attr["reserve"]
        if attr["hold"]:
            raw["resources"] = classical
            jobspec.setattr("system.hold", True)
            attr["pair"] = "classical"
            attr["scout"] = (
                None
                if getattr(args, "dry_run", False)
                else submit_scout(jobspec, envelope, attr, classical)
            )
        jobspec.setattr("system.qfw", attr)
        jobspec.setattr_shell_option(
            "userrc", os.environ.get("FLUX_QFW_USERRC", USERRC)
        )


def submit_scout(jobspec, envelope, attr, classical):
    """Submit the scout job for an external device and return its id.

    The scout asks for one core and the exclusive qpu, so it waits in the
    queue like any job and only runs while it holds a lease. It has the same
    envelope and backend as the user's job and the same time limit. Its
    program is flux-qfw scout, which finds the user's job by the qfw.scout
    attribute the plugin writes after this returns.

    The flux CLI has no hook that runs after submit, so the scout is
    submitted from here. If the user's job then fails to submit, the scout
    finds no partner and exits after a minute.
    """
    scout = JobspecV1.from_command(
        [attr["python"], "-m", "flux_qfw.cli", "scout"],
        num_tasks=1,
        num_nodes=1,
        cores_per_task=1,
    )
    scout.setattr("system.job.name", "scout:{}".format(envelope.qpu))
    system = jobspec.jobspec.get("attributes", {}).get("system", {}) or {}
    if system.get("duration"):
        scout.duration = system["duration"]
    if system.get("queue"):
        scout.setattr("system.queue", system["queue"])
    env = dict(jobspec.environment or {})
    if attr.get("pythonpath"):
        env["PYTHONPATH"] = attr["pythonpath"]
    scout.environment = env
    if jobspec.cwd:
        scout.cwd = jobspec.cwd
    scout_attr = dict(attr)
    scout_attr["pair"] = "scout"
    scout_attr["hold"] = False
    scout.setattr("system.qfw", scout_attr)
    scout.jobspec["resources"] = resources.add_device(
        classical_foothold(classical), envelope.qpu, envelope.qpus
    )
    try:
        jobid = flux.job.submit(flux.Flux(), scout)
    except Exception as error:
        raise SystemExit(
            "flux-qfw: could not submit the scout job for {}: {}".format(
                envelope.qpu, error
            )
        )
    print(
        "flux-qfw: scout job {} submitted for {}".format(
            flux.job.JobID(jobid).f58, envelope.qpu
        ),
        file=sys.stderr,
    )
    return int(jobid)


def classical_foothold(classical):
    """Return a one core request with the same levels as the classical one.

    Keeping the same levels means it matches the same graph.
    """
    entry = copy.deepcopy(classical[0])
    entry["count"] = 1
    node = entry
    while node.get("with"):
        child = node["with"][0]
        child["count"] = 1
        node["with"] = [child]
        node = child
    return [entry]
