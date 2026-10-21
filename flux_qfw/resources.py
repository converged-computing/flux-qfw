"""The shape of a quantum device in the Fluxion graph.

A QPM is a qdevice_<service_id> vertex with one or more qpu children. It sits
under the cluster root next to the racks, because any node can reach it over
the network and no node owns it. A hybrid job keeps its normal classical
request and adds the device as a second top level entry.

This module does not import flux, so the unit tests can run without it. The
functions that talk to a running instance live in instance.py.
"""

import re

DEVICE_PREFIX = "qdevice_"
QPU = "qpu"

_SAFE = re.compile(r"[^A-Za-z0-9_-]")


def safe_id(service_id):
    """Return the service id with any character Fluxion cannot use replaced."""
    if not service_id or not isinstance(service_id, str):
        raise ValueError("service id must be a non-empty string")
    return _SAFE.sub("_", service_id)


def device_type(service_id):
    """Return the Fluxion vertex type for a device."""
    return DEVICE_PREFIX + safe_id(service_id)


def service_id_from_type(vtype):
    """Return the service id for a device type, or None for any other type."""
    if isinstance(vtype, str) and vtype.startswith(DEVICE_PREFIX):
        return vtype[len(DEVICE_PREFIX) :]
    return None


def device_request(service_id, count=1):
    """Return the jobspec entry that asks for count qpus on one device.

    The qpu has to be exclusive. Fluxion only records the allocation and puts
    the qpu in the job's R when it is asked for exclusively.
    """
    count = int(count)
    if count < 1:
        raise ValueError("qpu count must be at least 1")
    return {
        "type": device_type(service_id),
        "count": 1,
        "with": [{"type": QPU, "count": count, "exclusive": True}],
    }


def add_device(resources, service_id, count=1):
    """Return the classical request with a device entry added at the end.

    A second entry for the same device is refused. Fluxion counts sibling
    entries of one type together and cannot tell them apart.
    """
    if not isinstance(resources, list) or not resources:
        raise ValueError("resources must be a non-empty list")
    if resources[0].get("type") != "node":
        # Without -N there is no node level to put beside the device, and
        # Fluxion rejects the pair. We say so now instead of at match time.
        raise ValueError(
            "a hybrid job needs an explicit node count; add -N (for example -N1 -n1)"
        )
    wanted = device_type(service_id)
    for entry in resources:
        if entry.get("type") == wanted:
            raise ValueError("resources already request {}".format(wanted))
    return list(resources) + [device_request(service_id, count)]


def device_spec(service_id, count=1, properties=None):
    """Return one device as a subtree with count qpu children."""
    props = {"qfw": "", "service_id": service_id}
    if properties:
        props.update(properties)
    return {
        "type": device_type(service_id),
        "count": 1,
        "properties": props,
        "with": [{"type": QPU, "count": int(count), "properties": {"qfw": ""}}],
    }


def _vertex(vid, vtype, index, path, props):
    """Return a graph vertex in the format Fluxion reads."""
    return {
        "id": str(vid),
        "metadata": {
            "type": vtype,
            "basename": vtype,
            "name": "{}{}".format(vtype, index),
            "id": index,
            "uniq_id": vid,
            "rank": -1,
            "exclusive": False,
            "unit": "",
            "size": 1,
            "properties": props,
            "paths": {"containment": path},
        },
    }


def _edge(source, target):
    return {
        "source": str(source),
        "target": str(target),
        "metadata": {"subsystem": "containment"},
    }


def root_of(graph):
    """Return the one vertex that has no parent."""
    targets = {e["target"] for e in graph["edges"]}
    roots = [n for n in graph["nodes"] if n["id"] not in targets]
    if len(roots) != 1:
        raise ValueError("expected one root vertex, found {}".format(len(roots)))
    return roots[0]


def device_subgraph(root, first_id, specs):
    """Return the vertices and edges that add devices under the root.

    specs is a list of (service_id, count). Each device gets one
    qdevice_<id>0 vertex with qpu0 through qpuN beneath it. Vertex ids start
    at first_id.
    """
    nodes, edges = [], []
    root_path = root["metadata"]["paths"]["containment"]
    vid = first_id
    for service_id, count in specs:
        spec = device_spec(service_id, count)
        did, vid = vid, vid + 1
        dpath = "{}/{}0".format(root_path, spec["type"])
        nodes.append(_vertex(did, spec["type"], 0, dpath, spec["properties"]))
        edges.append(_edge(root["id"], did))
        for i in range(int(count)):
            qid, vid = vid, vid + 1
            nodes.append(
                _vertex(qid, QPU, i, "{}/{}{}".format(dpath, QPU, i), {"qfw": ""})
            )
            edges.append(_edge(did, qid))
    return nodes, edges


def add_devices_to_graph(doc, specs):
    """Add devices to a graph before the instance that loads it starts.

    doc is a bare graph, which is the value of R's scheduling key, or an R
    that already has a scheduling key. A plain R has to be encoded first
    (instance.encode_r does that). specs is a list of (service_id, count).
    Devices already in the graph are skipped. The document is changed in
    place, and the service ids that were added are returned.
    """
    if "graph" not in doc and "scheduling" not in doc:
        raise ValueError("the R has no scheduling graph; encode it first")
    graph = graph_of(doc)
    present = devices_in(doc)
    todo = [(sid, n) for sid, n in specs if sid not in present]
    if todo:
        root = root_of(graph)
        first = max(int(n["id"]) for n in graph["nodes"]) + 1
        nodes, edges = device_subgraph(root, first, todo)
        graph["nodes"].extend(nodes)
        graph["edges"].extend(edges)
    return [sid for sid, _ in todo]


def graph_of(doc):
    """Return the graph inside an R, a scheduling document, or a bare graph."""
    if "graph" in doc:
        return doc["graph"]
    if "scheduling" in doc and "graph" in doc["scheduling"]:
        return doc["scheduling"]["graph"]
    raise ValueError("document has no JGF graph")


def devices_in(doc):
    """Return a map of service id to the number of qpus it has."""
    graph = graph_of(doc)
    by_id = {n["id"]: n["metadata"] for n in graph["nodes"]}
    parent = {e["target"]: e["source"] for e in graph["edges"]}
    devices = {}
    for vid, meta in by_id.items():
        sid = service_id_from_type(meta.get("type"))
        if sid is not None:
            devices.setdefault(sid, 0)
    for vid, meta in by_id.items():
        if meta.get("type") != QPU:
            continue
        pmeta = by_id.get(parent.get(vid), {})
        sid = service_id_from_type(pmeta.get("type"))
        if sid is not None:
            devices[sid] = devices.get(sid, 0) + 1
    return devices


def granted(R):
    """Return the devices in a job's own R as [(service_id, [qpu paths])]."""
    graph = graph_of(R)
    by_id = {n["id"]: n["metadata"] for n in graph["nodes"]}
    parent = {e["target"]: e["source"] for e in graph["edges"]}
    out = {}
    for vid, meta in by_id.items():
        if meta.get("type") != QPU:
            continue
        pmeta = by_id.get(parent.get(vid), {})
        sid = service_id_from_type(pmeta.get("type"))
        if sid is None:
            continue
        out.setdefault(sid, []).append(meta.get("paths", {}).get("containment"))
    return sorted(out.items())


def node_to_core_path(doc):
    """Return the vertex types between node and core in a graph.

    A graph built from a plain R has none. A graph from hwloc or written by
    hand often has a socket level. Returns None when the graph has no path
    from a node to a core.
    """
    graph = graph_of(doc)
    by = {n["id"]: n["metadata"].get("type") for n in graph["nodes"]}
    parent = {e["target"]: e["source"] for e in graph["edges"]}
    core = next((vid for vid, t in by.items() if t == "core"), None)
    if core is None:
        return None
    chain = []
    cur = parent.get(core)
    while cur is not None and by.get(cur) != "node":
        chain.append(by[cur])
        cur = parent.get(cur)
    if by.get(cur) != "node":
        return None
    chain.reverse()
    return chain


def mirror_path(resources, intermediates):
    """Add the graph's levels between node and core to the classical request.

    In Fluxion a "with" means a direct child. So the flat node, slot, core
    request that flux builds cannot match a graph with sockets. Next to a
    device entry, Fluxion would then drop the compute part instead of
    failing. Only the first entry is changed, and only when it is a node
    whose child is a slot.
    """
    if not intermediates:
        return resources
    first = resources[0]
    if first.get("type") != "node" or not first.get("with"):
        return resources
    child = first["with"][0]
    if child.get("type") != "slot":
        return resources
    inner = first["with"]
    for vtype in reversed(intermediates):
        inner = [{"type": vtype, "count": 1, "with": inner}]
    rewritten = dict(first)
    rewritten["with"] = inner
    return [rewritten] + list(resources[1:])
