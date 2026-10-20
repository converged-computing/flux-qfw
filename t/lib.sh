# Helpers shared by the t/ integration tests. They run under flux start.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(dirname "$HERE")"
export PYTHONPATH="$ROOT${PYTHONPATH:+:$PYTHONPATH}"
export FLUX_CLI_PLUGINPATH="$ROOT/cli-plugins"
WORK="$(mktemp -d /tmp/flux-qfw-t.XXXXXX)"
trap 'rm -rf "$WORK"' EXIT

pass() { echo "ok - $*"; }
fail() { echo "not ok - $*" >&2; exit 1; }

# Run this test again inside its own flux start instance of SIZE brokers,
# unless it already is in one. We check our own variable and not FLUX_URI.
# A cluster image can set FLUX_URI for every shell, and running load_fluxion
# against a real system instance would replace its config and graph.
own_instance() {
    local size="$1"
    shift
    if [ -z "${FLUX_QFW_TEST_INSTANCE:-}" ]; then
        exec env FLUX_QFW_TEST_INSTANCE=1 flux start -s"${size}" bash "$0" "$@"
    fi
}

# Load Fluxion with the R in $1. monitor-force-up lets fake hosts count as up.
load_fluxion() {
    # Unload whatever scheduler the instance started with. That is
    # sched-simple, or Fluxion itself when flux-sched is installed with
    # sysconfdir=/etc. The -f flag skips modules that are not loaded.
    flux module remove -f sched-fluxion-qmanager
    flux module remove -f sched-fluxion-feasibility
    flux module remove -f sched-fluxion-resource
    flux module remove -f sched-simple
    flux module remove resource
    flux config load <<EOF2
[resource]
noverify = true
norestrict = true
path = "$1"
[sched-fluxion-resource]
match-format = "rv1"
[sched-fluxion-qmanager]
queue-policy = "${QMANAGER_POLICY:-fcfs}"
EOF2
    flux module load resource monitor-force-up
    flux module load sched-fluxion-resource
    flux module load sched-fluxion-qmanager
    flux queue start --all --quiet
}
