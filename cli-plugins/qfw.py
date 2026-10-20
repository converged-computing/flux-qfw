"""Lets flux find the flux-qfw CLI plugin.

flux loads every .py file on its CLI plugin path and uses any CLIPlugin class
it finds. This file is the only thing that belongs on FLUX_CLI_PLUGINPATH.
The plugin itself lives in the flux_qfw package.
"""

from flux_qfw.cli_plugin import QfwCLIPlugin  # noqa: F401
