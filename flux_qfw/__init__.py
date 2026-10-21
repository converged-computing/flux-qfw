"""flux-qfw schedules QFw quantum devices with Fluxion.

Each QFw QPM is a qdevice_<service_id> vertex with qpu children in the
Fluxion resource graph. A hybrid job asks for its normal classical resources
and a device. For a device the site owns, Fluxion gives the job both at once.
For a device behind a vendor queue, a scout job holds the device and the
user's job is held until the vendor is ready.
"""

__version__ = "0.0.1"
