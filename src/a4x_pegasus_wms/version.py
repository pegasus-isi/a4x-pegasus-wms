"""_summary_.

_extended_summary_
"""

from pathlib import Path

THIS_DIR = Path(__file__).parent
__version__ = (THIS_DIR / "version.txt").read_text()
__homepage__ = "https://pegasus.isi.edu"
__source__ = "https://github.com/pegasus-isi/a4x-pegasus-wms.git"
__issues__ = "https://github.com/pegasus-isi/a4x-pegasus-wms/issues"
__changelog__ = (
    "https://github.com/pegasus-isi/a4x-pegasus-wms/blob/master/CHANGELOG.md"
)
__documentation__ = "https://readthedocs.org"
__chat__ = "pegasus-users.slack.com"
