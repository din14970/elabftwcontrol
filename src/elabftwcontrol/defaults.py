import logging
import sys
from pathlib import Path

DEFAULT_CONFIG_FILE = Path.home() / ".elabftwcontrol.json"

logging.basicConfig(stream=sys.stderr)

logger = logging.getLogger("elabftwcontrol")
logger.setLevel(logging.WARN)
