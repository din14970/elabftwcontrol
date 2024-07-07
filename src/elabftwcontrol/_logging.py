import logging
import sys

from elabftwcontrol.defaults import DEFAULT_LOG_LEVEL

logging.basicConfig(stream=sys.stderr)

logger = logging.getLogger("elabftwcontrol")
logger.setLevel(DEFAULT_LOG_LEVEL)


def set_log_level(
    level: str,
) -> None:
    logger.setLevel(level)
