import json
from typing import Any

from elabftwcontrol._logging import logger
from elabftwcontrol.core.models import MetadataModel


class MetadataParser:
    """Parses metadata string from an API object"""

    def __call__(self, metadata: str | None) -> MetadataModel:
        return MetadataModel(**self._safe_parse_metadata(metadata))

    @classmethod
    def _safe_parse_metadata(
        cls,
        obj_metadata: str | None,
    ) -> dict[str, Any]:
        if obj_metadata is None:
            return {}

        try:
            data = json.loads(obj_metadata)
        except (TypeError, json.JSONDecodeError) as e:
            logger.warn(f"Metadata obj object could not be parsed as valid JSON:\n{e}")
            return {}

        return data


class TagParser:
    """Parses tags from an API object"""

    def __call__(self, data: str | None) -> list[str]:
        if data is None:
            return []
        try:
            return data.split("|")
        except Exception:
            logger.warn(f"Data `{data}` could not be parsed as tags")
        return []


class TagIdParser:
    """Parses tag ids from an API object"""

    def __call__(self, data: str | None) -> list[int]:
        if data is None:
            return []
        try:
            return [int(tag_id.strip()) for tag_id in data.split(",")]
        except Exception:
            logger.warn(f"Data `{data}` could not be parsed as tag ids")
        return []
