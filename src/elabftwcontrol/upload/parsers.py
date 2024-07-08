from __future__ import annotations

from itertools import chain
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, NamedTuple, Sequence, Union

from elabftwcontrol._logging import logger
from elabftwcontrol.upload.manifests import ElabObjManifests, ManifestIndex
from elabftwcontrol.utils import read_yaml

Pathlike = Union[Path, str]


class Manifests(NamedTuple):
    manifests: Sequence[Mapping[str, Any]]


class ManifestParser:
    """Parses desired state manifests from a predefined file structure"""

    def __init__(
        self,
        path: Pathlike,
    ) -> None:
        self.path = Path(path)

    def parse(self) -> ManifestIndex:
        files = self.get_files(self.path)
        raw_object_definitions = self.parse_definition_files(files)
        manifests = self.interpret_definitions(raw_object_definitions)
        return ManifestIndex.from_manifests(manifests.manifests)

    @classmethod
    def get_files(cls, path: Path) -> list[Path]:
        if path.is_dir():
            return cls.find_yml_files_in_folder(path)
        else:
            return [path]

    @classmethod
    def find_yml_files_in_folder(
        cls,
        definitions_folder: Path,
    ) -> list[Path]:
        definition_files = list(
            map(
                Path,
                chain(
                    definitions_folder.rglob("*.yaml"),
                    definitions_folder.rglob("*.yml"),
                ),
            )
        )
        logger.info(f"Found definition files: {definition_files}")
        return definition_files

    @classmethod
    def parse_definition_files(
        cls,
        definition_files: Iterable[Pathlike],
    ) -> Manifests:
        object_definitions = []
        for filepath in definition_files:
            parsed_manifests = cls.parse_definition_file(filepath)
            if not parsed_manifests:
                continue
            for parsed_manifest in parsed_manifests:
                logger.debug(parsed_manifest)
                object_definitions.append(parsed_manifest)

        return Manifests(manifests=object_definitions)

    @classmethod
    def parse_definition_file(
        cls,
        filepath: Pathlike,
    ) -> List[Dict[str, Any]]:
        logger.info(f"Parsing definition file: {filepath}")
        data = read_yaml(filepath)

        if isinstance(data, dict):
            data = [data]

        return data

    @classmethod
    def interpret_definitions(
        cls,
        raw_definitions: Manifests,
    ) -> ElabObjManifests:
        return ElabObjManifests(**raw_definitions._asdict())
