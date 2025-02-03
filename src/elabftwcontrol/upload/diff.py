from typing import Any, Collection, Mapping, NamedTuple

from typing_extensions import Self


class Change(NamedTuple):
    old: Any
    new: Any

    def __str__(self) -> str:
        return f"{self.old} -> {self.new}"


def _dict_to_str(
    data: Mapping[str, Any],
    marker: str,
    indent: int,
) -> str:
    result = ""
    for k, v in data.items():
        result += " " * indent + f"{marker} {k}: {v}\n"
    return result[:-1]


def _sequence_to_str(
    data: Collection[str],
    marker: str,
    indent: int,
) -> str:
    result = ""
    for k in data:
        result += " " * indent + f"{marker} {k}\n"
    return result[:-1]


_RED = "\033[31m"
_GREEN = "\033[32m"
_YELLOW = "\033[33m"
_RESET = "\033[0m"


class FieldsToAdd(NamedTuple):
    data: Mapping[str, Any]

    def __bool__(self) -> bool:
        if self.data:
            return True
        else:
            return False

    def to_str(self, indent: int = 0) -> str:
        return _dict_to_str(data=self.data, marker=f"{_GREEN}+{_RESET}", indent=indent)


class FieldsToChange(NamedTuple):
    data: Mapping[str, Change]

    def __bool__(self) -> bool:
        if self.data:
            return True
        else:
            return False

    def to_str(self, indent: int = 0) -> str:
        return _dict_to_str(data=self.data, marker=f"{_YELLOW}~{_RESET}", indent=indent)


class FieldsToDelete(NamedTuple):
    data: Collection[str]

    def __bool__(self) -> bool:
        if self.data:
            return True
        else:
            return False

    def to_str(self, indent: int = 0) -> str:
        return _sequence_to_str(
            data=self.data, marker=f"{_RED}-{_RESET}", indent=indent
        )


class DictComparisonResult(NamedTuple):
    to_add: FieldsToAdd
    to_change: FieldsToChange
    to_delete: FieldsToDelete

    def __bool__(self) -> bool:
        if self.to_add or self.to_change or self.to_delete:
            return True
        else:
            return False

    def to_str(self, indent: int = 0) -> str:
        results = []
        for collection in (self.to_add, self.to_change, self.to_delete):
            if collection:
                results.append(collection.to_str(indent))
        return "\n".join(results)

    @classmethod
    def from_dicts(
        cls,
        old: dict[str, Any],
        new: dict[str, Any],
    ) -> Self:
        keys_old = set(old.keys())
        keys_new = set(new.keys())

        keys_to_add = keys_new - keys_old
        to_add = {k: new[k] for k in keys_to_add}

        to_delete = keys_old - keys_new

        to_check = keys_new.intersection(keys_old)
        to_change: dict[str, Change] = {}
        for k in to_check:
            if old[k] == new[k]:
                continue
            to_change[k] = Change(old=old[k], new=new[k])

        return cls(
            to_add=FieldsToAdd(to_add),
            to_change=FieldsToChange(to_change),
            to_delete=FieldsToDelete(to_delete),
        )


class MetadataFieldsToAdd(NamedTuple):
    data: Mapping[str, FieldsToAdd]

    def __bool__(self) -> bool:
        if self.data:
            return True
        else:
            return False

    def to_str(self, indent: int = 0) -> str:
        result = ""
        for field_name, field_info in self.data.items():
            result += (
                " " * indent
                + f"""\
{_GREEN}+{_RESET} {field_name}:
{field_info.to_str(indent + 4)}
"""
            )
        return result


class MetadataFieldsToChange(NamedTuple):
    data: Mapping[str, DictComparisonResult]

    def __bool__(self) -> bool:
        if self.data:
            return True
        else:
            return False

    def to_str(self, indent: int = 0) -> str:
        result = ""
        for field_name, field_info in self.data.items():
            result += (
                " " * indent
                + f"""\
{_YELLOW}~{_RESET} {field_name}:
{field_info.to_str(indent + 4)}
"""
            )
        return result


class MetadataFieldsToDelete(NamedTuple):
    data: Collection[str]

    def __bool__(self) -> bool:
        if self.data:
            return True
        else:
            return False

    def to_str(self, indent: int = 0) -> str:
        return _sequence_to_str(
            data=self.data, marker=f"{_RED}-{_RESET}", indent=indent
        )


class MetadataComparisonResult(NamedTuple):
    to_add: MetadataFieldsToAdd
    to_change: MetadataFieldsToChange
    to_delete: MetadataFieldsToDelete

    def __bool__(self) -> bool:
        if self.to_add or self.to_change or self.to_delete:
            return True
        else:
            return False

    def to_str(self, indent: int = 0) -> str:
        result = ""
        for collection in (self.to_add, self.to_change, self.to_delete):
            if collection:
                result += collection.to_str(indent)
        return result

    @classmethod
    def from_dicts(
        cls,
        old: dict[str, dict[str, Any]],
        new: dict[str, dict[str, Any]],
    ) -> Self:
        keys_old = set(old.keys())
        keys_new = set(new.keys())

        keys_to_add = keys_new - keys_old
        to_add = {k: FieldsToAdd(new[k]) for k in keys_to_add}

        to_delete = keys_old - keys_new

        to_check = keys_new.intersection(keys_old)
        to_change: dict[str, DictComparisonResult] = {}
        for k in to_check:
            diff = DictComparisonResult.from_dicts(old[k], new[k])
            if diff:
                to_change[k] = diff

        return cls(
            to_add=MetadataFieldsToAdd(to_add),
            to_change=MetadataFieldsToChange(to_change),
            to_delete=MetadataFieldsToDelete(to_delete),
        )


class Diff(NamedTuple):
    main_fields: DictComparisonResult
    metadata_fields: MetadataComparisonResult

    def __bool__(self) -> bool:
        if self.main_fields or self.metadata_fields:
            return True
        else:
            return False

    def to_str(self, indent: int = 0) -> str:
        result = ""
        shift = " " * indent
        if self.main_fields:
            result += f"""\
{shift}Main fields
{shift}-----------
{self.main_fields.to_str(indent)}

"""
        if self.metadata_fields:
            result += f"""\
{shift}Metadata fields
{shift}---------------
{self.metadata_fields.to_str(indent)}"""
        return result.strip("\n")

    @classmethod
    def new(
        cls,
        old: dict[str, Any],
        new: dict[str, Any],
        old_metadata_fields: dict[str, dict[str, Any]],
        new_metadata_fields: dict[str, dict[str, Any]],
    ) -> Self:
        main_fields = DictComparisonResult.from_dicts(old=old, new=new)
        metadata_fields = MetadataComparisonResult.from_dicts(
            old=old_metadata_fields,
            new=new_metadata_fields,
        )
        return cls(
            main_fields=main_fields,
            metadata_fields=metadata_fields,
        )
