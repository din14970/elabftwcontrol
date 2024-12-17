from __future__ import annotations

import json
from dataclasses import dataclass
from enum import Enum
from typing import (
    Any,
    Callable,
    Final,
    Hashable,
    Iterable,
    Iterator,
    Mapping,
    NamedTuple,
    Optional,
    Sequence,
    TypeVar,
)

import pandas as pd
from typing_extensions import Self

from elabftwcontrol._logging import logger
from elabftwcontrol.core.interfaces import Dictable, HasIDAndMetadata
from elabftwcontrol.core.manifests import ElabObjManifest
from elabftwcontrol.core.metadata import (
    MetadataField,
    ParsedMetadataToMetadataFieldList,
    ParsedMetadataToPandasDtype,
    ParsedMetadataToSimpleDict,
    TableCellContentType,
)
from elabftwcontrol.core.models import MetadataModel
from elabftwcontrol.core.parsers import MetadataParser
from elabftwcontrol.utils import do_nothing, sanitize_name_for_glue

DEFAULT_ITEM_SCHEMA: Final[dict[str, str]] = {
    "id": "Int64",
    "elabid": "string",
    "category": "Int64",
    "category_title": "string",
    "title": "string",
    "body": "string",
    "tags": "string",
    "fullname": "string",
    "created_at": "datetime64[ns]",
    "modified_at": "datetime64[ns]",
    "rating": "Int64",
}

DEFAULT_EXPERIMENT_SCHEMA: Final[dict[str, str]] = {
    "id": "Int64",
    "elabid": "string",
    "title": "string",
    "body": "string",
    "status_title": "string",
    "tags": "string",
    "fullname": "string",
    "created_at": "datetime64[ns]",
    "modified_at": "datetime64[ns]",
    "rating": "Int64",
}

DEFAULT_EXPERIMENTS_TEMPLATE_SCHEMA: Final[dict[str, str]] = {
    "id": "Int64",
    "title": "string",
    "body": "string",
    "tags": "string",
    "fullname": "string",
}

DEFAULT_ITEMS_TYPE_SCHEMA: Final[dict[str, str]] = {
    "id": "Int64",
    "title": "string",
    "body": "string",
}


T = TypeVar("T")


class YAMLTransformer:
    def __call__(
        self,
        objects: Iterable[Dictable],
    ) -> Iterator[str]:
        raise NotImplementedError


class DefinitionTransformer:
    def __call__(
        self,
        objects: Iterable[Dictable],
    ) -> Iterator[ElabObjManifest]:
        raise NotImplementedError


class JSONTransformer:
    def __init__(
        self,
        dict_transformer: Callable[[Iterable[Dictable]], Iterator[dict[str, Any]]],
        string_transformer: Callable[[Iterable[dict[str, Any]]], Iterator[str]],
    ) -> None:
        self._dict_transformer = dict_transformer
        self._string_transformer = string_transformer

    @classmethod
    def new(
        cls,
        indent: Optional[int] = 2,
    ) -> Self:
        return cls(
            dict_transformer=_DictTransformer(),
            string_transformer=_JSONStringTransformer(indent=indent),
        )

    def __call__(
        self,
        objects: Iterable[Dictable],
    ) -> Iterator[str]:
        dictionaries = self._dict_transformer(objects)
        return self._string_transformer(dictionaries)


class _DictTransformer:
    def __call__(self, objects: Iterable[Dictable]) -> Iterator[dict[str, Any]]:
        for obj in objects:
            yield obj.to_dict()


class _JSONStringTransformer:
    def __init__(
        self,
        indent: Optional[int],
    ) -> None:
        self.indent = indent

    def __call__(self, objects: Iterable[dict[str, Any]]) -> Iterator[str]:
        for obj in objects:
            yield json.dumps(obj, indent=self.indent)


class TableShapes(str, Enum):
    WIDE = "wide"
    LONG = "long"


class ObjectTypes(str, Enum):
    ITEM = "item"
    ITEMS_TYPE = "items_type"
    EXPERIMENT = "experiment"
    EXPERIMENTS_TEMPLATE = "experiments_template"


@dataclass
class SplitDataFrame:
    key: str
    data: pd.DataFrame


class MultiPandasDataFrameTransformer:
    """Splits a raw pandas dataframe representation of API objects into multiple
    tables according to a splitting rule.

    Best instantiated via the `for_long_tables` or `for_wide_tables` factory
    methods.

    Parameters
    ----------
    splitter
        A callable that splits the dataframe into multiple tables.
    df_transform_getter
        A callable that returns a callable to transform each split dataframe based on
        the key provided.
    """

    def __init__(
        self,
        splitter: Callable[[pd.DataFrame], Iterable[SplitDataFrame]],
        df_transform_getter: Callable[[str], Callable[[pd.DataFrame], pd.DataFrame]],
    ) -> None:
        self.df_transform_getter = df_transform_getter
        self.splitter = splitter

    def __call__(self, raw_df: pd.DataFrame) -> Iterator[SplitDataFrame]:
        for split_df in self.split(raw_df):
            transformed = self.transform(split_df)
            yield SplitDataFrame(
                key=split_df.key,
                data=transformed,
            )

    def split(self, df: pd.DataFrame) -> Iterable[SplitDataFrame]:
        return self.splitter(df)

    def transform(self, split_df: SplitDataFrame) -> pd.DataFrame:
        return self.df_transform_getter(split_df.key)(split_df.data)

    @classmethod
    def for_raw_tables(
        cls,
        object_type: ObjectTypes,
        categories: Optional[Sequence[str]] = None,
    ) -> Self:
        def df_transform_getter(
            category: Hashable,
        ) -> Callable[[pd.DataFrame], pd.DataFrame]:
            return lambda df: df

        splitter = cls._get_splitter(object_type, categories)

        return cls(
            splitter=splitter,
            df_transform_getter=df_transform_getter,
        )

    @classmethod
    def for_long_tables(
        cls,
        object_type: ObjectTypes,
        categories: Optional[Sequence[str]] = None,
    ) -> Self:
        def df_transform_getter(
            category: Hashable,
        ) -> Callable[[pd.DataFrame], pd.DataFrame]:
            return PandasDataFrameMetadataTransformer.for_long_table(
                object_type=object_type,
            )

        splitter = cls._get_splitter(object_type, categories)

        return cls(
            splitter=splitter,
            df_transform_getter=df_transform_getter,
        )

    @classmethod
    def for_wide_tables(
        cls,
        object_type: ObjectTypes,
        categories_metadata_schema: Optional[
            Mapping[str, Optional[Mapping[str, Any]]]
        ] = None,
        cell_content: TableCellContentType = TableCellContentType.COMBINED,
        sanitize_column_names: bool = False,
    ) -> Self:
        def df_transform_getter(
            category: str,
        ) -> Callable[[pd.DataFrame], pd.DataFrame]:
            if categories_metadata_schema is None:
                metadata_schema = None
            else:
                metadata_schema = categories_metadata_schema.get(category)
            return PandasDataFrameMetadataTransformer.for_wide_table(
                object_type=object_type,
                cell_content=cell_content,
                metadata_schema=metadata_schema,
                sanitize_column_names=sanitize_column_names,
            )

        if categories_metadata_schema is None:
            categories_metadata_schema = {}
        splitter = cls._get_splitter(
            object_type,
            list(categories_metadata_schema.keys()),
        )

        return cls(
            splitter=splitter,
            df_transform_getter=df_transform_getter,
        )

    @classmethod
    def _get_splitter(
        cls,
        object_type: ObjectTypes,
        categories: Optional[Sequence[str]],
    ) -> Callable[[pd.DataFrame], Iterable[SplitDataFrame]]:
        match object_type:
            case ObjectTypes.ITEM:
                splitter = cls._get_single_column_splitter("category_title")
            case ObjectTypes.EXPERIMENT:
                if not categories:
                    raise ValueError(
                        "For experiments, category names that appear in experiment titles "
                        "must be supplied."
                    )
                logger.warning(
                    "Experiments are split up by template name, which must appear in the title"
                )
                splitter = cls._get_substring_splitter("title", categories)
            case _:
                splitter = cls._get_fake_splitter(single_category=object_type.value)
        return splitter

    @classmethod
    def _get_single_column_splitter(
        cls,
        column_name: str,
    ) -> Callable[[pd.DataFrame], Iterator[SplitDataFrame]]:
        def splitter(df: pd.DataFrame) -> Iterator[SplitDataFrame]:
            column = df[column_name]

            categories = column.unique()
            for category in categories:
                yield SplitDataFrame(
                    key=category,
                    data=df[column == category].reset_index(drop=True),
                )

        return splitter

    @classmethod
    def _get_substring_splitter(
        cls,
        column_name: str,
        categories: Iterable[str],
    ) -> Callable[[pd.DataFrame], Iterator[SplitDataFrame]]:
        def splitter(df: pd.DataFrame) -> Iterator[SplitDataFrame]:
            column = df[column_name]

            for category in categories:
                selector = column.str.contains(category)
                yield SplitDataFrame(
                    key=category,
                    data=df[selector].reset_index(drop=True),
                )

        return splitter

    @classmethod
    def _get_fake_splitter(
        cls,
        single_category: str,
    ) -> Callable[[pd.DataFrame], Iterator[SplitDataFrame]]:
        def splitter(df: pd.DataFrame) -> Iterator[SplitDataFrame]:
            yield SplitDataFrame(key=single_category, data=df)

        return splitter


class PandasDataFrameTransformer:
    """Convert API objects directly to a dataframe, each field corresponding to a column"""

    def __init__(
        self,
        dict_converter: Callable[[Iterable[Dictable]], Iterator[dict[str, Any]]],
    ) -> None:
        self.dict_converter = dict_converter

    def __call__(self, objects: Iterable[Dictable]) -> pd.DataFrame:
        dictionaries = _DictTransformer()(objects)
        raw_table = pd.DataFrame(dictionaries)
        logger.info(
            "Pandas dataframe with %s rows and %s columns created."
            % (raw_table.shape[0], raw_table.shape[1])
        )
        return raw_table

    @classmethod
    def new(cls) -> Self:
        return cls(
            dict_converter=_DictTransformer(),
        )


class PandasDataFrameMetadataTransformer:
    """Transform the metadata of a raw object dataframe"""

    def __init__(
        self,
        object_schema: Optional[Mapping[str, Any]],
        metadata_converter: Optional[Callable[[pd.Series[str]], pd.DataFrame]],
    ) -> None:
        self.object_schema = object_schema
        self.metadata_converter = metadata_converter

    def __call__(self, raw_df: pd.DataFrame) -> pd.DataFrame:
        df = raw_df.copy()
        metadata = df["metadata"] if "metadata" in df else None

        if self.object_schema is not None:
            new_df = df[[]].copy()
            for col, dtype in self.object_schema.items():
                new_df[col] = df[col].astype(dtype)
            df = new_df

        df = df.rename(columns=lambda x: f"_{x}")

        if self.metadata_converter is not None:
            assert metadata is not None
            long_metadata = self.metadata_converter(metadata)
            df = df.join(long_metadata, how="left").reset_index(drop=True)
        return df

    @classmethod
    def get_default_obj_schema(cls, obj_type: ObjectTypes) -> dict[str, str]:
        match obj_type:
            case ObjectTypes.ITEM:
                schema = DEFAULT_ITEM_SCHEMA
            case ObjectTypes.EXPERIMENT:
                schema = DEFAULT_EXPERIMENT_SCHEMA
            case ObjectTypes.EXPERIMENTS_TEMPLATE:
                schema = DEFAULT_EXPERIMENTS_TEMPLATE_SCHEMA
            case ObjectTypes.ITEMS_TYPE:
                schema = DEFAULT_ITEMS_TYPE_SCHEMA
            case _:
                raise ValueError(f"Object type '{obj_type}' not recognized")
        return schema

    @classmethod
    def for_long_table(
        cls,
        object_type: ObjectTypes,
    ) -> PandasDataFrameMetadataTransformer:
        """Create a transformer to produce long tables"""
        object_schema = cls.get_default_obj_schema(object_type)
        metadata_converter = LongMetadataTransformer.new()
        return cls(
            metadata_converter=metadata_converter,
            object_schema=object_schema,
        )

    @classmethod
    def for_wide_table(
        cls,
        object_type: ObjectTypes,
        cell_content: TableCellContentType = TableCellContentType.COMBINED,
        metadata_schema: Optional[Mapping[str, Any]] = None,
        sanitize_column_names: bool = False,
    ) -> PandasDataFrameMetadataTransformer:
        """Create a transformer to produce long tables"""
        metadata_converter = WideMetadataTransformer.new(
            cell_content=cell_content,
            metadata_schema=metadata_schema,
            sanitize_column_names=sanitize_column_names,
        )
        object_schema = cls.get_default_obj_schema(object_type)

        return cls(
            metadata_converter=metadata_converter,
            object_schema=object_schema,
        )


class WideMetadataTransformer:
    """Converts a pandas Series of eLabFTW metadata strings to wide format dataframe"""

    def __init__(
        self,
        metadata_field_parser: Callable[[str], MetadataModel],
        metadata_field_transformer: Callable[[MetadataModel], dict[str, Any]],
        metadata_schema: Optional[Mapping[str, Any]],
        metadata_schema_guesser: Callable[[Sequence[MetadataModel]], dict[str, Any]],
        column_name_sanitizer: Optional[Callable[[str], str]],
    ) -> None:
        self.metadata_field_parser = metadata_field_parser
        self.metadata_field_transformer = metadata_field_transformer
        self.metadata_schema = metadata_schema
        self.metadata_schema_guesser = metadata_schema_guesser
        self.column_name_sanitizer = column_name_sanitizer

    @classmethod
    def new(
        cls,
        cell_content: TableCellContentType,
        metadata_schema: Optional[Mapping[str, Any]],
        sanitize_column_names: bool,
    ) -> WideMetadataTransformer:
        column_name_sanitizer = (
            sanitize_name_for_glue if sanitize_column_names else None
        )

        def metadata_schema_guesser(
            metadata: Sequence[MetadataModel],
        ) -> dict[str, str]:
            return cls.guess_schema(
                parsed_metadata=metadata,
                metadata_schema_transformer=ParsedMetadataToPandasDtype.new(
                    cell_content
                ),
            )

        return cls(
            metadata_field_parser=MetadataParser(),
            metadata_field_transformer=ParsedMetadataToSimpleDict.new(cell_content),
            metadata_schema=metadata_schema,
            metadata_schema_guesser=metadata_schema_guesser,
            column_name_sanitizer=column_name_sanitizer,
        )

    def __call__(self, metadata: pd.Series[str]) -> pd.DataFrame:
        def parse_and_transform(data: str) -> dict[str, Any]:
            parsed = self.metadata_field_parser(data)
            return self.metadata_field_transformer(parsed)

        parsed_metadata = [self.metadata_field_parser(_meta) for _meta in metadata]
        extra_fields = [
            self.metadata_field_transformer(_meta) for _meta in parsed_metadata
        ]
        df = pd.DataFrame(extra_fields).replace("", None)

        original_columns = df.columns

        if self.metadata_schema is None:
            metadata_schema = self.metadata_schema_guesser(parsed_metadata)
            metadata_schema = {col: metadata_schema[col] for col in original_columns}

        new_df = df[[]].copy()
        for col, dtype in metadata_schema.items():
            if col in df:
                new_df[col] = df[col].astype(dtype)
            else:
                new_df[col] = float("nan")

        if self.column_name_sanitizer is not None:
            new_df = new_df.rename(columns=self.column_name_sanitizer)

        return new_df

    @classmethod
    def guess_schema(
        cls,
        parsed_metadata: Sequence[MetadataModel],
        metadata_schema_transformer: Callable[[MetadataModel], dict[str, str]],
    ) -> dict[str, Any]:
        """Return the schema of the pandas table based on column mode type"""
        dtypes = [metadata_schema_transformer(metadata) for metadata in parsed_metadata]
        dtype_df = pd.DataFrame(dtypes)

        most_common_type = {}
        for col in dtype_df.columns:
            mode = dtype_df[col].dropna().mode()
            most_common = mode.iloc[0] if not mode.empty else "string"
            most_common_type[col] = most_common
        return most_common_type


class LongMetadataTransformer:
    """Converts a pandas Series of eLabFTW metadata strings to long format dataframe"""

    def __init__(
        self,
        metadata_field_parser: Callable[[str], MetadataModel],
        metadata_field_transformer: Callable[[MetadataModel], list[MetadataField]],
    ) -> None:
        self.metadata_field_parser = metadata_field_parser
        self.metadata_field_transformer = metadata_field_transformer

    def __call__(self, metadata: pd.Series[str]) -> pd.DataFrame:
        def parse_and_transform(data: str) -> list[MetadataField]:
            parsed = self.metadata_field_parser(data)
            return self.metadata_field_transformer(parsed)

        extra_fields = metadata.apply(parse_and_transform).explode().dropna()
        df = (
            pd.DataFrame(
                extra_fields.tolist(),
                index=extra_fields.index,
            )
            .replace("", None)
            .rename(columns=lambda col: f"field_{col}")
        )

        return df

    @classmethod
    def new(cls) -> Self:
        return cls(
            metadata_field_parser=MetadataParser(),
            metadata_field_transformer=ParsedMetadataToMetadataFieldList(),
        )


class LazyWideTableUtils:
    @classmethod
    def create_header(
        self,
        object_columns: Sequence[str],
        metadata_columns: Sequence[str],
        column_name_sanitizer: Optional[Callable[[str], str]] = None,
    ) -> list[str]:
        header = [f"_{col}" for col in object_columns]
        if column_name_sanitizer:
            field_columns: Iterable[str] = map(
                column_name_sanitizer,
                metadata_columns,
            )
        else:
            field_columns = metadata_columns
        header.extend(field_columns)
        return header

    @classmethod
    def sanitize_cell_value(cls, cell_value: Any) -> Any:
        if isinstance(cell_value, str):
            transformed = cell_value.replace("\n", "\\n").replace('"', "")
            return transformed
        else:
            return cell_value

    @classmethod
    def transform_object_to_row_wide(
        cls,
        obj: HasIDAndMetadata,
        object_columns: Sequence[str],
        metadata_columns: Sequence[str],
        metadata_parser: Callable[[Optional[str]], MetadataModel],
        dict_converter: Callable[[MetadataModel], dict[str, Any]],
        column_name_sanitizer: Optional[Callable[[str], str]],
    ) -> dict[str, Any]:
        row_data = {}

        if column_name_sanitizer is None:
            column_name_sanitizer = do_nothing

        for column in object_columns:
            row_data[f"_{column}"] = getattr(obj, column, None)

        parsed_metadata = metadata_parser(obj.metadata)
        field_dictionary = dict_converter(parsed_metadata)

        for column in metadata_columns:
            cell_value = cls.sanitize_cell_value(field_dictionary.get(column))
            row_data[column_name_sanitizer(column)] = cell_value

        return row_data


class CSVTransformer:
    """Converts API objects data to a wide table format row by row"""

    def __init__(
        self,
        object_columns: Sequence[str],
        metadata_columns: Sequence[str],
        metadata_parser: Callable[[Optional[str]], MetadataModel],
        dict_converter: Callable[[MetadataModel], dict[str, Any]],
        column_name_sanitizer: Optional[Callable[[str], str]],
    ) -> None:
        self.object_columns = object_columns
        self.metadata_columns = metadata_columns
        self.dict_converter = dict_converter
        self.column_name_sanitizer = column_name_sanitizer
        self.metadata_parser = metadata_parser

    def __call__(
        self,
        objects: Iterable[HasIDAndMetadata],
    ) -> Iterable[dict[str, Any]]:
        return map(self._transform, objects)

    def get_header(self) -> list[str]:
        return LazyWideTableUtils.create_header(
            object_columns=self.object_columns,
            metadata_columns=self.metadata_columns,
            column_name_sanitizer=self.column_name_sanitizer,
        )

    @classmethod
    def new(
        cls,
        object_columns: Sequence[str],
        metadata_columns: Sequence[str],
        cell_content: TableCellContentType,
        sanitize_columns: bool,
    ) -> CSVTransformer:
        return cls(
            object_columns=object_columns,
            metadata_columns=metadata_columns,
            metadata_parser=MetadataParser(),
            dict_converter=ParsedMetadataToSimpleDict.new(cell_content),
            column_name_sanitizer=sanitize_name_for_glue if sanitize_columns else None,
        )

    def _transform(
        self,
        obj: HasIDAndMetadata,
    ) -> dict[str, Any]:
        return LazyWideTableUtils.transform_object_to_row_wide(
            obj=obj,
            object_columns=self.object_columns,
            metadata_columns=self.metadata_columns,
            metadata_parser=self.metadata_parser,
            dict_converter=self.dict_converter,
            column_name_sanitizer=self.column_name_sanitizer,
        )


class CSVLongMetadataTableTransformer:
    """Convert all metadata from objects to a long fixed schema table"""

    SCHEMA: tuple[str, ...] = MetadataField._fields

    def __init__(
        self,
        metadata_parser: Callable[[Optional[str]], MetadataModel],
        metadatafield_transformer: Callable[[MetadataModel], list[MetadataField]],
        field_sanitizer: Callable[[Any], Any],
    ) -> None:
        self.metadata_parser = metadata_parser
        self.metadatafield_transformer = metadatafield_transformer
        self.field_sanitizer = field_sanitizer

    @classmethod
    def new(cls) -> Self:
        return cls(
            metadata_parser=MetadataParser(),
            metadatafield_transformer=ParsedMetadataToMetadataFieldList(),
            field_sanitizer=LazyWideTableUtils.sanitize_cell_value,
        )

    def __call__(
        self,
        objects: Iterable[HasIDAndMetadata],
    ) -> Iterator[dict[str, Any]]:
        for obj in objects:
            fields = self._transform(obj)
            for field in fields:
                yield field

    def get_header(self) -> tuple[str, ...]:
        return self.SCHEMA

    def _transform(self, obj: HasIDAndMetadata) -> Iterator[dict[str, Any]]:
        """Convert a single API object to a bunch of metadata fields"""
        fields = self.metadatafield_transformer(self.metadata_parser(obj.metadata))
        for field in fields:
            field_dict = {f"field_{k}": v for k, v in field._asdict().items()}
            field_dict["_id"] = obj.id
            field_dict["field_value"] = self.field_sanitizer(field.value)
            yield field_dict


class CSVElabDataTableTransformer:
    """Convert elabftw data of objects to a table with fixed schema"""

    def __init__(
        self,
        columns: Sequence[str],
        column_name_sanitizer: Optional[Callable[[str], str]],
    ) -> None:
        self.columns = columns
        self.column_name_sanitizer = column_name_sanitizer

    def __call__(
        self,
        objects: Iterable[Any],
    ) -> Iterable[dict[str, Any]]:
        return map(self.transform, objects)

    def get_header(self) -> list[str]:
        if self.column_name_sanitizer:
            return list(map(self.column_name_sanitizer, self.columns))
        else:
            return list(self.columns)

    @classmethod
    def new(
        cls,
        columns: Sequence[str],
        sanitize_columns: bool,
    ) -> CSVElabDataTableTransformer:
        return cls(
            columns=columns,
            column_name_sanitizer=sanitize_name_for_glue if sanitize_columns else None,
        )

    def transform(
        self,
        obj: Any,
    ) -> dict[str, Any]:
        row_data = {}

        for column in self.columns:
            if hasattr(obj, column):
                renamed_column = (
                    self.column_name_sanitizer(column)
                    if self.column_name_sanitizer
                    else column
                )
                row_data[renamed_column] = getattr(obj, column)

        return row_data


class WideObjectTableData(NamedTuple):
    table_name: str
    object_columns: Sequence[str]
    metadata_columns: Sequence[str]
    objects: Iterable[HasIDAndMetadata]


class ExcelTransformer:
    def __init__(
        self,
        metadata_parser: Callable[[Optional[str]], MetadataModel],
        dict_converter: Callable[[MetadataModel], dict[str, Any]],
        column_name_sanitizer: Optional[Callable[[str], str]],
    ) -> None:
        self.metadata_parser = metadata_parser
        self.dict_converter = dict_converter
        self.column_name_sanitizer = column_name_sanitizer

    @classmethod
    def new(
        cls,
        cell_content: TableCellContentType,
        sanitize_columns: bool,
    ) -> ExcelTransformer:
        return cls(
            metadata_parser=MetadataParser(),
            dict_converter=ParsedMetadataToSimpleDict.new(cell_content),
            column_name_sanitizer=sanitize_name_for_glue if sanitize_columns else None,
        )

    def __call__(
        self,
        sheets: Iterable[WideObjectTableData],
    ) -> Iterator[SplitDataFrame]:
        return map(self.transform_sheet_to_dataframe, sheets)

    def transform_sheet_to_dataframe(
        self,
        sheet: WideObjectTableData,
    ) -> SplitDataFrame:
        header = LazyWideTableUtils.create_header(
            object_columns=sheet.object_columns,
            metadata_columns=sheet.metadata_columns,
            column_name_sanitizer=self.column_name_sanitizer,
        )
        df = pd.DataFrame(
            (
                self.transform_object_to_row(
                    obj=obj,
                    metadata_columns=sheet.metadata_columns,
                    object_columns=sheet.object_columns,
                )
                for obj in sheet.objects
            ),
            columns=header,
        )
        return SplitDataFrame(
            key=sheet.table_name,
            data=df,
        )

    def transform_object_to_row(
        self,
        obj: HasIDAndMetadata,
        object_columns: Sequence[str],
        metadata_columns: Sequence[str],
    ) -> dict[str, Any]:
        return LazyWideTableUtils.transform_object_to_row_wide(
            obj=obj,
            object_columns=object_columns,
            metadata_columns=metadata_columns,
            metadata_parser=self.metadata_parser,
            dict_converter=self.dict_converter,
            column_name_sanitizer=self.column_name_sanitizer,
        )
