from __future__ import annotations

import json
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

from elabftwcontrol._logging import logger
from elabftwcontrol.download.interfaces import Dictable, HasIDAndMetadata
from elabftwcontrol.download.metadata import (
    MetadataField,
    MetadataParser,
    ParsedMetadataToMetadataFieldList,
    ParsedMetadataToPandasDtype,
    ParsedMetadataToSimpleDict,
    TableCellContentType,
)
from elabftwcontrol.download.utils import do_nothing, sanitize_name_for_glue

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


class JSONTransformer:
    def __init__(
        self,
        indent: Optional[int] = 2,
    ) -> None:
        self.indent = indent

    def __call__(
        self,
        objects: Iterable[Dictable],
    ) -> Iterator[str]:
        dictionaries = _DictTransformer()(objects)
        jsonstrings = _JSONStringTransformer(indent=self.indent)(dictionaries)
        return jsonstrings

    @classmethod
    def transform(
        cls,
        obj: Dictable,
        indent: Optional[int],
    ) -> str:
        dictionary = _DictTransformer.transform(obj)
        return _JSONStringTransformer.transform(dictionary, indent)


class _DictTransformer:
    def __call__(self, objects: Iterable[Dictable]) -> Iterator[dict[str, Any]]:
        for obj in objects:
            yield self.transform(obj)

    @classmethod
    def transform(
        cls,
        obj: Dictable,
    ) -> dict[str, Any]:
        return obj.to_dict()


class _JSONStringTransformer:
    def __init__(
        self,
        indent: Optional[int],
    ) -> None:
        self.indent = indent

    def __call__(self, objects: Iterable[dict[str, Any]]) -> Iterator[str]:
        for obj in objects:
            yield self.transform(obj, self.indent)

    @classmethod
    def transform(
        cls,
        obj: dict[str, Any],
        indent: Optional[int],
    ) -> str:
        return json.dumps(obj, indent=indent)


class TableShapes(str, Enum):
    WIDE = "wide"
    LONG = "long"


class ObjectTypes(str, Enum):
    ITEM = "item"
    ITEMS_TYPE = "items_type"
    EXPERIMENT = "experiment"
    EXPERIMENTS_TEMPLATE = "experiments_template"


class SplitDataFrame(NamedTuple):
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

    @classmethod
    def for_raw_tables(
        cls,
        object_type: ObjectTypes,
        categories: Optional[Sequence[str]] = None,
    ) -> MultiPandasDataFrameTransformer:
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
    ) -> MultiPandasDataFrameTransformer:
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
        order_columns: bool = False,
    ) -> MultiPandasDataFrameTransformer:
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
                order_columns=order_columns,
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
                splitter = cls.get_single_column_splitter("category_title")
            case ObjectTypes.EXPERIMENT:
                if not categories:
                    raise ValueError(
                        "For experiments, category names that appear in experiment titles "
                        "must be supplied."
                    )
                logger.warning(
                    "Experiments are split up by template name, which must appear in the title"
                )
                splitter = cls.get_substring_splitter("title", categories)
            case _:
                splitter = cls.get_fake_splitter(single_category=object_type)
        return splitter

    def __call__(self, raw_df: pd.DataFrame) -> Iterator[SplitDataFrame]:
        return self.transform(
            raw_df,
            splitter=self.splitter,
            df_transform_getter=self.df_transform_getter,
        )

    @classmethod
    def transform(
        cls,
        raw_df: pd.DataFrame,
        splitter: Callable[[pd.DataFrame], Iterable[SplitDataFrame]],
        df_transform_getter: Callable[[str], Callable[[pd.DataFrame], pd.DataFrame]],
    ) -> Iterator[SplitDataFrame]:
        for key, df in splitter(raw_df):
            transformer = df_transform_getter(key)
            transformed = transformer(df)
            yield SplitDataFrame(
                key=key,
                data=transformed,
            )

    @classmethod
    def get_single_column_splitter(
        cls,
        column_name: str,
    ) -> Callable[[pd.DataFrame], Iterator[SplitDataFrame]]:
        def splitter(df: pd.DataFrame) -> Iterator[SplitDataFrame]:
            column = df[column_name]

            categories = column.unique()
            for category in categories:
                yield SplitDataFrame(key=category, data=df[column == category])

        return splitter

    @classmethod
    def get_substring_splitter(
        cls,
        column_name: str,
        categories: Iterable[str],
    ) -> Callable[[pd.DataFrame], Iterator[SplitDataFrame]]:
        def splitter(df: pd.DataFrame) -> Iterator[SplitDataFrame]:
            column = df[column_name]

            for category in categories:
                selector = column.str.contains(category)
                yield SplitDataFrame(key=category, data=df[selector])

        return splitter

    @classmethod
    def get_fake_splitter(
        cls,
        single_category: str,
    ) -> Callable[[pd.DataFrame], Iterator[SplitDataFrame]]:
        def splitter(df: pd.DataFrame) -> Iterator[SplitDataFrame]:
            yield SplitDataFrame(key=single_category, data=df)

        return splitter


class PandasDataFrameTransformer:
    """Convert API objects directly to a dataframe, each field corresponding to a column"""

    def __call__(self, objects: Iterable[Dictable]) -> pd.DataFrame:
        raw_table = self.convert_objects_to_df(objects)
        logger.info(
            "Pandas dataframe with %s rows and %s columns created."
            % (raw_table.shape[0], raw_table.shape[1])
        )
        return raw_table

    @classmethod
    def convert_objects_to_df(cls, objects: Iterable[Dictable]) -> pd.DataFrame:
        dictionaries = _DictTransformer()(objects)
        return pd.DataFrame(dictionaries)


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
        return self.transform(
            raw_df,
            object_schema=self.object_schema,
            metadata_converter=self.metadata_converter,
        )

    @classmethod
    def transform(
        cls,
        raw_df: pd.DataFrame,
        object_schema: Optional[Mapping[str, Any]],
        metadata_converter: Optional[Callable[[pd.Series[str]], pd.DataFrame]],
    ) -> pd.DataFrame:
        df = raw_df.copy()
        metadata = df["metadata"] if "metadata" in df else None

        if object_schema is not None:
            new_df = df[[]].copy()
            for col, dtype in object_schema.items():
                new_df[col] = df[col].astype(dtype)
            df = new_df

        df = df.rename(columns=lambda x: f"_{x}")

        if metadata_converter is not None:
            assert metadata is not None
            long_metadata = metadata_converter(metadata)
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
        object_schema=cls.get_default_obj_schema(object_type)
        metadata_converter=LongMetadataTransformer.new()
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
        order_columns: bool = False,
    ) -> PandasDataFrameMetadataTransformer:
        """Create a transformer to produce long tables"""
        metadata_converter = WideMetadataTransformer.new(
            cell_content=cell_content,
            metadata_schema=metadata_schema,
            sanitize_column_names=sanitize_column_names,
            order_columns=order_columns,
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
        metadata_field_parser: Callable[[str], dict[str, Any]],
        metadata_field_transformer: Callable[[dict[str, Any]], dict[str, Any]],
        metadata_schema: Optional[Mapping[str, Any]],
        metadata_schema_guesser: Callable[[pd.Series], dict[str, Any]],
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
        order_columns: bool,
    ) -> WideMetadataTransformer:
        column_name_sanitizer = (
            sanitize_name_for_glue if sanitize_column_names else None
        )
        return cls(
            metadata_field_parser=MetadataParser(),
            metadata_field_transformer=ParsedMetadataToSimpleDict.new(
                cell_content,
                order_fields=order_columns,
            ),
            metadata_schema=metadata_schema,
            metadata_schema_guesser=lambda x: cls.guess_schema(
                cell_content=cell_content,
                parsed_metadata=x,
            ),
            column_name_sanitizer=column_name_sanitizer,
        )

    def __call__(self, metadata: pd.Series[str]) -> pd.DataFrame:
        return self.unroll_metadata_column(
            metadata,
            metadata_field_parser=self.metadata_field_parser,
            metadata_field_transformer=self.metadata_field_transformer,
            metadata_schema=self.metadata_schema,
            metadata_schema_guesser=self.metadata_schema_guesser,
            column_name_sanitizer=self.column_name_sanitizer,
        )

    @classmethod
    def unroll_metadata_column(
        cls,
        metadata: pd.Series[str],
        metadata_field_parser: Callable[[str], dict[str, Any]],
        metadata_field_transformer: Callable[[dict[str, Any]], dict[str, Any]],
        metadata_schema: Optional[Mapping[str, Any]],
        metadata_schema_guesser: Callable[[pd.Series], dict[str, Any]],
        column_name_sanitizer: Optional[Callable[[str], str]],
    ) -> pd.DataFrame:
        parsed_metadata = metadata.apply(metadata_field_parser)
        extra_fields = parsed_metadata.apply(metadata_field_transformer)
        df = pd.DataFrame(
            extra_fields.tolist(),
            index=extra_fields.index,
        ).replace("", None)

        original_columns = df.columns

        if metadata_schema is None:
            metadata_schema = metadata_schema_guesser(parsed_metadata)
            metadata_schema = {col: metadata_schema[col] for col in original_columns}

        new_df = df[[]].copy()
        for col, dtype in metadata_schema.items():
            if col in df:
                new_df[col] = df[col].astype(dtype)
            else:
                new_df[col] = float("nan")

        if column_name_sanitizer is not None:
            new_df = new_df.rename(columns=column_name_sanitizer)

        return new_df

    @classmethod
    def guess_schema(
        cls,
        cell_content: TableCellContentType,
        parsed_metadata: pd.Series[Any],
    ) -> dict[str, Any]:
        transformer = ParsedMetadataToPandasDtype.new(cell_content)
        dtypes = parsed_metadata.apply(transformer)
        dtype_df = pd.DataFrame(dtypes.tolist())

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
        metadata_field_parser: Callable[[str], dict[str, Any]],
        metadata_field_transformer: Callable[[dict[str, Any]], list[MetadataField]],
    ) -> None:
        self.metadata_field_parser = metadata_field_parser
        self.metadata_field_transformer = metadata_field_transformer

    def __call__(self, metadata: pd.Series[str]) -> pd.DataFrame:
        return self.unroll_metadata_column(
            metadata,
            metadata_field_parser=self.metadata_field_parser,
            metadata_field_transformer=self.metadata_field_transformer,
        )

    @classmethod
    def new(cls) -> LongMetadataTransformer:
        return cls(
            metadata_field_parser=MetadataParser(),
            metadata_field_transformer=ParsedMetadataToMetadataFieldList(),
        )

    @classmethod
    def unroll_metadata_column(
        cls,
        metadata: pd.Series[str],
        metadata_field_parser: Callable[[str], dict[str, Any]],
        metadata_field_transformer: Callable[[dict[str, Any]], list[MetadataField]],
    ) -> pd.DataFrame:
        parsed_metadata = metadata.apply(metadata_field_parser)
        extra_fields = (
            parsed_metadata.apply(metadata_field_transformer).explode().dropna()
        )
        df = (
            pd.DataFrame(
                extra_fields.tolist(),
                index=extra_fields.index,
            )
            .replace("", None)
            .rename(columns=lambda col: f"field_{col}")
        )

        return df


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
    def get_field_to_cell_value_converter(
        cls,
        cell_content: TableCellContentType,
    ) -> Callable[[MetadataField], Any]:
        field_to_cell_value = ParsedMetadataToSimpleDict.get_cell_content_getter(
            cell_content
        )

        def field_to_cell_value_converter(field: MetadataField) -> Any:
            return cls.sanitize_cell_value(field_to_cell_value(field))

        return field_to_cell_value_converter

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
        field_to_cell_value: Callable[[MetadataField], Any],
        column_name_sanitizer: Optional[Callable[[str], str]],
        metadata_parser: Callable[[Optional[str]], list[MetadataField]],
    ) -> dict[str, Any]:
        row_data = {}

        if column_name_sanitizer is None:
            column_name_sanitizer = do_nothing

        for column in object_columns:
            row_data[f"_{column}"] = getattr(obj, column, None)

        fields = metadata_parser(obj.metadata)
        field_map = {field.name: field for field in fields}
        for column in metadata_columns:
            if column in field_map:
                column_value = field_to_cell_value(field_map[column])
            else:
                column_value = None
            row_data[column_name_sanitizer(column)] = column_value

        return row_data

    @classmethod
    def metadata_parser(cls, metadata: Optional[str]) -> list[MetadataField]:
        parsed_metadata = MetadataParser()(metadata)
        return ParsedMetadataToMetadataFieldList()(parsed_metadata)


class CSVTransformer:
    """Converts API objects data to a wide table format row by row"""

    def __init__(
        self,
        object_columns: Sequence[str],
        metadata_columns: Sequence[str],
        field_to_cell_value: Callable[[MetadataField], Any],
        column_name_sanitizer: Optional[Callable[[str], str]],
        metadata_parser: Callable[[Optional[str]], list[MetadataField]],
    ) -> None:
        self.object_columns = object_columns
        self.metadata_columns = metadata_columns
        self.field_to_cell_value = field_to_cell_value
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
        field_to_cell_value = LazyWideTableUtils.get_field_to_cell_value_converter(
            cell_content
        )

        return cls(
            object_columns=object_columns,
            metadata_columns=metadata_columns,
            field_to_cell_value=field_to_cell_value,
            column_name_sanitizer=sanitize_name_for_glue if sanitize_columns else None,
            metadata_parser=LazyWideTableUtils.metadata_parser,
        )

    def _transform(
        self,
        obj: HasIDAndMetadata,
    ) -> dict[str, Any]:
        return LazyWideTableUtils.transform_object_to_row_wide(
            obj=obj,
            object_columns=self.object_columns,
            metadata_columns=self.metadata_columns,
            field_to_cell_value=self.field_to_cell_value,
            column_name_sanitizer=self.column_name_sanitizer,
            metadata_parser=self.metadata_parser,
        )


class CSVLongMetadataTableTransformer:
    """Convert all metadata from objects to a long fixed schema table"""

    SCHEMA: tuple[str, ...] = MetadataField._fields

    def __call__(
        self,
        objects: Iterable[HasIDAndMetadata],
    ) -> Iterator[dict[str, Any]]:
        for obj in objects:
            fields = self.transform(
                obj,
                metadata_parser=LazyWideTableUtils.metadata_parser,
                field_sanitizer=LazyWideTableUtils.sanitize_cell_value,
            )
            for field in fields:
                yield field

    def get_header(self) -> tuple[str, ...]:
        return self.SCHEMA

    @classmethod
    def transform(
        cls,
        obj: HasIDAndMetadata,
        metadata_parser: Callable[[Optional[str]], list[MetadataField]],
        field_sanitizer: Callable[[Any], Any],
    ) -> Iterator[dict[str, Any]]:
        fields: list[MetadataField] = metadata_parser(obj.metadata)
        for field in fields:
            field_dict = {f"field_{k}": v for k, v in field._asdict().items()}
            field_dict["_id"] = obj.id
            field_dict["field_value"] = field_sanitizer(field.value)
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
        return map(self._transform, objects)

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

    def _transform(
        self,
        obj: Any,
    ) -> dict[str, Any]:
        return self.transform(
            obj=obj,
            columns=self.columns,
            column_name_sanitizer=self.column_name_sanitizer,
        )

    @classmethod
    def transform(
        cls,
        obj: Any,
        columns: Sequence[str],
        column_name_sanitizer: Optional[Callable[[str], str]],
    ) -> dict[str, Any]:
        row_data = {}

        for column in columns:
            if hasattr(obj, column):
                renamed_column = (
                    column_name_sanitizer(column) if column_name_sanitizer else column
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
        field_to_cell_value: Callable[[MetadataField], Any],
        column_name_sanitizer: Optional[Callable[[str], str]],
        metadata_parser: Callable[[Optional[str]], list[MetadataField]],
    ) -> None:
        self.field_to_cell_value = field_to_cell_value
        self.column_name_sanitizer = column_name_sanitizer
        self.metadata_parser = metadata_parser

    @classmethod
    def new(
        cls,
        cell_content: TableCellContentType,
        sanitize_columns: bool,
    ) -> ExcelTransformer:
        field_to_cell_value = LazyWideTableUtils.get_field_to_cell_value_converter(
            cell_content
        )

        return cls(
            field_to_cell_value=field_to_cell_value,
            column_name_sanitizer=sanitize_name_for_glue if sanitize_columns else None,
            metadata_parser=LazyWideTableUtils.metadata_parser,
        )

    def __call__(
        self,
        sheets: Iterable[WideObjectTableData],
    ) -> Iterator[SplitDataFrame]:
        return map(self.transform_sheet_to_dataframe, sheets)

    def transform_sheet_to_dataframe(
        self, sheet: WideObjectTableData
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
            field_to_cell_value=self.field_to_cell_value,
            column_name_sanitizer=self.column_name_sanitizer,
            metadata_parser=self.metadata_parser,
        )
