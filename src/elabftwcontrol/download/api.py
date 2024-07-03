from __future__ import annotations

import json
from enum import Enum
from functools import cached_property
from itertools import chain
from typing import Any, Callable, Collection, Iterable, Iterator, NamedTuple, Optional

import pandas as pd
from pydantic import BaseModel, model_validator

from elabftwcontrol.client import ElabftwApi, Experiment, Item
from elabftwcontrol.defaults import logger
from elabftwcontrol.download.interfaces import Category, Dictable
from elabftwcontrol.download.metadata import MetadataParser, ParsedMetadataToPandasDtype
from elabftwcontrol.download.output import (
    ExcelWriter,
    LineWriter,
    OutputFormats,
    PandasWriter,
)
from elabftwcontrol.download.transformers import (
    JSONTransformer,
    MultiPandasDataFrameTransformer,
    ObjectTypes,
    PandasDataFrameMetadataTransformer,
    PandasDataFrameTransformer,
    SplitDataFrame,
    TableCellContentType,
    TableShapes,
    TableTypes,
)


class MetadataColumns(str, Enum):
    TEMPLATE = "template"
    ALL = "all"


class IngestConfiguration(BaseModel):
    """Parameters for downloading items and experiment data from eLabFTW and converting to a number of formats

    Parameters
    ----------
    object_type
        item or experiment
    format
        Output formats. Available are json, csv, parquet, and excel.
    output
        Path to a file where the data should be stored. Defaults to stdout. Typically a path
        to a local file. If you have the AWS dependencies installed and you select
        csv or parquet, you can write directly to s3 using a path prefixed by
        s3://<bucket>/<prefix>.
    categories
        The items types or templates of the items or experiments to download. By default all
        objects are downloaded. For single table formats (csv, parquet) in wide format, it is
        recommended to select individual categories. For experiments, the template name
        must appear in the template of the experiment.
    indent
        Whether to indent json data that is returned. Only applies to JSON.
    table_shape
        Only relevant to tabular formats. Defines whether metadata information is
        expanded to individual columns (wide) or stacked in different rows (long).
    table_type
        Only relevant to tabular formats. Whether to have only object data, metadata, or both
        in the columns of the table.
    metadata_columns
        Only relevant to wide tabular formats. Whether to add metadata columns of all items
        or only those defined in the template of the category.
    cell_content
        For wide tabular formats, the information that is put in columns corresponding to
        metadata fields. This is relevant for fields that have units. By default value
        and unit are combined into a string. If combined is selected then all metadata
        will become strings.
    sanitize_column_names
        Only relevant to tabular formats. Whether to keep the field names as column names or
        sanitize them to a simple character set that is compliant with column name rules in
        AWS Glue tables.
    glue_table
        Only relevant to tabular formats. If you write data directly to S3 you can register
        the table as a glue table. Input must be in the format: <glue db>:<glue table>.
    """

    object_type: ObjectTypes = ObjectTypes.ITEM
    format: OutputFormats = OutputFormats.JSON
    output: Optional[str] = None
    categories: Optional[list[str]] = None
    indent: bool = False
    table_shape: TableShapes = TableShapes.WIDE
    table_type: TableTypes = TableTypes.COMBINED
    metadata_columns: MetadataColumns = MetadataColumns.TEMPLATE
    cell_content: TableCellContentType = TableCellContentType.COMBINED
    sanitize_column_names: bool = False
    glue_table: Optional[str] = None

    @model_validator(mode="after")
    def check_consistency(self) -> IngestConfiguration:
        self._check_one_category_wide_single_table()
        return self

    def _check_one_category_wide_single_table(self) -> None:
        if (
            self.format in (OutputFormats.PARQUET, OutputFormats.CSV)
            and self.table_shape == TableShapes.WIDE
            and (not self.categories or len(self.categories) != 1)
            and self.metadata_columns == MetadataColumns.TEMPLATE
            and self.table_type != TableTypes.OBJECT
        ):
            raise ValueError(
                "A single wide table format with metadata must specify one category"
            )


class IngestJob:
    def __init__(
        self,
        api: ElabftwApi,
        config: IngestConfiguration,
    ) -> None:
        self.api = api
        self.config = config

    def __call__(self) -> None:
        object_type = self.config.object_type
        logger.info("Starting ingestion for %s" % object_type)
        match object_type:
            case ObjectTypes.ITEM:
                self.ingest_items_data(api=self.api, config=self.config)
            case ObjectTypes.EXPERIMENT:
                self.ingest_experiments_data(api=self.api, config=self.config)
            case ObjectTypes.ITEMS_TYPE:
                self.ingest_items_types_data(api=self.api, config=self.config)
            case ObjectTypes.EXPERIMENTS_TEMPLATE:
                self.ingest_experiments_template_data(api=self.api, config=self.config)
            case _:
                raise NotImplementedError(
                    f"Ingestion for object type '{object_type}' not implemented"
                )
        logger.info("Completed ingestion")

    @classmethod
    def ingest_experiments_data(
        cls,
        api: ElabftwApi,
        config: IngestConfiguration,
    ) -> None:
        category_info = CategoryInfo(getter=api.experiments_templates.iter)
        experiments = cls.get_experiments(api)

        cls.transform_and_save_objects(
            objects=experiments,
            category_info=category_info,
            config=config,
        )

    @classmethod
    def ingest_experiments_template_data(
        cls,
        api: ElabftwApi,
        config: IngestConfiguration,
    ) -> None:
        def throw_error() -> Any:
            raise ValueError(
                "This operation requires information on categories, but "
                "experiment templates are categories themselves."
            )

        category_info = CategoryInfo(getter=throw_error)

        if config.categories:
            templates_info = CategoryInfo(getter=api.experiments_templates.iter)
            template_ids = templates_info.get_ids(categories=config.categories)
            templates = [
                templates_info.categories_map[template_id]
                for template_id in template_ids
            ]
        else:
            templates = list(api.experiments_templates.iter())

        cls.transform_and_save_objects(
            objects=templates,
            category_info=category_info,
            config=config,
        )

    @classmethod
    def ingest_items_data(
        cls,
        api: ElabftwApi,
        config: IngestConfiguration,
    ) -> None:
        if not config.categories:
            included_item_ids = None
        else:
            included_item_ids = CategoryInfo(api.items_types.iter).get_ids(
                categories=config.categories
            )
            logger.debug("Item types categories specified: %s" % config.categories)
            logger.debug("Item types ids included: %s" % included_item_ids)
        items = cls.get_items(api, included_item_ids)

        category_info = CategoryInfo(api.items_types.iter_full)
        cls.transform_and_save_objects(
            objects=items,
            category_info=category_info,
            config=config,
        )

    @classmethod
    def ingest_items_types_data(
        cls,
        api: ElabftwApi,
        config: IngestConfiguration,
    ) -> None:
        def throw_error() -> Any:
            raise ValueError(
                "This operation requires information on categories, but "
                "items types are categories themselves."
            )

        category_info = CategoryInfo(getter=throw_error)

        items_types_info = CategoryInfo(api.items_types.iter)
        category_ids = items_types_info.get_ids(config.categories)
        item_types = api.items_types.iter_full(category_ids)

        cls.transform_and_save_objects(
            objects=item_types,
            category_info=category_info,
            config=config,
        )

    @classmethod
    def transform_and_save_objects(
        cls,
        objects: Iterable[Dictable],
        category_info: CategoryInfo,
        config: IngestConfiguration,
    ) -> None:
        if config.format == OutputFormats.JSON:
            logger.info("Ingesting as JSON")
            JSONConverter(config)(objects)
        else:
            if config.format == OutputFormats.EXCEL:
                logger.info(
                    "Attempting to process metadata, convert to multiple dataframes, "
                    "and save to Excel."
                )
                excel_ingester = ExcelConverter(
                    config=config,
                    category_info=category_info,
                )
                excel_ingester(objects)

            else:
                logger.info(
                    "Attempting to process metadata and convert to a single dataframe."
                )
                ingester = SinglePandasTableConverter(
                    config=config,
                    category_info=category_info,
                )
                ingester(objects)

    @classmethod
    def get_items(
        cls,
        api: ElabftwApi,
        category_ids: Optional[Iterable[int]],
    ) -> Iterator[Item]:
        """Iterates over categories and yields iterators over the items in those categories"""
        if category_ids is None:
            return api.items.iter()
        else:
            return chain(*(api.items.iter(cat=cat_id) for cat_id in category_ids))

    @classmethod
    def get_experiments(cls, api: ElabftwApi) -> Iterator[Experiment]:
        return api.experiments.iter()


class JSONConverter(NamedTuple):
    config: IngestConfiguration

    def __call__(
        self,
        objects: Iterable[Dictable],
    ) -> None:
        transformed = self.transform(objects)
        self.save(transformed)

    def transform(
        self,
        objects: Iterable[Dictable],
    ) -> Iterator[str]:
        indent = 2 if self.config.indent else None
        transformer = JSONTransformer(indent=indent)
        lines = transformer(objects)
        return lines

    def save(
        self,
        lines: Iterable[str],
    ) -> None:
        outputter = LineWriter.from_output(self.config.output)
        outputter(lines=lines)


class SinglePandasTableConverter(NamedTuple):
    config: IngestConfiguration
    category_info: CategoryInfo

    def __call__(
        self,
        objects: Iterable[Dictable],
    ) -> None:
        transformed = self.transform(objects)
        self.save(transformed)

    def transform(
        self,
        objects: Iterable[Dictable],
    ) -> pd.DataFrame:
        raw_table = PandasDataFrameTransformer()(objects)
        if self.config.table_shape == TableShapes.WIDE:
            logger.info("Deriving metadata schema for wide format")
            metadata_schema = self.get_category_metadata_schema()
            transformer = PandasDataFrameMetadataTransformer.for_wide_table(
                table_type=self.config.table_type,
                object_type=self.config.object_type,
                cell_content=self.config.cell_content,
                metadata_schema=metadata_schema,
                sanitize_column_names=self.config.sanitize_column_names,
                order_columns=True,
            )
        else:
            transformer = PandasDataFrameMetadataTransformer.for_long_table(
                table_type=self.config.table_type,
                object_type=self.config.object_type,
            )
        return transformer(raw_table)

    def save(
        self,
        df: pd.DataFrame,
    ) -> None:
        single_writer = PandasWriter.new(
            self.config.output,
            format=self.config.format,
        )
        single_writer(df)

    def get_category_metadata_schema(
        self,
    ) -> Optional[dict[str, str]]:
        if self.config.table_type == TableTypes.OBJECT:
            logger.debug("No metadata schema required since we don't require metadata.")
            return None
        if self.config.metadata_columns == MetadataColumns.ALL:
            logger.debug("No metadata schema required since we want all columns.")
            return None
        elif self.config.metadata_columns == MetadataColumns.TEMPLATE:
            category_ids = self.category_info.get_ids(self.config.categories)
            logger.debug("We need category metadata based on ids: %s" % category_ids)
            if len(category_ids) == 1:
                schema = self.category_info.get_metadata_schema(
                    cell_content=self.config.cell_content,
                    category_id=category_ids[0],
                )
                logger.debug("Metadata schema:\n%s" % json.dumps(schema, indent=2))
                return schema
            else:
                raise ValueError(
                    "Can not derive a single metadata schema with no or multiple template ids provided."
                )
        else:
            raise ValueError(
                f"Unrecognized option for metadata columns: '{self.config.metadata_columns}'"
            )


class ExcelConverter(NamedTuple):
    config: IngestConfiguration
    category_info: CategoryInfo

    def __call__(
        self,
        objects: Iterable[Dictable],
    ) -> None:
        split_tables = self.transform(objects)
        self.save(split_tables)

    def transform(
        self,
        objects: Iterable[Dictable],
    ) -> Iterator[SplitDataFrame]:
        raw_table = PandasDataFrameTransformer()(objects)
        if self.config.table_shape == TableShapes.WIDE:
            categories_metadata_schema = self.get_categories_metadata_schemas()
            multi_tables = MultiPandasDataFrameTransformer.for_wide_tables(
                table_type=self.config.table_type,
                object_type=self.config.object_type,
                categories_metadata_schema=categories_metadata_schema,
                cell_content=self.config.cell_content,
                sanitize_column_names=self.config.sanitize_column_names,
                order_columns=True,
            )
        else:
            multi_tables = MultiPandasDataFrameTransformer.for_long_tables(
                table_type=self.config.table_type,
                object_type=self.config.object_type,
                categories=self.config.categories,
            )
        return multi_tables(raw_table)

    def save(
        self,
        dataframes: Iterable[SplitDataFrame],
    ) -> None:
        writer = ExcelWriter.from_output(self.config.output)
        writer(dataframes)

    def get_categories_metadata_schemas(
        self,
    ) -> Optional[dict[str, Optional[dict[str, str]]]]:
        if self.config.table_type == TableTypes.OBJECT:
            logger.debug("No metadata schema required since we don't require metadata.")
            return None
        if self.config.metadata_columns == MetadataColumns.ALL:
            logger.debug("No metadata schema required since we want all metadata.")
            if self.config.categories:
                return {category: None for category in self.config.categories}
            else:
                return None
        elif self.config.metadata_columns == MetadataColumns.TEMPLATE:
            logger.debug("We only want template metadata columns, not all.")
            category_ids = self.category_info.get_ids(self.config.categories)
            return self.category_info.get_metadata_schemas_by_name(
                cell_content=self.config.cell_content,
                category_ids=category_ids,
            )
        else:
            raise ValueError(
                f"Unrecognized option for metadata columns: '{self.config.metadata_columns}'"
            )


class CategoryInfo:
    """Retrieves and stores information about categories/templates"""

    def __init__(
        self,
        getter: Callable[[], Iterable[Category]],
    ) -> None:
        self.getter = getter

    @cached_property
    def categories_map(self) -> dict[int, Category]:
        return self.fetch_categories_map(self.getter)

    @cached_property
    def ids(self) -> list[int]:
        return list(self.categories_map.keys())

    def get_ids(self, categories: Optional[Iterable[str]]) -> list[int]:
        if not categories:
            return self.ids
        else:
            ids = []
            for category in categories:
                if category not in self.name_to_ids:
                    raise KeyError(
                        f"Category '{category}' does not exist. "
                        f"Valid options: {list(self.name_to_ids.keys())}"
                    )
                ids.extend(self.name_to_ids[category])
            return ids

    @cached_property
    def name_to_ids(self) -> dict[str, list[int]]:
        name_to_ids: dict[str, list[int]] = {}
        for category_id, category in self.categories_map.items():
            name = category.title
            if name not in name_to_ids:
                name_to_ids[name] = list()
            else:
                logger.warning(
                    "The categegory name '%s' appears in multiple category ids" % name
                )
            name_to_ids[name].append(category_id)

        return name_to_ids

    @cached_property
    def id_to_name(self) -> dict[int, str]:
        return {
            category_id: category.title
            for category_id, category in self.categories_map.items()
        }

    def get_metadata_schemas_by_name(
        self,
        cell_content: TableCellContentType,
        category_ids: Optional[Collection[int]] = None,
    ) -> dict[str, Optional[dict[str, str]]]:
        """Get the metadata schemas from multiple categories."""
        if category_ids is None:
            category_ids = self.categories_map.keys()

        categories_metadata_schema: dict[str, Optional[dict[str, Any]]] = {}
        for category_id in category_ids:
            category = self.categories_map[category_id]
            if category.title in categories_metadata_schema:
                raise RuntimeError(
                    f"The category name '{category.title}' is duplicated, "
                    "can not create a mapping based on category names."
                )

            schema = self.get_metadata_schema(
                cell_content=cell_content,
                category_id=category_id,
            )
            categories_metadata_schema[category.title] = schema
        return categories_metadata_schema

    def get_metadata_schemas(
        self,
        cell_content: TableCellContentType,
        category_ids: Optional[Collection[int]] = None,
    ) -> dict[int, Optional[dict[str, str]]]:
        """Get the metadata schemas from multiple categories."""
        if category_ids is None:
            category_ids = self.categories_map.keys()

        categories_metadata_schema: dict[int, Optional[dict[str, Any]]] = {}
        for category_id in category_ids:
            schema = self.get_metadata_schema(
                cell_content=cell_content,
                category_id=category_id,
            )
            categories_metadata_schema[category_id] = schema
        return categories_metadata_schema

    def get_metadata_schema(
        self,
        cell_content: TableCellContentType,
        category_id: int,
    ) -> Optional[dict[str, Any]]:
        """Get the metadata schema from a known category id"""
        if category_id not in self.categories_map:
            raise KeyError(f"Category id {category_id} not found.")
        category = self.categories_map[category_id]
        schema = self.parse_schema_from_metadata(
            metadata=category.metadata,
            cell_content=cell_content,
        )
        return schema

    @classmethod
    def parse_schema_from_metadata(
        cls,
        metadata: Optional[str],
        cell_content: TableCellContentType,
    ) -> dict[str, str]:
        """Get the schema of the object metadata in wide table format from the
        category metadata string"""
        parsed_metadata = MetadataParser()(metadata)
        transformer = ParsedMetadataToPandasDtype.new(cell_content)
        return transformer(parsed_metadata)

    @classmethod
    def fetch_categories_map(
        cls,
        getter: Callable[[], Iterable[Category]],
    ) -> dict[int, Category]:
        """Get a mapping of category ids to category data"""
        logger.info("Fetching categories information...")
        category_map = {category.id: category for category in getter()}
        logger.info("Returned %s categories" % len(category_map))
        logger.debug("Category ids: %s" % list(category_map.keys()))
        logger.debug(
            "Category titles: %s"
            % [category.title for category in category_map.values()]
        )
        return category_map
