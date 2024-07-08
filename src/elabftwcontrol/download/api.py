from __future__ import annotations

import json
from functools import cached_property
from itertools import chain
from typing import (
    Any,
    Callable,
    Collection,
    Iterable,
    Iterator,
    NamedTuple,
    Optional,
    Sequence,
)

import pandas as pd
from pydantic import BaseModel, model_validator

from elabftwcontrol._logging import logger
from elabftwcontrol.client import ElabftwApi, Experiment, Item
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
)
from elabftwcontrol.global_config import GlobalConfig


def get_item_types_as_df(
    client: Optional[ElabftwApi] = None,
    ids: Optional[int | Sequence[int]] = None,
) -> pd.DataFrame:
    """Get a table representation of the item types / categories

    Parameters
    ----------
    client
        A custom ElabftwApi object.
    ids
        Limit the returned records to these ids. By default all are included.

    Returns
    -------
    table
        The items type data in pandas table format. Fields are mapped to columns.
    """
    if isinstance(ids, int):
        ids = [ids]

    client = _client_or_default(client)
    item_types = client.items_types.iter_full(category_ids=ids)
    return _convert_to_table(
        objects=item_types,
        obj_type=ObjectTypes.ITEMS_TYPE,
        expand_metadata=False,
    )


def get_experiment_templates_as_df(
    client: Optional[ElabftwApi] = None,
    ids: Optional[int | Sequence[int]] = None,
) -> pd.DataFrame:
    """Get a table representation of the experiment templates

    Parameters
    ----------
    client
        A custom ElabftwApi object.
    ids
        Limit the returned records to these ids. By default all are included.

    Returns
    -------
    table
        The items type data in pandas table format. Fields are mapped to columns.
    """
    if isinstance(ids, int):
        ids = [ids]

    client = _client_or_default(client)
    experiment_templates = client.experiments_templates.iter(ids=ids)
    return _convert_to_table(
        objects=experiment_templates,
        obj_type=ObjectTypes.EXPERIMENTS_TEMPLATE,
        expand_metadata=False,
    )


def get_experiments_as_df(
    client: Optional[ElabftwApi] = None,
    ids: Optional[int | Collection[int]] = None,
    template_names: Optional[str | Sequence[str]] = None,
    expand_metadata: bool = False,
    table_shape: TableShapes = TableShapes.WIDE,
    cell_content: TableCellContentType = TableCellContentType.VALUE,
    sanitize_column_names: bool = False,
) -> pd.DataFrame:
    """Get a table representation of the experiments

    Parameters
    ----------
    client
        A custom ElabftwApi object.
    ids
        Limit the returned records to these ids. By default all are included.
        Order may not be conserved.
    template_names
        Experiments derived from these templates should be included. By default
        all experiments are included.
    expand_metadata
        Processes the table to expand the metadata information to columns and
        rows. This also makes a selection of the original columns and prefixes them
        with an underscore.
    table_shape
        Only relevant if `expand_metadata=True`. Options are "wide" and "long".
        "long" stacks all metadata information into separate rows, meaning that each
        experiment with n metadata fields is expanded to n rows. All field values stay
        represented as strings. "wide" creates new columns for metadata information
        and conserves one row per experiment. Data types are automatically inferred
        from the field type.
    cell_content
        Only relevant if "expand_metadata=True` and `table_shape="wide"`. By default
        only the field values are stored in the metadata field columns. This can become
        ambiguous if the field also had units. Setting this argument to `"combined"` will
        retain both value and unit as a string. It is also possible to select only `"unit`.
    sanitize_column_names
        Only relevant if "expand_metadata=True` and `table_shape="wide"`. The metadata
        column names correspond to field names. Setting this option to `True` will
        convert those names to a limited character set.

    Returns
    -------
    table
        The experiment data in pandas table format. Fields are mapped to columns.

    Notes
    -----
    * the `template_names` parameter works by substring search. It is assumed that
      the title of the template is still part of the experiment title. If this is
      not the case, there is no reliable way to determine which template an experiment
      originated from.
    """
    client = _client_or_default(client)

    if isinstance(template_names, str):
        template_names = [template_names]

    if isinstance(ids, int):
        ids = [ids]

    experiments = IngestJob.get_experiments(
        client,
        ids=ids,
        template_names=template_names,
    )
    return _convert_to_table(
        objects=experiments,
        obj_type=ObjectTypes.EXPERIMENT,
        expand_metadata=expand_metadata,
        table_shape=table_shape,
        cell_content=cell_content,
        sanitize_column_names=sanitize_column_names,
    )


def get_items_as_df(
    client: Optional[ElabftwApi] = None,
    ids: Optional[int | Collection[int]] = None,
    category_ids: Optional[int | Sequence[int]] = None,
    expand_metadata: bool = False,
    table_shape: TableShapes = TableShapes.WIDE,
    cell_content: TableCellContentType = TableCellContentType.VALUE,
    sanitize_column_names: bool = False,
) -> pd.DataFrame:
    """Get a table representation of the items

    Parameters
    ----------
    client
        A custom ElabftwApi object.
    ids
        Limit the returned records to these ids. By default all are included.
        Order may not be conserved.
    category_ids
        Items derived from only these categories should be included. By default
        all items are included.
    expand_metadata
        Processes the table to expand the metadata information to columns and
        rows. This also makes a selection of the original columns and prefixes them
        with an underscore.
    table_shape
        Only relevant if `expand_metadata=True`. Options are "wide" and "long".
        "long" stacks all metadata information into separate rows, meaning that each
        item with n metadata fields is expanded to n rows. All field values stay
        represented as strings. "wide" creates new columns for metadata information
        and conserves one row per item. Data types are automatically inferred
        from the field type.
    cell_content
        Only relevant if "expand_metadata=True` and `table_shape="wide"`. By default
        only the field values are stored in the metadata field columns. This can become
        ambiguous if the field also had units. Setting this argument to `"combined"` will
        retain both value and unit as a string. It is also possible to select only `"unit`.
    sanitize_column_names
        Only relevant if "expand_metadata=True` and `table_shape="wide"`. The metadata
        column names correspond to field names. Setting this option to `True` will
        convert those names to a limited character set.

    Returns
    -------
    table
        The experiment data in pandas table format. Fields are mapped to columns.
    """
    client = _client_or_default(client)

    if isinstance(category_ids, int):
        category_ids = [category_ids]

    if isinstance(ids, int):
        ids = [ids]

    items = IngestJob.get_items(
        client,
        ids=ids,
        category_ids=category_ids,
    )
    return _convert_to_table(
        objects=items,
        obj_type=ObjectTypes.ITEM,
        expand_metadata=expand_metadata,
        table_shape=table_shape,
        cell_content=cell_content,
        sanitize_column_names=sanitize_column_names,
    )


def _client_or_default(client: Optional[ElabftwApi]) -> ElabftwApi:
    if client is None:
        client = GlobalConfig.get_client()
    return client


def _convert_to_table(
    objects: Iterable[Dictable],
    obj_type: ObjectTypes,
    expand_metadata: bool = True,
    table_shape: TableShapes = TableShapes.WIDE,
    cell_content: TableCellContentType = TableCellContentType.VALUE,
    sanitize_column_names: bool = False,
) -> pd.DataFrame:
    raw_table = PandasDataFrameTransformer()(objects)
    if expand_metadata:
        if table_shape == TableShapes.WIDE:
            transformer = PandasDataFrameMetadataTransformer.for_wide_table(
                object_type=obj_type,
                cell_content=cell_content,
                metadata_schema=None,
                sanitize_column_names=sanitize_column_names,
                order_columns=True,
            )
        else:
            transformer = PandasDataFrameMetadataTransformer.for_long_table(
                object_type=obj_type,
            )

        return transformer(raw_table)
    else:
        return raw_table


class IngestConfiguration(BaseModel):
    """Parameters for downloading and storing data from eLabFTW

    Parameters
    ----------
    object_type
        Type of object to get from the server.
        Options: `"item"`, `"experiment"`, `"items_type"`, `"experiments_template"`
    ids
        Limit the output to records with these ids. Order may not be conserved.
    format
        Output formats. Available are `"json"`, `"csv"`, `"parquet"`, and `"excel"`.
        In the case of `"excel"`, records are split into different sheets by category.
    output
        Path to a file where the data should be stored. Defaults to stdout. Typically a path
        to a local file. If you have the AWS dependencies installed and you select
        csv or parquet, you can write directly to s3 using a path prefixed by
        s3://<bucket>/<prefix>.
    categories
        The items types or templates of the items or experiments to download. By default all
        objects are downloaded. For single table formats (csv, parquet) in wide format, it is
        recommended to select individual categories. For experiments, the template title
        must appear in the template of the experiment.
    indent
        Only applied when `format="json"`. Whether to indent json data that is returned.
    expand_metadata
        Only relevant to tabular output formats. If `True`, the data in the metadata column
        will be expanded to multiple columns and/or rows.
    table_shape
        Only relevant to tabular output formats. Defines whether metadata information is
        expanded to individual columns (wide) or stacked in rows (long).
    cell_content
        For wide tabular formats, the information that is put in columns corresponding to
        metadata fields. By default only the field value is conserved in the columns.
        This may be ambiguous for fields containing units. If `combined` is selected then
        field and unit are combined as a string. It is also possible to select only the unit.
    use_template_metadata_schema
        Only relevant to wide tabular formats. If true, the metadata columns are forced
        correspond to the fields of the item category or experiment template.
    sanitize_column_names
        Only relevant to wide tabular formats. Whether to keep the field names as column
        names or sanitize them to a simple character set that is compliant with column name
        rules in AWS Glue tables.
    glue_table
        Only relevant to tabular formats. If you write data directly to S3 you can register
        the table as a glue table. Input must be in the format: `<glue db>:<glue table>`.
    """

    object_type: ObjectTypes = ObjectTypes.ITEM
    ids: list[int] = []
    format: OutputFormats = OutputFormats.JSON
    output: Optional[str] = None
    categories: list[str] = []
    indent: bool = False
    expand_metadata: bool = False
    table_shape: TableShapes = TableShapes.WIDE
    use_template_metadata_schema: bool = False
    cell_content: TableCellContentType = TableCellContentType.VALUE
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
            and self.expand_metadata
            and self.use_template_metadata_schema
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
        experiments = cls.get_experiments(
            api,
            ids=config.ids,
            template_names=config.categories,
        )

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
            templates_info = CategoryInfo(
                getter=lambda: api.experiments_templates.iter(ids=config.ids),
            )
            template_ids = templates_info.get_ids(categories=config.categories)
            templates = [
                templates_info.categories_map[template_id]
                for template_id in template_ids
            ]
        else:
            templates = list(api.experiments_templates.iter(ids=config.ids))

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

        items = cls.get_items(
            api,
            ids=config.ids,
            category_ids=included_item_ids,
        )

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

        items_types_info = CategoryInfo(lambda: api.items_types.iter(ids=config.ids))
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
        ids: Optional[Collection[int]] = None,
        category_ids: Optional[Iterable[int]] = None,
    ) -> Iterator[Item]:
        """Iterates over categories and yields iterators over the items in those categories"""
        if category_ids is None:
            return api.items.iter(ids=ids)
        else:
            return chain(
                *(api.items.iter(ids=ids, cat=cat_id) for cat_id in category_ids)
            )

    @classmethod
    def get_experiments(
        cls,
        api: ElabftwApi,
        ids: Optional[Collection[int]] = None,
        template_names: Optional[Sequence[str]] = None,
    ) -> Iterator[Experiment]:
        experiments = api.experiments.iter(ids=ids)
        if not template_names:
            for experiment in experiments:
                yield experiment
        else:
            for experiment in experiments:
                for template_name in template_names:
                    if template_name in experiment.title:
                        yield experiment
                        break


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
        if not self.config.expand_metadata:
            return raw_table

        if self.config.table_shape == TableShapes.WIDE:
            logger.info("Deriving metadata schema for wide format")
            metadata_schema = self.get_category_metadata_schema()
            transformer = PandasDataFrameMetadataTransformer.for_wide_table(
                object_type=self.config.object_type,
                cell_content=self.config.cell_content,
                metadata_schema=metadata_schema,
                sanitize_column_names=self.config.sanitize_column_names,
                order_columns=True,
            )
        else:
            transformer = PandasDataFrameMetadataTransformer.for_long_table(
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
        if self.config.use_template_metadata_schema:
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
            logger.debug("No metadata schema required since we want all columns.")
            return None


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
        if not self.config.expand_metadata:
            multi_tables = MultiPandasDataFrameTransformer.for_raw_tables(
                object_type=self.config.object_type,
                categories=self.config.categories,
            )
        else:
            if self.config.table_shape == TableShapes.WIDE:
                categories_metadata_schema = self.get_categories_metadata_schemas()
                multi_tables = MultiPandasDataFrameTransformer.for_wide_tables(
                    object_type=self.config.object_type,
                    categories_metadata_schema=categories_metadata_schema,
                    cell_content=self.config.cell_content,
                    sanitize_column_names=self.config.sanitize_column_names,
                    order_columns=True,
                )
            else:
                multi_tables = MultiPandasDataFrameTransformer.for_long_tables(
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
        if self.config.use_template_metadata_schema:
            logger.debug("We only want template metadata columns, not all.")
            category_ids = self.category_info.get_ids(self.config.categories)
            return self.category_info.get_metadata_schemas_by_name(
                cell_content=self.config.cell_content,
                category_ids=category_ids,
            )
        else:
            logger.debug("No metadata schema required since we want all metadata.")
            if self.config.categories:
                return {category: None for category in self.config.categories}
            else:
                return None


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
