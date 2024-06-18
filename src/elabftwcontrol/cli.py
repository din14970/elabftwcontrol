import logging
import warnings
from enum import Enum
from pathlib import Path
from typing import Optional

import typer
from typing_extensions import Annotated

from elabftwcontrol.client import ElabftwApi
from elabftwcontrol.configure import (
    create_or_append_configuration_file,
    delete_configuration_file,
    list_config_profiles,
)
from elabftwcontrol.defaults import DEFAULT_CONFIG_FILE, logger
from elabftwcontrol.download import (
    IngestConfiguration,
    IngestJob,
    MetadataColumns,
    ObjectTypes,
    OutputFormats,
    TableCellContentType,
    TableShapes,
    TableTypes,
)

CMD = "elabftwctl"

app = typer.Typer(
    name=CMD,
    no_args_is_help=True,
    help="CLI utility and library for interacting with eLabFTW",
)


class ConfigActions(str, Enum):
    set = "set"
    list = "list"
    delete = "delete"


@app.command(
    help="Interact with the configuration for connecting to eLabFTW",
    no_args_is_help=True,
)
def config(
    action: Annotated[
        ConfigActions,
        typer.Argument(help="Action to perform on the config."),
    ],
    filepath: Annotated[
        Optional[Path],
        typer.Option(
            help="Path where config details exist or will be stored.",
        ),
    ] = None,
    profile: Annotated[
        Optional[str],
        typer.Option(
            help="Name of the profile. All by default or 'default' on set.",
        ),
    ] = None,
    show_keys: Annotated[
        bool,
        typer.Option(
            "--show_keys",
            help="Reveal the API keys in the terminal.",
        ),
    ] = False,
) -> None:
    if filepath is None:
        filepath = DEFAULT_CONFIG_FILE
    match action.value:
        case ConfigActions.set:
            create_or_append_configuration_file(
                filepath=filepath,
                profile=profile,
                show_keys=show_keys,
            )
        case ConfigActions.list:
            list_config_profiles(
                filepath=filepath,
                profile=profile,
                show_keys=show_keys,
            )
        case ConfigActions.delete:
            delete_configuration_file(
                filepath=filepath,
                profile=profile,
                show_keys=show_keys,
            )
        case _:
            print("Action not recognized")
            raise typer.Abort()


def _get_api(profile: Optional[str]) -> ElabftwApi:
    if profile:
        try:
            api = ElabftwApi.from_config_file(profile=profile)
        except Exception:
            raise ValueError(
                f"No profile with name {profile} was found. "
                "Please configure it using 'elabftwctl config set'."
            )
    else:
        try:
            api = ElabftwApi.new()
        except ValueError:
            try:
                api = ElabftwApi.from_config_file(profile="default")
            except Exception:
                raise ValueError(
                    "No default profile was found. "
                    "Please configure it using 'elabftwctl config set'."
                )
    return api


def _set_logger_verbosity(verbosity: int) -> None:
    if verbosity == 0:
        logger.setLevel(logging.WARN)
    elif verbosity == 1:
        logger.setLevel(logging.INFO)
    else:
        logger.setLevel(logging.DEBUG)


@app.command(
    help="Download or ingest data from eLabFTW in various formats",
    no_args_is_help=True,
)
def get(
    object_type: Annotated[
        ObjectTypes,
        typer.Argument(help="Which type of item to ingest."),
    ],
    format: Annotated[
        OutputFormats,
        typer.Option(
            help="Data format in which the output is represented.",
        ),
    ] = OutputFormats.JSON,
    output: Annotated[
        Optional[str],
        typer.Option(
            help=(
                "Path where the results should be directed. Can be a path on the filesystem "
                "or a location on AWS S3 if prefixed by s3://. If not provided defaults to stdout."
            ),
        ),
    ] = None,
    category: Annotated[
        Optional[list[str]],
        typer.Option(
            help="Limit the output to these categories or templates. Provide titles as input.",
        ),
    ] = None,
    indent: Annotated[
        bool,
        typer.Option(
            help="Whether to indent JSON results.",
        ),
    ] = False,
    table_shape: Annotated[
        TableShapes,
        typer.Option(
            help=(
                "In tabular formats, whether or not to expand the metadata fields "
                "into columns or stack the information in rows."
            ),
        ),
    ] = TableShapes.WIDE,
    table_type: Annotated[
        TableTypes,
        typer.Option(
            help=(
                "In tabular formats, what information/columns to include in the table."
            ),
        ),
    ] = TableTypes.COMBINED,
    metadata_columns: Annotated[
        MetadataColumns,
        typer.Option(
            help=(
                "In wide tabular formats, whether to include all fields as columns or only "
                "the fields from the corresponding templates."
            ),
        ),
    ] = MetadataColumns.ALL,
    cell_content: Annotated[
        TableCellContentType,
        typer.Option(
            help=(
                "In wide tabular formats, what field information to use to populate the "
                "metadata columns."
            ),
        ),
    ] = TableCellContentType.VALUE,
    sanitize_column_names: Annotated[
        bool,
        typer.Option(
            help="Convert column names to a limited character set.",
        ),
    ] = False,
    glue_table: Annotated[
        Optional[str],
        typer.Option(
            help=(
                "If you write single table data to S3, you can register it in glue. "
                "Use the format <DB NAME>:<TABLE NAME>."
            ),
        ),
    ] = None,
    profile: Annotated[
        Optional[str],
        typer.Option(
            help=(
                f"Name of the eLabFTW connection profile to use, see `{CMD} config`. "
                "If not set, 'default' is used."
            ),
        ),
    ] = None,
    verbosity: Annotated[
        int,
        typer.Option(
            "--verbose",
            "-v",
            count=True,
            help="Increase verbosity level.",
        ),
    ] = 0,
) -> None:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        api = _get_api(profile)
        _set_logger_verbosity(verbosity)

        try:
            config = IngestConfiguration(
                object_type=object_type,
                format=format,
                output=output,
                categories=category,
                indent=indent,
                table_shape=table_shape,
                table_type=table_type,
                metadata_columns=metadata_columns,
                cell_content=cell_content,
                sanitize_column_names=sanitize_column_names,
                glue_table=glue_table,
            )
            logger.info("Inputs:\n %s" % config.model_dump_json(indent=2))
            IngestJob(api=api, config=config)()
        except Exception as e:
            logger.critical("%s" % e)


@app.command(
    help="Apply state defined in YAML to eLabFTW",
    no_args_is_help=True,
)
def apply(
    action: Annotated[
        Path,
        typer.Argument(help="Folder in which the metadata.yaml file lives"),
    ],
) -> None:
    pass


@app.command(
    help="Remove state defined in YAML to eLabFTW",
    no_args_is_help=True,
)
def destroy(
    action: Annotated[
        Path,
        typer.Argument(help="Folder in which the metadata.yaml file lives"),
    ],
) -> None:
    pass


def main() -> None:
    app()


if __name__ == "__main__":
    main()
