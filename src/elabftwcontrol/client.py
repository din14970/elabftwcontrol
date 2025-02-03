from __future__ import annotations

import os
from functools import cached_property
from pathlib import Path
from typing import (
    Any,
    Collection,
    Dict,
    Iterable,
    Iterator,
    List,
    Optional,
    Protocol,
    Sequence,
    TypeVar,
    Union,
)

from elabapi_python import (  # type: ignore
    ApiClient,
    Experiment,
    ExperimentsApi,
    ExperimentsTemplatesApi,
    ExperimentTemplate,
    InfoApi,
    Item,
    ItemsApi,
    ItemsType,
    ItemsTypesApi,
    Link,
    LinksToExperimentsApi,
    LinksToItemsApi,
    Tag,
    TagsApi,
)

from elabftwcontrol._logging import logger
from elabftwcontrol.configure import AccessConfig, MultiConfig
from elabftwcontrol.defaults import DEFAULT_CONFIG_FILE
from elabftwcontrol.types import EntityTypes, SingleObjectTypes

Pathlike = Union[Path, str]


DEFAULT_REQUEST_BATCH_SIZE = 1000


class ElabftwApi:
    def __init__(
        self,
        client: ApiClient,
        request_batch_size: int = DEFAULT_REQUEST_BATCH_SIZE,
    ) -> None:
        self.client = client
        self.request_batch_size = request_batch_size

    @property
    def host_name(self) -> str:
        return self.client.configuration.host

    @cached_property
    def info(self) -> InfoApi:
        return InfoApi(self.client)

    @cached_property
    def items(self) -> ItemCRUD:
        return ItemCRUD.from_client(self.client)

    @cached_property
    def experiments(self) -> ExperimentCRUD:
        return ExperimentCRUD.from_client(self.client)

    @cached_property
    def items_types(self) -> ItemsTypeCRUD:
        return ItemsTypeCRUD.from_client(self.client)

    @cached_property
    def experiments_templates(self) -> ExperimentTemplateCRUD:
        return ExperimentTemplateCRUD.from_client(self.client)

    @cached_property
    def tags(self) -> TagCRUD:
        return TagCRUD.from_client(self.client)

    @cached_property
    def links(self) -> LinkCRUD:
        return LinkCRUD.from_client(self.client)

    @classmethod
    def from_config_file(
        cls,
        filepath: Pathlike = DEFAULT_CONFIG_FILE,
        profile: str = "default",
    ) -> ElabftwApi:
        """Create an eLabFTW client from a config file and profile"""
        configuration = MultiConfig.from_file(filepath).get_profile(profile)
        return cls._create(configuration)

    @classmethod
    def new(
        cls,
        host_url: Optional[str] = None,
        api_key: Optional[str] = None,
        verify_ssl: Optional[bool] = None,
        debug: Optional[bool] = None,
    ) -> ElabftwApi:
        host_url = host_url or os.getenv("ELABCTL_HOST_URL")
        if not host_url:
            raise ValueError(
                "Host URL must be specified or defined in the ELABCTL_HOST_URL environment variable."
            )

        api_key = api_key or os.getenv("ELABCTL_API_KEY")
        if not api_key:
            raise ValueError(
                "API key must be specified or defined in the ELABCTL_API_KEY environment variable."
            )

        if verify_ssl is None:
            verify_ssl = bool(int(os.getenv("ELABCTL_VERIFY_SSL", "1")))

        if debug is None:
            debug = bool(int(os.getenv("ELABCTL_DEBUG", "0")))

        configuration = AccessConfig(
            host_url=host_url,
            api_key=api_key,
            verify_ssl=verify_ssl,
            debug=debug,
        )
        return cls._create(configuration)

    @classmethod
    def _create(
        cls,
        configuration: AccessConfig,
    ) -> ElabftwApi:
        api_client = ApiClient(configuration.get_api_config())
        api_client.set_default_header(
            header_name="Authorization",
            header_value=configuration.api_key,
        )
        return cls(client=api_client)


class ApiResponseObject(Protocol):
    id: int

    def to_dict(self) -> dict[str, Any]: ...


T = TypeVar("T", bound=ApiResponseObject, covariant=True)


class EntityRUD(Protocol[T]):
    def read(self, id: int) -> T: ...

    def iter(self) -> Iterable[T]: ...

    def patch(self, id: int, body: dict[str, Any]) -> None: ...

    def delete(self, id: int) -> None: ...


class GroupEntityCreate(Protocol):
    def create(self) -> int: ...


class SingleEntityCreate(Protocol):
    def create(self, category_id: int) -> int: ...


class ItemsTypeCRUD(EntityRUD, GroupEntityCreate):
    def __init__(
        self,
        api: ItemsTypesApi,
    ) -> None:
        self.api = api

    @classmethod
    def from_client(
        cls,
        api_client: ApiClient,
    ) -> ItemsTypeCRUD:
        return cls(api=ItemsTypesApi(api_client))

    def create(self) -> int:
        _, _, response = self.api.post_items_types_with_http_info()
        category_id = self._get_new_category_id(response)
        logger.info(f"Created new category: {category_id}")
        return category_id

    def get_by_name(self, name: str) -> ItemsType:
        category = None
        templates = self.api.read_items_types()
        for template in templates:
            if template.title == name:
                category = template
                break
        else:
            raise ValueError(f"Category '{name}' was not found!")
        return self.read(category.id)

    def search(self, q: str) -> List[ItemsType]:
        templates = self.api.read_items_types()

        if not q:
            return templates

        to_return: List[ItemsType] = []
        for template in templates:
            if template.title is not None and q in template.title:
                to_return.append(template)
            if template.body is not None and q in template.body:
                to_return.append(template)

        return to_return

    def read(self, id: int) -> ItemsType:
        return self.api.get_items_type(id)

    def iter(self, ids: Optional[Sequence[int]] = None) -> List[ItemsType]:
        # it is much more efficient to make one request than one per id
        all_types = self.api.read_items_types()
        if not ids:
            return all_types
        item_type_map = {item_type.id: item_type for item_type in all_types}
        return [item_type_map[id] for id in ids]

    def iter_full(
        self,
        category_ids: Optional[Iterable[int]] = None,
    ) -> List[ItemsType]:
        if not category_ids:
            templates = self.iter()
            category_ids = (template.id for template in templates)

        threads = []
        for category_id in category_ids:
            threads.append(self.api.get_items_type(category_id, async_req=True))
        return [thread.get() for thread in threads]

    def patch(
        self,
        id: int,
        body: Dict[str, Any],
    ) -> None:
        self.api.patch_items_type(id, body=body)
        logger.info(f"Patched category {id}")

    def delete(
        self,
        id: int,
    ) -> None:
        self.api.delete_items_type(id)
        logger.info(f"Deleted category {id}")

    def _get_new_category_id(self, response: Any) -> int:
        return int(response.get("Location").split("=")[-1])


class ExperimentTemplateCRUD(EntityRUD, GroupEntityCreate):
    def __init__(self, api: ExperimentsTemplatesApi) -> None:
        self.api = api

    @classmethod
    def from_client(cls, api_client: ApiClient) -> ExperimentTemplateCRUD:
        return cls(api=ExperimentsTemplatesApi(api_client))

    def create(self) -> int:
        _, _, response = self.api.post_experiment_template_with_http_info()
        template_id = self._get_new_template_id(response)
        logger.info(f"Created new template: {template_id}")
        return template_id

    def search(self, q: str) -> List[ExperimentTemplate]:
        templates = self.api.read_experiments_templates()

        if not q:
            return templates

        to_return: List[ExperimentTemplate] = []
        for template in templates:
            if template.title is not None and q in template.title:
                to_return.append(template)
            if template.body is not None and q in template.body:
                to_return.append(template)

        return to_return

    def read(self, id: int) -> ExperimentTemplate:
        return self.api.get_experiment_template(id)

    def iter(self, ids: Optional[Sequence[int]] = None) -> List[ExperimentTemplate]:
        # it is much more efficient to make one request than one per id
        all_templates = self.api.read_experiments_templates()
        if not ids:
            return all_templates
        template_map = {template.id: template for template in all_templates}
        return [template_map[id] for id in ids]

    def patch(self, id: int, body: Dict[str, Any]) -> None:
        self.api.patch_experiment_template(id, body=body)
        logger.info(f"Patched template {id}")

    def delete(self, id: int) -> None:
        self.api.delete_experiment_template(id)
        logger.info(f"Deleted template {id}")

    def _get_new_template_id(self, response: Any) -> int:
        return int(response.get("Location").split("/")[-1])


class ItemCRUD(EntityRUD, SingleEntityCreate):
    def __init__(
        self,
        api: ItemsApi,
        read_batch_size: int,
    ) -> None:
        self.api = api
        self.read_batch_size = read_batch_size

    @classmethod
    def from_client(
        cls,
        api_client: ApiClient,
        read_batch_size: int = DEFAULT_REQUEST_BATCH_SIZE,
    ) -> ItemCRUD:
        return cls(
            api=ItemsApi(api_client),
            read_batch_size=read_batch_size,
        )

    def create(self, category_id: int) -> int:
        _, _, response = self.api.post_item_with_http_info(
            body={"category_id": category_id}
        )
        new_item_id = self._get_new_item_id(response)
        logger.info(f"Created item {new_item_id}")
        return new_item_id

    def read(self, id: int) -> Item:
        return self.api.get_item(id)

    def search(self, q: str, **kwargs: Any) -> List[Item]:
        return self.api.read_items(q=q, **kwargs)

    def iter(
        self,
        ids: Optional[Collection[int]] = None,
        **kwargs: Any,
    ) -> Iterator[Item]:
        """Return items by making batched requests"""
        read_number_of_items = 0
        while True:
            items = self.api.read_items(
                limit=self.read_batch_size - 1,
                offset=read_number_of_items,
                **kwargs,
            )
            if not items:
                logger.debug("No more items found, exiting.")
                break

            for item in items:
                if ids and item.id not in ids:
                    continue

                yield item

            if len(items) < self.read_batch_size:
                logger.debug("Less items found than the batch size. Exiting.")
                break

            read_number_of_items += self.read_batch_size

    def patch(self, id: int, body: Dict[str, Any]) -> None:
        self.api.patch_item(id, body=body)
        logger.info(f"Patched item {id}")

    def delete(self, id: int) -> None:
        self.api.delete_item(id)
        logger.info(f"Deleted item {id}")

    def _get_new_item_id(self, response: Any) -> int:
        return int(response.get("Location").split("/")[-1])


class ExperimentCRUD(EntityRUD, SingleEntityCreate):
    def __init__(
        self,
        api: ExperimentsApi,
        read_batch_size: int,
    ) -> None:
        self.api = api
        self.read_batch_size = read_batch_size

    @classmethod
    def from_client(
        cls,
        api_client: ApiClient,
        read_batch_size: int = DEFAULT_REQUEST_BATCH_SIZE,
    ) -> ExperimentCRUD:
        return cls(
            api=ExperimentsApi(api_client),
            read_batch_size=read_batch_size,
        )

    def create(self, category_id: int = -1) -> int:
        _, _, response = self.api.post_experiment_with_http_info(
            body={"category_id": category_id}
        )
        new_experiment_id = self._get_new_experiment_id(response)
        logger.info(f"Created new experiment {new_experiment_id}")
        return new_experiment_id

    def read(self, id: int) -> Experiment:
        return self.api.get_experiment(id)

    def search(self, q: str, **kwargs: Any) -> List[Experiment]:
        return self.api.read_experiments(q=q, **kwargs)

    def iter(
        self,
        ids: Optional[Collection[int]] = None,
        **kwargs: Any,
    ) -> Iterator[Experiment]:
        """Return items by making batched requests"""
        read_number_of_experiments = 0
        while True:
            experiments = self.api.read_experiments(
                limit=self.read_batch_size - 1,
                offset=read_number_of_experiments,
                **kwargs,
            )
            if not experiments:
                logger.debug("No more experiments found, exiting.")
                break

            for experiment in experiments:
                if ids and experiment.id not in ids:
                    continue

                yield experiment

            if len(experiments) < self.read_batch_size:
                logger.debug("Less experiments found than the batch size. Exiting.")
                break

            read_number_of_experiments += self.read_batch_size

    def patch(self, id: int, body: Dict[str, Any]) -> None:
        self.api.patch_experiment(id, body=body)
        logger.info(f"Patched experiment {id}")

    def delete(self, id: int) -> None:
        self.api.delete_experiment(id)
        logger.info(f"Deleted experiment {id}")

    def _get_new_experiment_id(self, response: Any) -> int:
        return int(response.get("Location").split("/")[-1])


class LinkCRUD:
    def __init__(
        self,
        links_to_items_api: LinksToItemsApi,
        links_to_experiments_api: LinksToExperimentsApi,
    ) -> None:
        self.links_to_items_api = links_to_items_api
        self.links_to_experiments_api = links_to_experiments_api

    @classmethod
    def from_client(cls, api_client: ApiClient) -> LinkCRUD:
        return cls(
            links_to_items_api=LinksToItemsApi(api_client),
            links_to_experiments_api=LinksToExperimentsApi(api_client),
        )

    def create(
        self,
        source_type: SingleObjectTypes,
        source_id: int,
        destination_type: SingleObjectTypes,
        destination_id: int,
    ) -> None:
        self._verify_source_and_destination_types(source_type, destination_type)

        if destination_type == "items":
            method = self.links_to_items_api.post_entity_items_links
        else:
            method = self.links_to_experiments_api.post_entity_experiments_links

        method(
            entity_type=source_type,
            id=source_id,
            subid=destination_id,
            body={"action": "create"},
        )

    def exists(
        self,
        source_type: SingleObjectTypes,
        source_id: int,
        destination_type: SingleObjectTypes,
        destination_id: int,
    ) -> bool:
        self._verify_source_and_destination_types(source_type, destination_type)

        links = self.read(
            source_type=source_type,
            source_id=source_id,
            destination_type=destination_type,
        )
        for link in links:
            if link.itemid == destination_id:
                return True
        return False

    def read(
        self,
        source_type: SingleObjectTypes,
        source_id: int,
        destination_type: SingleObjectTypes,
    ) -> List[Link]:
        self._verify_source_and_destination_types(source_type, destination_type)

        if destination_type == "items":
            method = self.links_to_items_api.read_entity_items_links
        else:
            method = self.links_to_experiments_api.read_entity_experiments_links

        return method(
            entity_type=source_type,
            id=source_id,
        )

    def delete(
        self,
        source_type: SingleObjectTypes,
        source_id: int,
        destination_type: SingleObjectTypes,
        destination_id: int,
    ) -> None:
        self._verify_source_and_destination_types(source_type, destination_type)

        if destination_type == "items":
            # possible typo in eLabFTW API spec
            method = self.links_to_items_api.delete_entitiy_items_link
        else:
            method = self.links_to_experiments_api.delete_entity_experiments_link

        method(
            entity_type=source_type,
            id=source_id,
            subid=destination_id,
        )

    @classmethod
    def _verify_source_and_destination_types(
        cls,
        source_type: str,
        destination_type: str,
    ) -> None:
        assert source_type in ("items", "experiments")
        assert destination_type in ("items", "experiments")


class TagCRUD:
    def __init__(
        self,
        api: TagsApi,
    ) -> None:
        self.api = api

    @classmethod
    def from_client(cls, api_client: ApiClient) -> TagCRUD:
        return cls(api=TagsApi(api_client))

    def create(
        self,
        entity_type: EntityTypes,
        entity_id: int,
        tag: str,
    ) -> None:
        self.api.post_tag(
            entity_type.value,
            entity_id,
            body={"tag": tag},
        )
        logger.debug(f"Added tag '{tag}' to {entity_type} {entity_id}")

    def read(
        self,
        entity_type: EntityTypes,
        entity_id: int,
        tag_id: int,
    ) -> Tag:
        return self.api.read_tag(entity_type.value, entity_id, tag_id)

    def iter(
        self,
        entity_type: EntityTypes,
        entity_id: int,
    ) -> List[Tag]:
        return self.api.read_tags(entity_type.value, entity_id)

    def delete(
        self,
        entity_type: EntityTypes,
        entity_id: int,
        tag_id: int,
    ) -> None:
        self.api.patch_tag(
            entity_type.value,
            entity_id,
            tag_id,
            body={"action": "unreference"},
        )
        logger.debug(f"Removed tag {tag_id} from {entity_type.value} {entity_id}")

    def delete_all(
        self,
        entity_type: EntityTypes,
        entity_id: int,
    ) -> None:
        self.api.delete_tag(entity_type.value, entity_id)
        logger.debug(f"Deleted all tags from {entity_type.value} {entity_id}")
