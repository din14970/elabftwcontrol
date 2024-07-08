from __future__ import annotations

import os
from functools import cached_property
from pathlib import Path
from typing import (
    Any,
    Callable,
    Collection,
    Dict,
    Generic,
    Iterable,
    Iterator,
    List,
    Optional,
    Sequence,
    Type,
    TypeVar,
    Union,
)

from elabapi_python import (
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
from elabapi_python.rest import ApiException

from elabftwcontrol._logging import logger
from elabftwcontrol.configure import AccessConfig, MultiConfig
from elabftwcontrol.defaults import DEFAULT_CONFIG_FILE
from elabftwcontrol.types import (
    ApiResponseObject,
    ElabObj,
    EntityRUD,
    EntityTypes,
    GroupEntityCreate,
    SingleEntityCreate,
    SingleObjectTypes,
)

Pathlike = Union[Path, str]

T = TypeVar("T", bound=ElabObj)

DEFAULT_REQUEST_BATCH_SIZE = 1000


class ObjectSyncer(Generic[T]):
    """Serves as an intermediary between the API client and models"""

    def __init__(self, api: ElabftwApi) -> None:
        self.api = api

    @classmethod
    def from_config_file(
        cls,
        filepath: Optional[Pathlike] = None,
        profile: Optional[str] = None,
    ) -> ObjectSyncer:
        if filepath is None:
            filepath = DEFAULT_CONFIG_FILE
        if profile is None:
            profile = "default"
        api = ElabftwApi.from_config_file(filepath=filepath, profile=profile)
        return cls(api)

    def get_api_endpoint(self, elab_obj: T | Type[T]) -> EntityRUD:
        """Get the API endpoint that matches the object"""
        return getattr(self.api, elab_obj.obj_type)

    def iter(
        self,
        elab_obj_cls: Type[T],
    ) -> Iterator[T]:
        """Iterate over remote objects"""
        api_endpoint = self.get_api_endpoint(elab_obj_cls)
        return (
            elab_obj_cls.from_api_data(label=str(obj.id), data=obj)
            for obj in api_endpoint.iter()
        )

    def exists(
        self,
        elab_obj: T,
    ) -> bool:
        """Verify whether a remote version of the object exists."""
        api_endpoint = self.get_api_endpoint(elab_obj)
        if elab_obj.id is None:
            logger.info(f"Object {elab_obj} has no id.")
            return False
        else:
            try:
                fetched = self._fetch(elab_obj, api_endpoint)
                return fetched.id == elab_obj.id
            except ApiException:
                return False
            except Exception as e:
                raise e

    def push(
        self,
        elab_obj: T,
        create_method: Callable[[], int],
    ) -> None:
        """Modify the data on an existing remote template and create if it doesn't exist"""
        api_endpoint = self.get_api_endpoint(elab_obj)
        if elab_obj.id is None:
            elab_obj.id = create_method()
        self._patch(elab_obj, api_endpoint)

    def patch_tags(
        self,
        elab_obj: T,
    ) -> None:
        tag_endpoint = self.api.tags
        tags = elab_obj.get("tags")
        if tags is not None:
            obj_id = self._verify_has_id(elab_obj)
            entity_type = elab_obj.obj_type
            logger.info(f"Patching tags on {elab_obj}.")
            tag_endpoint.delete(elab_obj.obj_type, entity_id=obj_id)
            for tag in tags:
                tag_endpoint.create(
                    entity_type,
                    entity_id=obj_id,
                    tag=tag,
                )

    def pull(
        self,
        elab_obj: T,
    ) -> None:
        """Fetch the latest data from the remote and update obj in place"""
        new_obj = self.fetch(elab_obj)
        elab_obj.data = new_obj.data

    def delete(
        self,
        elab_obj: T,
    ) -> None:
        """Delete this template on the remote"""
        api_endpoint = self.get_api_endpoint(elab_obj)
        obj_id = self._verify_has_id(elab_obj)
        api_endpoint.delete(obj_id)
        elab_obj.id = None

    def fetch(
        self,
        elab_obj: T,
    ) -> T:
        """Pull the matching data from the remote and return without overwriting"""
        api_endpoint = self.get_api_endpoint(elab_obj)
        data = self._fetch(elab_obj, api_endpoint)
        return elab_obj.from_api_data(label=str(data.id), data=data)

    @classmethod
    def _fetch(
        cls,
        elab_obj: T,
        api_endpoint: EntityRUD,
    ) -> ApiResponseObject:
        id = cls._verify_has_id(elab_obj)
        return api_endpoint.read(id)

    @classmethod
    def _patch(
        cls,
        elab_obj: T,
        api_endpoint: EntityRUD,
    ) -> None:
        obj_id = cls._verify_has_id(elab_obj)
        body = cls.prepare_patch_message_body(elab_obj)
        api_endpoint.patch(obj_id, body)

    @classmethod
    def prepare_patch_message_body(cls, obj: ElabObj) -> Dict[str, Any]:
        req_body = {}
        for field in obj.updatable_fields:
            value = obj.get(field)
            if value is not None:
                req_body[field] = str(value)
        return req_body

    @classmethod
    def _verify_has_id(cls, obj: ElabObj) -> int:
        if obj.id is None:
            raise ValueError(f"Object {obj} does not have an ID")
        return obj.id


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

        verify_ssl = verify_ssl or bool(int(os.getenv("ELABCTL_VERIFY_SSL", "1")))
        debug = debug or bool(int(os.getenv("ELABCTL_DEBUG", "0")))

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
            entity_type,
            entity_id,
            body={"tag": tag},
        )
        logger.info(f"Added tag '{tag}' to {entity_type} {entity_id}")

    def read(
        self,
        entity_type: EntityTypes,
        entity_id: int,
        tag_id: int,
    ) -> Tag:
        return self.api.read_tag(entity_type, entity_id, tag_id)

    def iter(
        self,
        entity_type: EntityTypes,
        entity_id: int,
    ) -> List[Tag]:
        return self.api.read_tags(entity_type, entity_id)

    def delete(
        self,
        entity_type: EntityTypes,
        entity_id: int,
    ) -> None:
        self.api.delete_tag(entity_type, entity_id)
        logger.info(f"Deleted all tags from {entity_type} {entity_id}")
