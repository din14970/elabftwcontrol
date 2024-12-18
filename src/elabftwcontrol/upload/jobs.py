from typing import Callable, TypeVar, Generic
from enum import Enum

from elabftwcontrol.client import ElabftwApi


class JobType(Enum):
    CREATE = "create"
    UPDATE = "update"
    DELETE = "delete"


class ElabObjectType(Enum):
    ITEM = "item"
    ITEMS_TYPE = "items_type"
    EXPERIMENT = "experiment"
    EXPERIMENTS_TEMPLATE = "experiments_template"


T = TypeVar("T")


class Job(Generic[T]):
    def __init__(
        self,
        job_type: JobType,
        obj_type: ElabObjectType,
        action: Callable[[], T],
    ) -> None:
        self.job_type = job_type
        self.obj_type = obj_type
        self.action = action

    def __call__(self) -> T:
        return self.action()
