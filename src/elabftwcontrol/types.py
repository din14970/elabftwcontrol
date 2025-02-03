from __future__ import annotations

from enum import Enum


class ElabObjectType(str, Enum):
    ITEM = "item"
    ITEMS_TYPE = "items_type"
    EXPERIMENT = "experiment"
    EXPERIMENTS_TEMPLATE = "experiments_template"


class EntityTypes(str, Enum):
    ITEM = "items"
    ITEMS_TYPE = "items_types"
    EXPERIMENT = "experiments"
    EXPERIMENTS_TEMPLATE = "experiments_templates"


class SingleObjectTypes(str, Enum):
    ITEM = EntityTypes.ITEM.value
    EXPERIMENT = EntityTypes.EXPERIMENT.value
