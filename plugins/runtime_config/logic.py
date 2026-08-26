from __future__ import annotations

import json
from copy import deepcopy
from dataclasses import dataclass
from typing import Any

from pydantic import ConfigDict, PositiveInt, TypeAdapter, ValidationError


@dataclass(frozen=True)
class RuntimeConfigDefinition:
    key: str
    name: str
    description: str
    annotation: Any
    default: Any
    value_type: str
    editor: str = "json"

    def validate(self, value: Any) -> Any:
        adapter = TypeAdapter(self.annotation, config=ConfigDict(strict=True))
        return adapter.validate_python(value)

    def default_value(self) -> Any:
        return deepcopy(self.default)


DEFINITIONS: dict[str, RuntimeConfigDefinition] = {
    "hltv_event_id_list": RuntimeConfigDefinition(
        key="hltv_event_id_list",
        name="监视的比赛",
        description="需要定时检查结果更新的 5E 赛事 ID 列表。修改后下一次任务执行时生效。",
        annotation=list[PositiveInt],
        default=[],
        value_type="integer_list",
    ),
}


def get_definition(key: str) -> RuntimeConfigDefinition:
    try:
        return DEFINITIONS[key]
    except KeyError as exc:
        raise KeyError(f"未知的热配置项: {key}") from exc


def decode_value(definition: RuntimeConfigDefinition, raw_value: str) -> Any:
    try:
        value = json.loads(raw_value)
    except json.JSONDecodeError as exc:
        raise ValueError(f"配置项 {definition.key} 不是有效的 JSON") from exc
    try:
        return definition.validate(value)
    except ValidationError as exc:
        raise ValueError(f"配置项 {definition.key} 的值不符合 {definition.value_type} 类型") from exc


def encode_value(definition: RuntimeConfigDefinition, value: Any) -> tuple[Any, str]:
    try:
        validated = definition.validate(value)
    except ValidationError as exc:
        raise ValueError(f"配置项 {definition.key} 的值不符合 {definition.value_type} 类型") from exc
    return validated, json.dumps(validated, ensure_ascii=False, separators=(",", ":"))
