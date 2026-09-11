from __future__ import annotations

import json
from copy import deepcopy
from dataclasses import dataclass
from typing import Annotated, Any

from pydantic import ConfigDict, Field, PositiveInt, StringConstraints, TypeAdapter, ValidationError


SeasonId = Annotated[str, Field(pattern=r"^S[1-9][0-9]*$", max_length=20)]
AIModelName = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=200)]
TimeLocationName = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=100)]
TimeZoneName = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=100)]
TimeLocations = Annotated[dict[TimeLocationName, TimeZoneName], Field(max_length=50)]


@dataclass(frozen=True)
class SeasonConfig:
    current: str
    previous: str


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
    "cs_season_id": RuntimeConfigDefinition(
        key="cs_season_id",
        name="当前赛季",
        description='当前完美赛季编号，例如 JSON 字符串 "S21"。保存后下一次查询或抓取时生效。',
        annotation=SeasonId,
        default="S21",
        value_type="string",
    ),
    "cs_last_season_id": RuntimeConfigDefinition(
        key="cs_last_season_id",
        name="上一个赛季",
        description='上一个完美赛季编号，例如 JSON 字符串 "S20"。保存后下一次查询或抓取时生效。',
        annotation=SeasonId,
        default="S20",
        value_type="string",
    ),
    "hltv_event_id_list": RuntimeConfigDefinition(
        key="hltv_event_id_list",
        name="监视的比赛",
        description="需要定时检查结果更新的 5E 赛事 ID 列表。修改后下一次任务执行时生效。",
        annotation=list[PositiveInt],
        default=[],
        value_type="integer_list",
    ),
    "cs_time_locations": RuntimeConfigDefinition(
        key="cs_time_locations",
        name="时间地点与时区",
        description=(
            "“/时间”命令显示的地点与 IANA 时区映射，例如 "
            '{"中国（北京）":"Asia/Shanghai"}。保存后下一次命令生效。'
        ),
        annotation=TimeLocations,
        default={
            "中国（北京）": "Asia/Shanghai",
            "美国东部（纽约）": "America/New_York",
            "美国中部（芝加哥）": "America/Chicago",
            "美国西部（洛杉矶）": "America/Los_Angeles",
        },
        value_type="string_map",
    ),
    "cs_ai_model": RuntimeConfigDefinition(
        key="cs_ai_model",
        name="AI 模型",
        description="AI 对话、记忆整理和自动摘要使用的主模型。保存后下一次 AI 请求生效。",
        annotation=AIModelName,
        default="deepseek-v4-flash-vision-exp",
        value_type="string",
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
