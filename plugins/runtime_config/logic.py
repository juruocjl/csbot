from __future__ import annotations

import json
from copy import deepcopy
from dataclasses import dataclass
from datetime import date
import re
from typing import Annotated, Any

from pydantic import (
    AfterValidator,
    ConfigDict,
    Field,
    PositiveInt,
    StringConstraints,
    TypeAdapter,
    ValidationError,
)


SeasonId = Annotated[str, Field(pattern=r"^S[1-9][0-9]*$", max_length=20)]
AIModelName = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=200)]
TimeLocationName = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=100)]
TimeZoneName = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=100)]
TimeLocations = Annotated[dict[TimeLocationName, TimeZoneName], Field(max_length=50)]
LiveWatchId = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=200)]
LiveWatchList = Annotated[list[LiveWatchId], Field(max_length=200)]
FollowedTeamName = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=100)]
FollowedTeamList = Annotated[list[FollowedTeamName], Field(max_length=50)]

BIRTHDAY_QQ_PATTERN = re.compile(r"^\d{5,12}$")
BIRTHDAY_MMDD_PATTERN = re.compile(r"^(\d{1,2})-(\d{1,2})$")


def normalize_birthday_override(value: Any) -> dict[str, str]:
    """Normalize and validate a ``{"QQ号": "MM-DD" | "none"}`` mapping.

    Keys are trimmed QQ numbers; values are either the case-insensitive
    ``none`` sentinel or a real calendar month/day (leap day included).
    """
    if not isinstance(value, dict):
        raise ValueError("生日覆盖配置必须是 JSON 对象")
    if len(value) > 500:
        raise ValueError("生日覆盖配置最多 500 条")
    normalized: dict[str, str] = {}
    for raw_key, raw_value in value.items():
        if not isinstance(raw_key, str) or not isinstance(raw_value, str):
            raise ValueError("生日覆盖配置的键和值都必须是字符串")
        key = raw_key.strip()
        if not BIRTHDAY_QQ_PATTERN.fullmatch(key):
            raise ValueError(f"生日覆盖配置的 QQ 号非法: {raw_key!r}")
        item = raw_value.strip()
        if item.lower() == "none":
            normalized[key] = "none"
            continue
        match = BIRTHDAY_MMDD_PATTERN.fullmatch(item)
        if not match:
            raise ValueError(f"生日覆盖配置的日期必须是 MM-DD 或 none: {raw_value!r}")
        month, day = int(match.group(1)), int(match.group(2))
        try:
            date(2024, month, day)
        except ValueError as exc:
            raise ValueError(f"生日覆盖配置的日期非法: {raw_value!r}") from exc
        normalized[key] = f"{month:02d}-{day:02d}"
    return normalized


BirthdayOverrides = Annotated[
    dict[str, str], Field(max_length=500), AfterValidator(normalize_birthday_override)
]


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
        name="监视的赛事",
        description=(
            "自动维护的 5E 赛事 ID：默认队伍、所有用户关注队伍参赛或 Major/S 级赛事均会加入。"
            "手动订阅请设置 hltv_manual_event_ids；不要直接改本列表。"
        ),
        annotation=list[PositiveInt],
        default=[],
        value_type="integer_list",
    ),
    "hltv_manual_event_ids": RuntimeConfigDefinition(
        key="hltv_manual_event_ids", name="手动订阅的赛事",
        description="手动保留的 5E 赛事 ID；与自动发现结果合并，刷新不会覆盖。结束后停止抓取。",
        annotation=list[PositiveInt], default=[], value_type="integer_list",
    ),
    "hltv_followed_teams": RuntimeConfigDefinition(
        key="hltv_followed_teams",
        name="关注的战队",
        description=(
            "默认关注队伍名称或完整 5E 队伍 ID；忽略大小写、标点，使用明确别名解析，不使用包含匹配。"
            "这些队伍的结果推送到默认推送群；个人关注另行保存并驱动赛事发现。"
        ),
        annotation=FollowedTeamList,
        default=["TYLOO", "LVG", "FaZe", "BC.Game"],
        value_type="string_list",
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
        default="gpt-6.2-sol",
        value_type="string",
    ),
    "live_watch_list": RuntimeConfigDefinition(
        key="live_watch_list",
        name="直播监视列表",
        description=(
            "定时检查的直播间 ID 列表，当前支持 dy_房间号 和 bili_房间号。"
            "保存后下一轮监视任务生效；设为空数组可暂停监视。"
        ),
        annotation=LiveWatchList,
        default=[],
        value_type="string_list",
    ),
    "birthday_overrides": RuntimeConfigDefinition(
        key="birthday_overrides",
        name="生日覆盖配置",
        description=(
            '按 QQ 号覆盖群成员生日或关闭自动祝福，格式为 JSON 对象 {"QQ号":"MM-DD"} '
            '或 {"QQ号":"none"}。设为 "none" 表示不发送自动生日祝福，删除该键恢复读取 QQ 资料。'
            "保存后立即生效。"
        ),
        annotation=BirthdayOverrides,
        default={},
        value_type="string_map",
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
