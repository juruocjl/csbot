from __future__ import annotations

import time
from copy import deepcopy
from dataclasses import dataclass
from typing import Any

from nonebot import get_driver, logger, require
from nonebot.plugin import PluginMetadata
from sqlalchemy import select

require("models")
require("utils")

from ..models import RuntimeConfig
from ..utils import async_session_factory
from .logic import DEFINITIONS, SeasonConfig, decode_value, encode_value, get_definition


__plugin_meta__ = PluginMetadata(
    name="runtime_config",
    description="从数据库读取、可在运行时修改的类型安全配置",
    usage="",
)


@dataclass(frozen=True)
class RuntimeConfigValue:
    key: str
    name: str
    description: str
    value_type: str
    editor: str
    value: Any
    default_value: Any
    updated_at: int | None
    updated_by: str | None


class RuntimeConfigManager:
    def __init__(self) -> None:
        self._default_overrides: dict[str, Any] = {}

    def register_default(self, key: str, value: Any) -> None:
        """Use an existing startup setting only when the database row is first created."""
        definition = get_definition(key)
        validated, _ = encode_value(definition, value)
        self._default_overrides[key] = deepcopy(validated)

    def _default_value(self, key: str) -> Any:
        if key in self._default_overrides:
            return deepcopy(self._default_overrides[key])
        return get_definition(key).default_value()

    async def get_seasons(self) -> SeasonConfig:
        """在同一次数据库查询中读取赛季对，供一次业务操作固定使用。"""
        keys = ("cs_season_id", "cs_last_season_id")
        async with async_session_factory() as session:
            result = await session.execute(select(RuntimeConfig).where(RuntimeConfig.key.in_(keys)))
            stored = {item.key: item.value for item in result.scalars().all()}
        values = {
            key: decode_value(get_definition(key), stored[key])
            if key in stored else self._default_value(key)
            for key in keys
        }
        return SeasonConfig(current=values[keys[0]], previous=values[keys[1]])

    async def ensure_defaults(self) -> None:
        async with async_session_factory() as session:
            async with session.begin():
                for definition in DEFINITIONS.values():
                    item = await session.get(RuntimeConfig, definition.key)
                    if item is not None:
                        continue
                    _, encoded = encode_value(definition, self._default_value(definition.key))
                    session.add(
                        RuntimeConfig(
                            key=definition.key,
                            value=encoded,
                            updated_at=0,
                            updated_by=None,
                        )
                    )

    async def get(self, key: str) -> Any:
        definition = get_definition(key)
        async with async_session_factory() as session:
            item = await session.get(RuntimeConfig, key)
        if item is None:
            logger.warning(f"热配置项 {key} 尚未写入数据库，使用注册默认值")
            return self._default_value(key)
        return decode_value(definition, item.value)

    async def list_values(self) -> list[RuntimeConfigValue]:
        async with async_session_factory() as session:
            result = await session.execute(
                select(RuntimeConfig).where(RuntimeConfig.key.in_(DEFINITIONS))
            )
            stored = {item.key: item for item in result.scalars().all()}

        values: list[RuntimeConfigValue] = []
        for definition in DEFINITIONS.values():
            item = stored.get(definition.key)
            values.append(
                RuntimeConfigValue(
                    key=definition.key,
                    name=definition.name,
                    description=definition.description,
                    value_type=definition.value_type,
                    editor=definition.editor,
                    value=(
                        decode_value(definition, item.value)
                        if item is not None
                        else self._default_value(definition.key)
                    ),
                    default_value=self._default_value(definition.key),
                    updated_at=item.updated_at if item is not None and item.updated_at else None,
                    updated_by=item.updated_by if item is not None else None,
                )
            )
        return values

    async def set(self, key: str, value: Any, updated_by: str) -> RuntimeConfigValue:
        definition = get_definition(key)
        validated, encoded = encode_value(definition, value)
        updated_at = int(time.time())
        async with async_session_factory() as session:
            async with session.begin():
                await session.merge(
                    RuntimeConfig(
                        key=key,
                        value=encoded,
                        updated_at=updated_at,
                        updated_by=updated_by,
                    )
                )
        return RuntimeConfigValue(
            key=definition.key,
            name=definition.name,
            description=definition.description,
            value_type=definition.value_type,
            editor=definition.editor,
            value=validated,
            default_value=self._default_value(definition.key),
            updated_at=updated_at,
            updated_by=updated_by,
        )


runtime_config = RuntimeConfigManager()


@get_driver().on_startup
async def seed_runtime_config_defaults() -> None:
    await runtime_config.ensure_defaults()
