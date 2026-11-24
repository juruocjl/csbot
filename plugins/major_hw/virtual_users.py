"""虚拟用户（主播）配置与解析。

主播作业需要完整复用 ``major_hw`` / ``major_hw_snapshots`` 的计算、排名、混排、
网页与截图链路，同时不能污染 ``group_members``（群同步、积分等）。因此虚拟用户
只是普通的 ``major_hw.uid``，仅在“展示层”用配置的头像与昵称替换 QQ 头像昵称。

配置来源分两层：

1. ``assets/major_virtual_users.json``：随仓库发布的默认配置；
2. ``local_storage["major_hw_virtual_users"]``：线上热注册的增量配置，同 ``uid``
   覆盖默认配置（便于后续通过 ``/主播作业 添加`` 热加主播）。

本模块不依赖数据库结构，纯配置解析 + 一个带 TTL 的轻缓存；任何一层读取失败都
只回退到其它层，不会影响 Major 作业主链路。
"""

from __future__ import annotations

import json
import re
import time
from dataclasses import dataclass
from pathlib import Path
from unicodedata import normalize

from nonebot import logger

DEFAULT_CONFIG_PATH = Path("assets") / "major_virtual_users.json"
STORAGE_KEY = "major_hw_virtual_users"
UID_MAX_LENGTH = 20
UID_PATTERN = re.compile(r"^[A-Za-z0-9_]+$")
QQ_AVATAR_TEMPLATE = "https://q1.qlogo.cn/g?b=qq&nk={uid}&s=100"
CACHE_TTL_SECONDS = 30.0


def qq_avatar(uid: str) -> str:
    """普通 QQ 用户头像，保持既有行为不变。"""

    return QQ_AVATAR_TEMPLATE.format(uid=uid)


def normalize_text(value: str) -> str:
    return normalize("NFKC", str(value)).strip().casefold()


def compact_text(value: str) -> str:
    return "".join(ch for ch in normalize_text(value) if ch.isalnum())


def is_valid_uid(uid: str) -> bool:
    """虚拟 uid 需满足 ``major_hw.uid`` 的 varchar(20) 且纯字母数字下划线。"""

    text = str(uid or "").strip()
    return bool(text) and len(text) <= UID_MAX_LENGTH and bool(UID_PATTERN.match(text))


@dataclass(frozen=True)
class VirtualUser:
    uid: str
    name: str
    aliases: tuple[str, ...] = ()
    avatar: str = ""

    def display_name(self) -> str:
        return self.name or self.uid

    def avatar_or_default(self) -> str:
        return self.avatar or qq_avatar(self.uid)

    def keyword_list(self) -> list[str]:
        keys = [self.uid, self.name, *self.aliases]
        return [key for key in (str(item).strip() for item in keys) if key]

    def matches(self, text: str) -> bool:
        target = normalize_text(text)
        if not target:
            return False
        target_compact = compact_text(text)
        for key in self.keyword_list():
            key_norm = normalize_text(key)
            if target == key_norm:
                return True
            key_compact = compact_text(key)
            if key_compact and target_compact == key_compact:
                return True
        return False


def _coerce_aliases(raw: object) -> tuple[str, ...]:
    if raw is None:
        return ()
    if isinstance(raw, str):
        parts = [part for part in re.split(r"[,，;；|/]+", raw) if part.strip()]
        return tuple(dict.fromkeys(part.strip() for part in parts))
    if isinstance(raw, (list, tuple, set)):
        parts = [str(item).strip() for item in raw if str(item).strip()]
        return tuple(dict.fromkeys(parts))
    return ()


def parse_entry(raw: object, source: str = "config") -> VirtualUser | None:
    """把一条配置解析成 :class:`VirtualUser`，非法条目返回 None。"""

    if not isinstance(raw, dict):
        logger.warning(f"{source}: 虚拟用户条目不是对象，已忽略: {raw!r}")
        return None
    uid = str(raw.get("uid") or "").strip()
    name = str(raw.get("name") or "").strip()
    avatar = str(raw.get("avatar") or "").strip()
    if not is_valid_uid(uid):
        logger.warning(f"{source}: 虚拟用户 uid 非法（需 1-{UID_MAX_LENGTH} 位字母数字下划线），已忽略: {uid!r}")
        return None
    if not name:
        name = uid
    return VirtualUser(
        uid=uid,
        name=name,
        aliases=_coerce_aliases(raw.get("aliases")),
        avatar=avatar,
    )


def parse_entries(raw: object, source: str = "config") -> list[VirtualUser]:
    """支持 ``[{'uid': ...}]`` 列表或 ``{'uid': {...}}`` 映射两种配置结构。"""

    if isinstance(raw, dict) and "uid" in raw:
        items = [raw]
    elif isinstance(raw, dict):
        items = []
        for key, value in raw.items():
            if isinstance(value, dict):
                item = dict(value)
                item.setdefault("uid", key)
                items.append(item)
            else:
                items.append({"uid": key, "name": value})
    elif isinstance(raw, (list, tuple)):
        items = list(raw)
    else:
        logger.warning(f"{source}: 虚拟用户配置结构无法识别，已忽略: {type(raw).__name__}")
        return []

    users: list[VirtualUser] = []
    for item in items:
        user = parse_entry(item, source)
        if user is not None:
            users.append(user)
    return users


def merge_entries(defaults: list[VirtualUser], dynamic: list[VirtualUser]) -> list[VirtualUser]:
    """按 uid 合并，``dynamic`` 覆盖同 uid 默认配置，新 uid 追加在末尾。"""

    merged: dict[str, VirtualUser] = {}
    order: list[str] = []
    for user in [*defaults, *dynamic]:
        key = user.uid.casefold()
        if key not in merged:
            order.append(key)
        merged[key] = user
    return [merged[key] for key in order]


class VirtualUserRegistry:
    def __init__(self, users: list[VirtualUser] | None = None):
        self._users: list[VirtualUser] = list(users or [])
        self._by_uid: dict[str, VirtualUser] = {user.uid.casefold(): user for user in self._users}

    def __len__(self) -> int:
        return len(self._users)

    def __iter__(self):
        return iter(self._users)

    def all(self) -> list[VirtualUser]:
        return list(self._users)

    def get(self, uid: str | int | None) -> VirtualUser | None:
        if uid is None:
            return None
        return self._by_uid.get(str(uid).strip().casefold())

    def resolve(self, text: str | int | None) -> VirtualUser | None:
        """按 uid / 展示名 / 别名解析虚拟用户，返回 None 表示普通 QQ 用户。"""

        if text is None:
            return None
        value = str(text).strip()
        if not value:
            return None
        direct = self.get(value)
        if direct is not None:
            return direct
        for user in self._users:
            if user.matches(value):
                return user
        # 宽松兜底：唯一包含匹配（如 “玩机器CSGO” 命中别名 “玩机器”）
        target = compact_text(value)
        if len(target) >= 2:
            hits = [
                user
                for user in self._users
                if any(
                    len(compact_text(key)) >= 2 and compact_text(key) in target
                    for key in user.keyword_list()
                )
            ]
            if len(hits) == 1:
                return hits[0]
        return None

    def alias_index(self) -> dict[str, VirtualUser]:
        index: dict[str, VirtualUser] = {}
        for user in self._users:
            for key in user.keyword_list():
                index.setdefault(normalize_text(key), user)
        return index


def load_default_registry(path: Path | str | None = None) -> VirtualUserRegistry:
    config_path = Path(path) if path is not None else DEFAULT_CONFIG_PATH
    try:
        with open(config_path, "r", encoding="utf-8") as file:
            raw = json.load(file)
    except FileNotFoundError:
        logger.info(f"未找到虚拟用户默认配置 {config_path}，按空配置处理")
        return VirtualUserRegistry([])
    except Exception:
        logger.exception(f"读取虚拟用户默认配置 {config_path} 失败，按空配置处理")
        return VirtualUserRegistry([])
    return VirtualUserRegistry(parse_entries(raw, str(config_path)))


async def load_storage_entries() -> list[VirtualUser]:
    """从 ``local_storage`` 读取动态增量配置；任何异常都只回退为空列表。"""

    try:
        from ..utils import local_storage
    except Exception:
        logger.exception("加载虚拟用户动态配置失败：local_storage 不可用")
        return []
    try:
        raw = await local_storage.get(STORAGE_KEY)
    except Exception:
        logger.exception("读取 virtual users local_storage 失败，仅使用默认配置")
        return []
    if not raw:
        return []
    try:
        data = json.loads(raw)
    except Exception:
        logger.exception("virtual users local_storage 不是合法 JSON，已忽略")
        return []
    return parse_entries(data, STORAGE_KEY)


_registry: VirtualUserRegistry | None = None
_registry_loaded_at = 0.0


def cached_registry() -> VirtualUserRegistry:
    """同步获取当前缓存（未初始化时使用默认配置）。"""

    global _registry
    if _registry is None:
        _registry = load_default_registry()
    return _registry


def invalidate_cache() -> None:
    global _registry, _registry_loaded_at
    _registry = None
    _registry_loaded_at = 0.0


async def refresh_registry() -> VirtualUserRegistry:
    """重新读取默认配置 + ``local_storage`` 并刷新缓存。"""

    global _registry, _registry_loaded_at
    defaults = load_default_registry()
    dynamic = await load_storage_entries()
    _registry = VirtualUserRegistry(merge_entries(defaults.all(), dynamic))
    _registry_loaded_at = time.monotonic()
    return _registry


async def get_registry(force: bool = False, ttl: float = CACHE_TTL_SECONDS) -> VirtualUserRegistry:
    global _registry_loaded_at
    if force or _registry is None or (time.monotonic() - _registry_loaded_at) > ttl:
        return await refresh_registry()
    return _registry


async def resolve_virtual_user(text: str | int | None) -> VirtualUser | None:
    return (await get_registry()).resolve(text)


async def virtual_display_name(uid: str | int) -> str | None:
    user = (await get_registry()).get(uid)
    return user.display_name() if user else None


async def virtual_avatar(uid: str | int) -> str:
    """虚拟用户返回配置头像，普通 QQ uid 保持原 ``q1.qlogo.cn`` 逻辑。"""

    user = (await get_registry()).get(uid)
    if user is not None:
        return user.avatar_or_default()
    return qq_avatar(str(uid))


def serialize_users(users: list[VirtualUser]) -> str:
    return json.dumps(
        [
            {
                "uid": user.uid,
                "name": user.name,
                "aliases": list(user.aliases),
                "avatar": user.avatar,
            }
            for user in users
        ],
        ensure_ascii=False,
        separators=(",", ":"),
    )


async def register_virtual_user(
    uid: str,
    name: str,
    avatar: str = "",
    aliases: list[str] | tuple[str, ...] | None = None,
) -> VirtualUser:
    """把虚拟用户写入 ``local_storage``（增量层），并刷新缓存。

    同 ``uid`` 覆盖默认配置；不修改默认 JSON 文件。
    """

    from ..utils import local_storage

    clean_uid = str(uid).strip()
    if not is_valid_uid(clean_uid):
        raise ValueError(f"uid 需为 1-{UID_MAX_LENGTH} 位字母数字下划线：{uid!r}")
    clean_name = str(name or "").strip() or clean_uid
    entry = VirtualUser(
        uid=clean_uid,
        name=clean_name,
        aliases=_coerce_aliases(aliases),
        avatar=str(avatar or "").strip(),
    )

    dynamic = await load_storage_entries()
    merged: dict[str, VirtualUser] = {user.uid.casefold(): user for user in dynamic}
    merged[clean_uid.casefold()] = entry
    await local_storage.set(STORAGE_KEY, serialize_users(list(merged.values())))
    await refresh_registry()
    return entry
