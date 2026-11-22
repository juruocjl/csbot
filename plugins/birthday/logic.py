"""Auto birthday wishes: pure standard-library logic, safe to unit test alone.

This module deliberately imports nothing from NoneBot, the database or the AI
runtime so the calendar/cooldown/text rules stay deterministic and testable.
"""
from __future__ import annotations

import calendar
from dataclasses import dataclass
from datetime import date
import re
from typing import Literal


CLEAR_OVERRIDE = "__CLEAR__"
DISABLED_VALUE = "none"

DISABLE_WORDS = frozenset({"none", "off", "关闭", "取消", "禁用", "disable"})
CLEAR_WORDS = frozenset({"default", "clear", "reset", "默认", "清除", "恢复", "恢复默认"})

USAGE_HINT = (
    "生日格式支持 07-10、7-10、7/10、7.10、7月10日、2000-07-10；"
    "也可以发送 关闭/none 关闭自动祝福，或发送 默认/清除 恢复读取 QQ 资料。"
)

AT_PATTERN = re.compile(r"\[(?:at|AT)\s*[:：]\s*(\d+)\]")
_MONTH_DAY_PATTERNS = (
    re.compile(r"^(\d{1,2})\s*[-/.]\s*(\d{1,2})$"),
    re.compile(r"^(\d{1,2})\s*月\s*(\d{1,2})\s*[日号]$"),
)
_FULL_DATE_PATTERN = re.compile(
    r"^(\d{4})\s*[-/.年]\s*(\d{1,2})\s*[-/.月]\s*(\d{1,2})\s*[日号]?$"
)
MAX_NOTICE_CHARS = 1200


def validate_mmdd(month: int, day: int) -> str:
    """Validate a real calendar month/day (leap day 2024-02-29 included)."""
    if type(month) is not int or type(day) is not int:
        raise ValueError(f"生日日期非法。{USAGE_HINT}")
    try:
        date(2024, month, day)
    except ValueError:
        raise ValueError(f"日期 {month}-{day} 不存在。{USAGE_HINT}") from None
    return f"{month:02d}-{day:02d}"


def parse_birthday_input(raw: str) -> str:
    """Normalize a user-supplied birthday into ``MM-DD`` / ``none`` / clear."""
    if not isinstance(raw, str):
        raise ValueError(USAGE_HINT)
    text = raw.strip()
    if not text:
        raise ValueError(USAGE_HINT)
    lowered = text.lower()
    if lowered in DISABLE_WORDS:
        return DISABLED_VALUE
    if lowered in CLEAR_WORDS:
        return CLEAR_OVERRIDE
    match = _FULL_DATE_PATTERN.fullmatch(text)
    if match:
        year, month, day = (int(part) for part in match.groups())
        try:
            date(year, month, day)
        except ValueError:
            raise ValueError(f"日期 {year}-{month:02d}-{day:02d} 不存在。{USAGE_HINT}") from None
        return validate_mmdd(month, day)
    for pattern in _MONTH_DAY_PATTERNS:
        match = pattern.fullmatch(text)
        if match:
            return validate_mmdd(int(match.group(1)), int(match.group(2)))
    raise ValueError(f"无法识别生日「{text}」。{USAGE_HINT}")


@dataclass(frozen=True)
class BirthdayResolution:
    mmdd: str | None
    source: Literal["override", "disabled", "profile", "none"]
    profile_year: int | None = None


@dataclass(frozen=True)
class MemberBirthday:
    """One group member with an effective birthday."""

    uid: str
    mmdd: str
    source: str
    name: str = ""
    nickname: str = ""
    card: str = ""
    profile_year: int | None = None


def resolve_effective_birthday(uid, profile_month, profile_day, overrides, *,
                               profile_year=None) -> BirthdayResolution:
    """Apply override > disabled > profile precedence for one member."""
    raw = (overrides or {}).get(str(uid).strip()) if overrides else None
    if isinstance(raw, str):
        value = raw.strip()
        if value.lower() == DISABLED_VALUE:
            return BirthdayResolution(None, "disabled", None)
        match = re.fullmatch(r"(\d{1,2})-(\d{1,2})", value)
        if match:
            try:
                override = validate_mmdd(int(match.group(1)), int(match.group(2)))
            except ValueError:
                override = None
            if override:
                return BirthdayResolution(override, "override", None)
    try:
        profile = validate_mmdd(profile_month, profile_day)
    except ValueError:
        profile = None
    if not profile:
        return BirthdayResolution(None, "none", None)
    year = profile_year if type(profile_year) is int and 1 <= profile_year <= 9999 else None
    return BirthdayResolution(profile, "profile", year)


def matches_birthday_on_date(mmdd: str | None, target_date: date) -> bool:
    """Match MM-DD, sending a 02-29 birthday on 02-28 in common years."""
    if not mmdd:
        return False
    if f"{target_date.month:02d}-{target_date.day:02d}" == mmdd:
        return True
    if mmdd == "02-29" and not calendar.isleap(target_date.year):
        return target_date.month == 2 and target_date.day == 28
    return False


def days_until_birthday(mmdd: str | None, today: date) -> int | None:
    """Days until the next MM-DD occurrence (today -> 0), or ``None`` if invalid.

    Keeps the same leap-day rule as :func:`matches_birthday_on_date`: a 02-29
    birthday fires on 02-28 in common years.
    """
    if not isinstance(mmdd, str):
        return None
    match = re.fullmatch(r"(\d{2})-(\d{2})", mmdd.strip())
    if not match:
        return None
    month, day = int(match.group(1)), int(match.group(2))
    for year in (today.year, today.year + 1):
        try:
            candidate = date(year, month, day)
        except ValueError:
            if (month, day) == (2, 29) and not calendar.isleap(year):
                candidate = date(year, 2, 28)
            else:
                return None
        delta = (candidate - today).days
        if delta >= 0:
            return delta
    return None


def check_cooldown(last_updated_at: int | None, now: int,
                   cooldown_seconds: int) -> tuple[bool, int]:
    """Return ``(allowed, remaining_seconds)`` for self-service edits."""
    try:
        window = int(cooldown_seconds)
    except (TypeError, ValueError):
        return True, 0
    if window <= 0:
        return True, 0
    if type(last_updated_at) is not int:
        return True, 0
    remaining = last_updated_at + window - int(now)
    if remaining <= 0:
        return True, 0
    return False, remaining


def format_cooldown_remaining(seconds: int) -> str:
    """Format remaining seconds as short readable Chinese."""
    try:
        seconds = int(seconds)
    except (TypeError, ValueError):
        seconds = 0
    if seconds <= 0:
        return "0 秒"
    if seconds < 60:
        return f"{seconds} 秒"
    minutes = (seconds + 59) // 60
    if minutes < 60:
        return f"{minutes} 分钟"
    hours, minutes = divmod(minutes, 60)
    if hours < 24:
        return f"{hours} 小时" + (f" {minutes} 分钟" if minutes else "")
    days, hours = divmod(hours, 24)
    return f"{days} 天" + (f" {hours} 小时" if hours else "")


def _truncate_notice(value: str) -> str:
    head = value[:MAX_NOTICE_CHARS]
    best = -1
    for separator in ("\n", "。", "！", "？", "；", "…", ".", "!", "?", ";"):
        best = max(best, head.rfind(separator))
    if best >= MAX_NOTICE_CHARS // 2:
        return head[:best + 1].rstrip()
    return head.rstrip()[:MAX_NOTICE_CHARS - 1] + "…"


def sanitize_notice_text(text: str, *, fallback_name: str = "",
                         blocked_texts=None) -> str:
    """Turn a model reply into a publishable plain-text group notice."""
    if not isinstance(text, str):
        raise ValueError("祝福正文不可发布")
    value = text.replace("\r\n", "\n").replace("\r", "\n")
    value = AT_PATTERN.sub(fallback_name or "", value)
    lines = [re.sub(r"[ \t]{2,}", " ", line).strip() for line in value.split("\n")]
    while lines and not lines[0]:
        lines.pop(0)
    while lines and not lines[-1]:
        lines.pop()
    value = "\n".join(lines).strip()
    blocked = {item.strip() for item in (blocked_texts or ())
               if isinstance(item, str) and item.strip()}
    if not value or value in blocked:
        raise ValueError("祝福正文不可发布")
    if len(value) > MAX_NOTICE_CHARS:
        value = _truncate_notice(value)
    return value


def build_birthday_prompt(*, group_id: str, user_id: str, display_name: str,
                          nickname: str, card: str, target_date: date, source: str,
                          birthday_year: int | None = None) -> str:
    """Prompt for the same DSH chatbot: research first, then a deadpan-exaggerated notice."""
    source_line = ("生日来自覆盖设置" if source == "override"
                   else "生日来自该群友的 QQ 资料")
    year_line = (f"资料生日年份：{birthday_year}（仅用于判断是否成年，不要主动报年龄）\n"
                 if birthday_year else "")
    return f"""你现在要为本群（群号 {group_id}）写一条将于 {target_date:%Y年%m月%d日} 0 点发布的 QQ 群公告生日祝福。
寿星信息：QQ={user_id}，当前群名片={card or '无'}，QQ 昵称={nickname or '无'}，参考称呼={display_name}（{source_line}）。
{year_line}
【第一步：动笔前必须先全面检索】
1. 先用 memory_search 分别按 QQ 号、群名片、QQ 昵称及查到的外号检索，摸清对方在群里的常用外号、性格、口头禅、标志性名场面与常聊话题。
2. 再用 execute_python 调用 csdata 检索其历史群聊发言与被提及记录（csdata.search / csdata.query），以及绑定信息、CS 战绩与常玩游戏（members / profiles / matches_pw / matches_gp / game_history / points_today）。

【第二步：写作风格——用“一本正经的夸张反差”消解 AI 味】
AI 写祝福最违和的是假装很熟的抒情套话；反而把群公告写成“排场极大、一本正经的夸张庆贺”，再塞进只有本群才知道的真实细节，反差感最好笑也最像自己人。
- **选一个贴合此人的夸张外壳**（按查到的真实人设自然挑选，不必每次固定同一种）：例如群史馆特大喜讯通告、Major 赛后 MVP / 年度 Top 1 授勋词、突发头版快讯、全群荣誉嘉奖令等。
- **反差靠具体实料，不靠空洞辞藻**：越是具体的真细节（真实外号、经典口癖、离谱下饭操作或高光战绩、作息习惯、爱玩的游戏），放进越郑重其事的排场里越有喜剧效果。把这些真实梗一本正经包装成“震撼全群的传奇履历”，末尾干脆利落收在生日祝福。
- **称呼自然**：查到群友习惯叫的真实外号就优先用外号，不要机械念 QQ 号或硬凑称呼。
- **按实料多少自然伸缩**：
  - 料很足：挑最出彩的 2~4 个真梗串成有节奏的夸张公告，约 180~350 字；
  - 料不多（仅 1~2 条零星记录）：围绕这件真事抖一个夸张机灵即可，约 100~180 字，不强行扩写成通稿；
  - 几乎没料（潜水/新人）：40~90 字短平快，用“本群稀有潜水传奇迎来一年一度合法现身日”这类轻松戏谑的轻量排场祝一句生日快乐，绝不假装很熟、不硬凑长篇。

【第三步：红线与输出格式】
- **拒绝油腻模板**：严禁“在这个特别的日子里”“愿你被世界温柔以待”“乘风破浪”“璀璨星河”等春晚式官话、空洞排比和干瘪彩虹屁。
- **严禁编造与越线**：提到的每一个外号、战绩、游戏、名场面都必须来自本次检索到的真实记录，查不到一律不脑补；调侃点到为止、底色喜庆友善，绝不人身攻击、不碰隐私与敏感话题。
- **纯文本群公告**：直接输出可发布的纯文本正文，不要 Markdown 符号（不用 **、#、列表符）、不要链接、不要表情包占位、绝对不要输出 [at:QQ号]；不要暴露检索过程，不要出现“我查了数据库/记忆”等字样，总字数控制在 350 字以内。"""
