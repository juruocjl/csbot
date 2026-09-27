"""Read-only group knowledge, scoped by the authenticated invocation.

Live OneBot calls have fixed names/arguments; no generic platform RPC, URLs,
configuration keys or group parameters are accepted from scripts.
"""
import asyncio
from datetime import datetime, timedelta, timezone
from io import BytesIO
import re

import aiohttp
from PIL import Image

CALLS = {
    'group_members': {'params': {'search': '', 'offset': 0, 'limit': 50}, 'description': 'QQ当前成员，按QQ/昵称/群名片匹配；分页，返回 total/truncated。昵称重复须确认QQ。'},
    'member_info': {'params': {'uid': '必填QQ字符串'}, 'description': '本群当前成员的QQ昵称、群名片、实际角色、头像地址；不会查群外陌生人。'},
    'member_avatar': {'params': {'uid': '必填QQ字符串'}, 'description': '核验本群成员后下载QQ头像，返回只读full_path/thumbnail_path供read_image查看；不自动发送。'},
    'group_admins': {'params': {}, 'description': 'QQ实时群主/管理员，另附机器人竞选状态；查询失败不会用旧状态冒充实时权限。'},
    'election_rules': {'params': {}, 'description': '实际复读/管理员竞选规则、时间窗口、当前群是否启用定时竞选/禁言。'},
    'points_today': {'params': {'uid': '可选QQ字符串', 'day': 0}, 'description': '复读业务日（本机时区23:55起）常规点、奖励点、惩罚触发次数；day=1为上一业务日。不传uid返回有流水者排行，并非竞选概率。'},
}


def checked_at():
    return datetime.now(timezone.utc).isoformat()


def qq_id(value):
    if not isinstance(value, str) or not re.fullmatch(r'[0-9]{1,20}', value):
        raise ValueError('uid must be a QQ string')
    return value


def live_bot():
    from nonebot import get_bots, get_driver
    from nonebot.adapters.onebot.v11 import Bot
    configured = str(getattr(get_driver().config, 'cs_botid', ''))
    bots = [bot for bot in get_bots().values() if isinstance(bot, Bot)]
    if configured and configured != '0':
        bots = [bot for bot in bots if bot.self_id == configured]
    if len(bots) != 1:
        raise ValueError('QQ connection unavailable or ambiguous; current group information cannot be confirmed')
    return bots[0]


def settings():
    from nonebot import get_driver
    config = get_driver().config
    return {key: getattr(config, key, default) for key, default in (
        ('cs_group_list', []), ('cs_fudu_ban_group_list', []), ('cs_fudu_delay', 10))}


def point_window(day=0, now=None):
    if type(day) is not int or not 0 <= day <= 365:
        raise ValueError('day must be an integer from 0 to 365')
    # Match plugins.utils.get_today_start_timestamp(refreshtime=86100).
    current = datetime.fromtimestamp(now) if now is not None else datetime.now()
    start = current.replace(hour=23, minute=55, second=0, microsecond=0)
    if current < start:
        start -= timedelta(days=1)
    start -= timedelta(days=day)
    return int(start.timestamp()), int(start.timestamp()) + 86400


class GroupKnowledge:
    def __init__(self, broker, *, bot_provider=live_bot, settings_provider=settings, cache=None):
        self.broker = broker
        self.gid = qq_id(broker.group_id)
        self.bot_provider = bot_provider
        self.settings_provider = settings_provider
        self.cache = cache
        self._members = None
        self._members_checked = None
        self._avatars = {}

    async def members(self):
        # One fresh snapshot per invocation, including unbound members.
        if self._members is None:
            try:
                async with asyncio.timeout(8):
                    raw = await self.bot_provider().get_group_member_list(group_id=int(self.gid), no_cache=True)
                if not isinstance(raw, list) or not raw:
                    raise ValueError('invalid member list')
                rows = []
                for item in raw:
                    if str(item.get('group_id', self.gid)) != self.gid:
                        raise ValueError('unexpected group')
                    uid = qq_id(str(item['user_id']))
                    role = item.get('role')
                    rows.append({'uid': uid, 'nickname': str(item.get('nickname') or '')[:200],
                                 'card': str(item.get('card') or '')[:200],
                                 'role': role if role in {'owner', 'admin', 'member'} else 'unknown',
                                 'avatar_url': f'https://q1.qlogo.cn/g?b=qq&nk={uid}&s=640'})
                self._members = sorted(rows, key=lambda row: int(row['uid']))
                self._members_checked = checked_at()
            except Exception:
                raise ValueError('QQ member lookup failed; current membership, names and roles are unknown') from None
        return self._members

    async def member(self, uid):
        qq_id(uid)
        for row in await self.members():
            if row['uid'] == uid:
                return row
        raise ValueError('member not found in this group')

    async def avatar(self, uid):
        row = await self.member(uid)
        if uid not in self._avatars:
            if len(self._avatars) >= 8:
                raise ValueError('avatar budget exhausted (8 per turn)')
            try:
                async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=8), trust_env=False) as session:
                    async with session.get(row['avatar_url'], allow_redirects=False) as response:
                        if response.status != 200:
                            raise ValueError('avatar HTTP failure')
                        content = bytearray()
                        async for chunk in response.content.iter_chunked(65536):
                            content.extend(chunk)
                            if len(content) > 2 * 1024**2:
                                raise ValueError('avatar too large')
                def save():
                    # Bound decompression separately from the general image cache.
                    with Image.open(BytesIO(content)) as picture:
                        if picture.width * picture.height > 4_000_000:
                            raise ValueError('avatar pixel limit')
                        picture.verify()
                    return self.cache.put(bytes(content), private=True)
                self._avatars[uid] = await asyncio.to_thread(save)
            except Exception:
                raise ValueError('QQ avatar unavailable; do not claim to have viewed it') from None
        return self.cache.info(self._avatars[uid]) | {'uid': uid, 'source': 'QQ avatar CDN', 'fetched_at': checked_at()}

    def rules(self):
        config = self.settings_provider()
        return {
            'source': 'plugins/fudu + plugins/allmsg (deployed implementation)',
            'fetched_at': checked_at(),
            'scheduled_election_enabled': self.gid in {str(g) for g in config['cs_group_list']},
            'punishment_ban_enabled': self.gid in {str(g) for g in config['cs_fudu_ban_group_list']},
            'repeat_delay_seconds': config['cs_fudu_delay'],
            'schedule': '服务器本地时区每日23:55；点数业务日也在23:55切换。',
            'eligibility': '候选来自当日00:00以来发过消息的人，须绑定Steam；排除local_storage记录的上一位竞选管理员（即使已下放）及转让排除名单。',
            'weight': '(常规点数/(惩罚触发次数+1)+奖励点数+1)*ln(1+天梯场次+0.6*官匹场次+0.3*内战场次)',
            'selection': '按权重随机抽一人，不是最高分必当选；个人概率=个人权重/总权重。无候选或总权重<=0会失败。',
            'windows': '自动23:55竞选取刚结束的上一点数业务日(day=1)，比赛取当日；23:55前手动roll比赛取昨日。/点数排行取当前业务日(day=0)及当日比赛，是预测，不能等同刚结束的竞选结果。',
            'transfer': '当前在位竞选管理员可在最近23:55起12小时内转让给本群另一成员；转出者加入排除名单，成功新一轮竞选后清空。该窗口按每日23:55计算，不是按实际手动roll时间。',
            'points': '常规点与奖励点分开；奖励点不参与禁言/下放概率。同一句的已有参与者再复读或使用TS poke为5，新参与者依加入顺序加1、2、3、4…（并非封顶3）；每个新参与者给原发送者奖励1。指定@一人的sb/傻逼/艾斯比额外5；管理员设置昵称给被设置者加20；禁言操作给操作者按秒加点、解除禁言50。',
            'punishment': '普通成员max(0.02,tanh((本次点数*加点后累计常规点-50)/500))；竞选在位管理员max(0,tanh((本次点数*加点后累计常规点/100-50)/500))。',
            'zero_point_meaning': '常规point=0记录一次普通成员惩罚触发；未启用禁言的群也会记此记录，因此不是实际QQ禁言次数。下次触发的禁言分钟数=本业务日触发次数+1。',
            'authority': '竞选状态与QQ权限分开；QQ管理员可能由群主手动修改，以group_admins实际角色为准。规则和实时数值不应保存为永久不变的记忆。',
        }

    async def call(self, name, params):
        if not isinstance(params, dict) or set(params) - set(CALLS[name]['params']):
            raise ValueError('unknown group query parameter; consult catalog')
        if name == 'election_rules':
            return self.rules()
        if name == 'points_today':
            start, end = point_window(params.get('day', 0))
            uid = params.get('uid')
            if uid is not None:
                await self.member(uid)
            sql = '''SELECT uid,
                COALESCE(SUM(CASE WHEN point_type=0 THEN point ELSE 0 END),0) AS regular_points,
                COALESCE(SUM(CASE WHEN point_type=1 THEN point ELSE 0 END),0) AS reward_points,
                SUM(CASE WHEN point_type=0 AND point=0 THEN 1 ELSE 0 END) AS punishment_triggers,
                COUNT(*) AS records FROM points WHERE timestamp>=:start AND timestamp<:end'''
            if uid is not None:
                sql += ' AND uid=:uid'
            result = await self.broker.query(sql + ' GROUP BY uid ORDER BY regular_points DESC,reward_points DESC,uid',
                                             {'start': start, 'end': end, 'uid': uid})
            if uid is not None and not result['rows']:
                result['rows'] = [{'uid': uid, 'regular_points': 0, 'reward_points': 0, 'punishment_triggers': 0, 'records': 0}]
            return result | {'window_start': datetime.fromtimestamp(start).astimezone().isoformat(),
                             'window_end': datetime.fromtimestamp(end).astimezone().isoformat(),
                             'note': '复读点数排行不是竞选权重排行；惩罚触发次数不代表实际禁言次数。'}
        if name == 'member_info':
            return {'member': await self.member(params.get('uid')), 'source': 'OneBot get_group_member_list', 'fetched_at': self._members_checked}
        if name == 'member_avatar':
            return await self.avatar(params.get('uid'))
        if name == 'group_admins':
            members = await self.members()
            state = await self.broker.call('election_state')
            return {'qq_roles': [row for row in members if row['role'] in {'owner', 'admin'}],
                    'roles_complete': all(row['role'] != 'unknown' for row in members),
                    'election_state': state, 'source': 'OneBot get_group_member_list', 'fetched_at': self._members_checked}
        offset, limit = params.get('offset', 0), params.get('limit', 50)
        search = params.get('search', '')
        if type(offset) is not int or offset < 0 or type(limit) is not int or not 1 <= limit <= 100 or not isinstance(search, str) or len(search) > 100:
            raise ValueError('invalid member search/page')
        rows = [row for row in await self.members() if any(search.casefold() in row[key].casefold() for key in ('uid', 'nickname', 'card'))]
        return {'rows': rows[offset:offset+limit], 'total': len(rows), 'truncated': offset+limit < len(rows),
                'source': 'OneBot get_group_member_list', 'fetched_at': self._members_checked}
