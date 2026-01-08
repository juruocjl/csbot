from nonebot import get_plugin_config
from nonebot.plugin import PluginMetadata
from .config import Config

from sqlalchemy import Integer, Float, String, LargeBinary, Boolean, Text, BigInteger
from sqlalchemy.orm import DeclarativeBase, MappedAsDataclass, Mapped, mapped_column

__plugin_meta__ = PluginMetadata(
    name="models",
    description="",
    usage="",
    config=Config,
)

config = get_plugin_config(Config)

class Base(MappedAsDataclass, DeclarativeBase):
    pass

# 消息记录
class GroupMsg(Base):
    __tablename__ = "groupmsg"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True, init=False)
    mid: Mapped[int] = mapped_column(Integer)
    sid: Mapped[str] = mapped_column(String(50))
    timestamp: Mapped[int] = mapped_column(Integer, name="timeStamp")
    data: Mapped[bytes] = mapped_column(LargeBinary) # 对应 BLOB 类型

# 图片缓存信息
class ImgCacheInfo(Base):
    __tablename__ = "img_cache_info"

    hash: Mapped[str] = mapped_column(String(255), primary_key=True)
    count: Mapped[int] = mapped_column(Integer, default=0)
    valid: Mapped[bool] = mapped_column(Boolean, default=False)

# 群成员绑定信息
class GroupMember(Base):
    __tablename__ = "group_members"

    gid: Mapped[str] = mapped_column(String(20), primary_key=True)
    uid: Mapped[str] = mapped_column(String(20), primary_key=True)

# 成员绑定的 SteamID 信息
class MemberSteamID(Base):
    __tablename__ = "members_steamid"

    uid: Mapped[str] = mapped_column(String(20), primary_key=True)
    steamid: Mapped[str] = mapped_column(String(20))

# 完美比赛数据
class MatchStatsPW(Base):
    __tablename__ = "matches"

    # --- 复合主键 ---
    mid: Mapped[str] = mapped_column(String(50), primary_key=True)
    steamid: Mapped[str] = mapped_column(String(20), primary_key=True)

    # --- 赛季与地图 ---
    seasonId: Mapped[str] = mapped_column(String(20))
    mapName: Mapped[str] = mapped_column(String(50))
    
    # --- 队伍与比分 ---
    team: Mapped[int] = mapped_column(Integer)
    winTeam: Mapped[int] = mapped_column(Integer)
    score1: Mapped[int] = mapped_column(Integer)
    score2: Mapped[int] = mapped_column(Integer)
    
    # --- 评分数据 ---
    pwRating: Mapped[float] = mapped_column(Float)
    we: Mapped[float] = mapped_column(Float)
    
    # --- 基础数据 ---
    timeStamp: Mapped[int] = mapped_column(Integer)
    kill: Mapped[int] = mapped_column(Integer)
    death: Mapped[int] = mapped_column(Integer)
    assist: Mapped[int] = mapped_column(Integer)
    duration: Mapped[int] = mapped_column(Integer)
    mode: Mapped[str] = mapped_column(String(100))
    
    # --- PVP/完美特有数据 ---
    pvpScore: Mapped[int] = mapped_column(Integer)
    pvpStars: Mapped[int] = mapped_column(Integer)
    pvpScoreChange: Mapped[int] = mapped_column(Integer)
    pvpMvp: Mapped[int] = mapped_column(Integer)
    
    # --- 组队信息 (0/1) ---
    isgroup: Mapped[int] = mapped_column(Integer)
    greenMatch: Mapped[int] = mapped_column(Integer)
    
    # --- 详细击杀数据 ---
    entryKill: Mapped[int] = mapped_column(Integer)
    headShot: Mapped[int] = mapped_column(Integer)
    headShotRatio: Mapped[float] = mapped_column(Float)
    
    # --- 道具 ---
    flashTeammate: Mapped[int] = mapped_column(Integer)
    flashSuccess: Mapped[int] = mapped_column(Integer) # 修正了 mvpValue 类型
    
    # --- 多杀 ---
    twoKill: Mapped[int] = mapped_column(Integer)
    threeKill: Mapped[int] = mapped_column(Integer)
    fourKill: Mapped[int] = mapped_column(Integer)
    fiveKill: Mapped[int] = mapped_column(Integer)
    
    # --- 残局 ---
    vs1: Mapped[int] = mapped_column(Integer)
    vs2: Mapped[int] = mapped_column(Integer)
    vs3: Mapped[int] = mapped_column(Integer)
    vs4: Mapped[int] = mapped_column(Integer)
    vs5: Mapped[int] = mapped_column(Integer)
    
    # --- 伤害与其他 ---
    dmgArmor: Mapped[int] = mapped_column(Integer)
    dmgHealth: Mapped[int] = mapped_column(Integer)
    adpr: Mapped[float] = mapped_column(Float) 
    rws: Mapped[float] = mapped_column(Float)
    
    teamId: Mapped[int] = mapped_column(BigInteger)
    throwsCnt: Mapped[int] = mapped_column(Integer)
    snipeNum: Mapped[int] = mapped_column(Integer)
    firstDeath: Mapped[int] = mapped_column(Integer)

# 完美比赛额外数据
class MatchStatsPWExtra(Base):
    __tablename__ = "matches_extra"

    mid: Mapped[str] = mapped_column(String(50), primary_key=True)

    team1Legacy: Mapped[float] = mapped_column(Float)
    team2Legacy: Mapped[float] = mapped_column(Float)

# 官匹比赛数据
class MatchStatsGP(Base):
    __tablename__ = "matches_gp"

    # --- 复合主键 ---
    mid: Mapped[str] = mapped_column(String(50), primary_key=True)
    steamid: Mapped[str] = mapped_column(String(20), primary_key=True)

    # --- 基础信息 ---
    mapName: Mapped[str] = mapped_column(String(50))
    team: Mapped[int] = mapped_column(Integer)
    winTeam: Mapped[int] = mapped_column(Integer)
    score1: Mapped[int] = mapped_column(Integer)
    score2: Mapped[int] = mapped_column(Integer)
    timeStamp: Mapped[int] = mapped_column(Integer)
    mode: Mapped[str] = mapped_column(String(100))
    duration: Mapped[int] = mapped_column(Integer)
    
    # --- 击杀/死亡数据 ---
    kill: Mapped[int] = mapped_column(Integer)
    handGunKill: Mapped[int] = mapped_column(Integer)
    entryKill: Mapped[int] = mapped_column(Integer)
    awpKill: Mapped[int] = mapped_column(Integer)
    death: Mapped[int] = mapped_column(Integer)
    entryDeath: Mapped[int] = mapped_column(Integer)
    assist: Mapped[int] = mapped_column(Integer)
    headShot: Mapped[int] = mapped_column(Integer)
    
    # --- 评分 (Float) ---
    rating: Mapped[float] = mapped_column(Float)
    
    # --- 投掷物/战术 ---
    itemThrow: Mapped[int] = mapped_column(Integer)
    flash: Mapped[int] = mapped_column(Integer)
    flashTeammate: Mapped[int] = mapped_column(Integer)
    flashSuccess: Mapped[int] = mapped_column(Integer)
    
    # --- 多杀统计 ---
    twoKill: Mapped[int] = mapped_column(Integer)
    threeKill: Mapped[int] = mapped_column(Integer)
    fourKill: Mapped[int] = mapped_column(Integer)
    fiveKill: Mapped[int] = mapped_column(Integer)
    
    # --- 残局 (Clutch) ---
    vs1: Mapped[int] = mapped_column(Integer)
    vs2: Mapped[int] = mapped_column(Integer)
    vs3: Mapped[int] = mapped_column(Integer)
    vs4: Mapped[int] = mapped_column(Integer)
    vs5: Mapped[int] = mapped_column(Integer)
    
    # --- 进阶数据 (Float) ---
    adpr: Mapped[float] = mapped_column(Float)
    rws: Mapped[float] = mapped_column(Float)
    kast: Mapped[float] = mapped_column(Float)
    
    # --- 其他 ---
    rank: Mapped[int] = mapped_column(Integer)
    throwsCnt: Mapped[int] = mapped_column(Integer)
    bombPlanted: Mapped[int] = mapped_column(Integer)
    bombDefused: Mapped[int] = mapped_column(Integer)
    smokeThrows: Mapped[int] = mapped_column(Integer)
    grenadeDamage: Mapped[int] = mapped_column(Integer)
    infernoDamage: Mapped[int] = mapped_column(Integer)
    mvp: Mapped[int] = mapped_column(Integer)

# 官匹比赛额外数据
class MatchStatsGPExtra(Base):
    __tablename__ = "matches_gp_extra"

    mid: Mapped[str] = mapped_column(String(50), primary_key=True)

    team1Legacy: Mapped[float | None] = mapped_column(Float, nullable=True)
    team2Legacy: Mapped[float | None] = mapped_column(Float, nullable=True)

    


# Steam 用户基础信息
class SteamBaseInfo(Base):
    __tablename__ = "steamid_baseinfo_v2"

    # 主键
    steamid: Mapped[str] = mapped_column(String(20), primary_key=True)
    
    # 更新信息
    updateTime: Mapped[int] = mapped_column(Integer)
    # 比赛更新信息
    updateMatchTime: Mapped[int] = mapped_column(Integer)
    # 基础信息
    avatarlink: Mapped[str] = mapped_column(String(500))
    name: Mapped[str] = mapped_column(String(100))
    ladderScore: Mapped[str] = mapped_column(Text)
    # 格式 [{"season": "S?", "currSStars": 0, "score": 0, "currSLevel": 0, "matchCount": 0, "startTime": "2020-07-06 00:00:00"}]
    lasttime: Mapped[int] = mapped_column(Integer)

# Steam 用户详细信息
class SteamDetailInfo(Base):
    __tablename__ = "steam_detail_info"

    # --- 复合主键 ---
    steamid: Mapped[str] = mapped_column(String(20), primary_key=True)
    seasonId: Mapped[str] = mapped_column(String(20), primary_key=True)

    # --- 基础综合数据 ---
    pvpScore: Mapped[int] = mapped_column(Integer)
    pvpStars: Mapped[int] = mapped_column(Integer)
    cnt: Mapped[int] = mapped_column(Integer)
    winRate: Mapped[float] = mapped_column(Float)
    pwRating: Mapped[float] = mapped_column(Float)

    # --- KDA 与 基础评分 ---
    kills: Mapped[int] = mapped_column(Integer)
    rws: Mapped[float] = mapped_column(Float)
    pwRatingTAvg: Mapped[float] = mapped_column(Float)
    pwRatingCtAvg: Mapped[float] = mapped_column(Float)
    kastPerRound: Mapped[float] = mapped_column(Float)

    # --- 火力 (FirePower) ---
    firePowerScore: Mapped[int] = mapped_column(Integer)
    killsPerRound: Mapped[float] = mapped_column(Float)
    killsPerWinRound: Mapped[float] = mapped_column(Float)
    damagePerRound: Mapped[float] = mapped_column(Float)
    damagePerRoundWin: Mapped[float] = mapped_column(Float)
    roundsWithAKill: Mapped[float] = mapped_column(Float)
    multiKillRoundsPercentage: Mapped[float] = mapped_column(Float)
    we: Mapped[float] = mapped_column(Float)
    pistolRoundRating: Mapped[float] = mapped_column(Float)

    # --- 枪法 (Marksmanship) ---
    marksmanshipScore: Mapped[int] = mapped_column(Integer)
    headshotRate: Mapped[float] = mapped_column(Float)
    killTime: Mapped[int] = mapped_column(Integer)
    smHitRate: Mapped[float] = mapped_column(Float)
    reactionTime: Mapped[float] = mapped_column(Float)
    rapidStopRate: Mapped[float] = mapped_column(Float)

    # --- 补枪与辅助 (FollowUp) ---
    followUpShotScore: Mapped[int] = mapped_column(Integer)
    savedTeammatePerRound: Mapped[float] = mapped_column(Float)
    tradeKillsPerRound: Mapped[float] = mapped_column(Float)
    tradeKillsPercentage: Mapped[float] = mapped_column(Float)
    assistKillsPercentage: Mapped[float] = mapped_column(Float)
    damagePerKill: Mapped[float] = mapped_column(Float)

    # --- 首杀 (First Blood) ---
    firstScore: Mapped[int] = mapped_column(Integer)
    firstHurt: Mapped[float] = mapped_column(Float)
    winAfterOpeningKill: Mapped[float] = mapped_column(Float)
    firstSuccessRate: Mapped[float] = mapped_column(Float)
    firstKill: Mapped[float] = mapped_column(Float)
    firstRate: Mapped[float] = mapped_column(Float)

    # --- 道具 (Item/Utility) ---
    itemScore: Mapped[int] = mapped_column(Integer)
    itemRate: Mapped[float] = mapped_column(Float)
    utilityDamagePerRounds: Mapped[float] = mapped_column(Float)
    flashAssistPerRound: Mapped[float] = mapped_column(Float)
    flashbangFlashRate: Mapped[float] = mapped_column(Float)
    timeOpponentFlashedPerRound: Mapped[float] = mapped_column(Float)

    # --- 残局 (Clutch / 1vN) ---
    oneVnScore: Mapped[int] = mapped_column(Integer)
    v1WinPercentage: Mapped[float] = mapped_column(Float)
    clutchPointsPerRound: Mapped[float] = mapped_column(Float)
    lastAlivePercentage: Mapped[float] = mapped_column(Float)
    timeAlivePerRound: Mapped[float] = mapped_column(Float)
    savesPerRoundLoss: Mapped[float] = mapped_column(Float)

    # --- 狙击 (Sniper) ---
    sniperScore: Mapped[int] = mapped_column(Integer)
    sniperFirstKillPercentage: Mapped[float] = mapped_column(Float)
    sniperKillsPercentage: Mapped[float] = mapped_column(Float)
    sniperKillPerRound: Mapped[float] = mapped_column(Float)
    roundsWithSniperKillsPercentage: Mapped[float] = mapped_column(Float)
    sniperMultipleKillRoundPercentage: Mapped[float] = mapped_column(Float)

# Steam 用户额外信息
class SteamExtraInfo(Base):
    __tablename__ = "steam_extra_info"

    steamid: Mapped[str] = mapped_column(String(20), primary_key=True)
    timeStamp: Mapped[int] = mapped_column(Integer, primary_key=True)

    legacyScore: Mapped[float] = mapped_column(Float)

# AI 记忆存储
class AIMemory(Base):
    __tablename__ = "ai_mem"

    gid: Mapped[str] = mapped_column(String(20), primary_key=True)
    # 使用 Text 类型，因为 'mem' 看起来可能存储较长的文本或 JSON
    mem: Mapped[str] = mapped_column(Text)

# 用户认证会话
class AuthSession(Base):
    __tablename__ = "auth_sessions"
    
    # 长 Token，用于 API 鉴权 (主键)
    token: Mapped[str] = mapped_column(String(64), primary_key=True)
    # 短验证码，用于群内验证 (添加索引以加快查找)
    code: Mapped[str] = mapped_column(String(10), index=True)
    # 绑定的 QQ 号和群号 (验证后填写)
    user_id: Mapped[str | None] = mapped_column(String(20), nullable=True)
    group_id: Mapped[str | None] = mapped_column(String(20), nullable=True)
    # 创建时间 (用于计算过期)
    created_at: Mapped[int] = mapped_column(Integer)
    # 上一次使用时间
    last_used_at: Mapped[int] = mapped_column(Integer, default=0)
    # 是否已验证
    is_verified: Mapped[bool] = mapped_column(Boolean, default=False)

# 用户信息存储  
class UserInfo(Base):
    __tablename__ = "user_info"
    
    user_id: Mapped[str] = mapped_column(String(20), primary_key=True)
    nickname: Mapped[str] = mapped_column(String(100))
    
    last_send_time: Mapped[int] = mapped_column(Integer, default=0)
    last_update_time: Mapped[int] = mapped_column(Integer, default=0)

# 会员商品信息
class MemberGoods(Base):
    __tablename__ = "member_goods"
    uid: Mapped[str] = mapped_column(String(20), primary_key=True)
    marketHashName: Mapped[str] = mapped_column(String(500), primary_key=True)

# 商品信息记录
class GoodsInfo(Base):
    __tablename__ = "goods_info"
    marketHashName: Mapped[str] = mapped_column(String(500), primary_key=True)
    timeStamp: Mapped[int] = mapped_column(Integer, primary_key=True)
    goodId: Mapped[int] = mapped_column(Integer)
    name: Mapped[str] = mapped_column(String(500))
    buffSellPrice: Mapped[int] = mapped_column(Integer)
    buffSellNum: Mapped[int] = mapped_column(Integer)
    yyypSellPrice: Mapped[int] = mapped_column(Integer)
    yyypSellNum: Mapped[int] = mapped_column(Integer)
    steamSellPrice: Mapped[int] = mapped_column(Integer)
    steamSellNum: Mapped[int] = mapped_column(Integer)

# Major作业 
class MajorHW(Base):
    __tablename__ = "major_hw"

    # 复合主键：给两个字段都加上 primary_key=True
    uid: Mapped[str] = mapped_column(String(20), primary_key=True)
    stage: Mapped[str] = mapped_column(String(50), primary_key=True)
    
    teams: Mapped[str] = mapped_column(Text)
    winrate: Mapped[float] = mapped_column(Float)
    expval: Mapped[float] = mapped_column(Float)

# 复读点数记录
class FuduPoint(Base):
    __tablename__ = "fudu_points"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True, init=False)
    uid: Mapped[str] = mapped_column(String(50))
    timestamp: Mapped[int] = mapped_column(Integer, name="timeStamp")
    point: Mapped[int] = mapped_column(Integer)

# 本地存储键值对
class StorageItem(Base):
    __tablename__ = "local_storage"

    key: Mapped[str] = mapped_column(String(255), primary_key=True)
    val: Mapped[str] = mapped_column(Text)

# 直播状态记录
class LiveStatus(Base):
    __tablename__ = "live_status"

    liveid: Mapped[str] = mapped_column(String(50), primary_key=True)
    islive: Mapped[int] = mapped_column(Integer)