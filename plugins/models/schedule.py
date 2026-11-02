"""Stable tournament identities and durable, opt-in group notifications."""
from sqlalchemy import BigInteger, Boolean, ForeignKey, Index, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column
from . import Base


class ScheduleTeam(Base):
    __tablename__ = "schedule_team"
    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    name: Mapped[str] = mapped_column(String(150))
    logo: Mapped[str] = mapped_column(Text)
    discovery_at: Mapped[int] = mapped_column(BigInteger)


class ScheduleTeamSource(Base):
    __tablename__ = "schedule_team_source"
    source: Mapped[str] = mapped_column(String(20), primary_key=True)
    external_id: Mapped[str] = mapped_column(String(100), primary_key=True)
    team_id: Mapped[str] = mapped_column(ForeignKey("schedule_team.id"), index=True)


class ScheduleTeamAlias(Base):
    __tablename__ = "schedule_team_alias"
    alias: Mapped[str] = mapped_column(String(150), primary_key=True)
    team_id: Mapped[str] = mapped_column(ForeignKey("schedule_team.id"), primary_key=True)


class ScheduleEvent(Base):
    __tablename__ = "schedule_event"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    payload: Mapped[str] = mapped_column(Text)
    updated_at: Mapped[int] = mapped_column(BigInteger)


class ScheduleMatch(Base):
    __tablename__ = "schedule_match"
    id: Mapped[str] = mapped_column(String(100), primary_key=True)
    event_id: Mapped[int] = mapped_column(Integer, index=True)
    team1_id: Mapped[str | None] = mapped_column(String(36), index=True)
    team2_id: Mapped[str | None] = mapped_column(String(36), index=True)
    start_ts: Mapped[int] = mapped_column(BigInteger, index=True)
    status: Mapped[str] = mapped_column(String(20), index=True)
    payload: Mapped[str] = mapped_column(Text)
    revision: Mapped[int] = mapped_column(Integer)
    updated_at: Mapped[int] = mapped_column(BigInteger)


class ScheduleFollow(Base):
    __tablename__ = "schedule_follow"
    user_id: Mapped[str] = mapped_column(String(20), primary_key=True)
    team_id: Mapped[str] = mapped_column(ForeignKey("schedule_team.id"), primary_key=True)
    created_at: Mapped[int] = mapped_column(BigInteger)


class ScheduleNotifyTarget(Base):
    __tablename__ = "schedule_notify_target"
    user_id: Mapped[str] = mapped_column(String(20), primary_key=True)
    team_id: Mapped[str] = mapped_column(ForeignKey("schedule_team.id"), primary_key=True)
    group_id: Mapped[str] = mapped_column(String(20), primary_key=True)
    created_at: Mapped[int] = mapped_column(BigInteger)


class ScheduleDelivery(Base):
    __tablename__ = "schedule_delivery"
    match_id: Mapped[str] = mapped_column(String(100), primary_key=True)
    group_id: Mapped[str] = mapped_column(String(20), primary_key=True)
    revision: Mapped[int] = mapped_column(Integer, primary_key=True)
    text: Mapped[str] = mapped_column(Text)
    recipients: Mapped[str] = mapped_column(Text)
    default_push: Mapped[bool] = mapped_column(Boolean)
    status: Mapped[str] = mapped_column(String(20), index=True)
    attempts: Mapped[int] = mapped_column(Integer)
    next_attempt: Mapped[int] = mapped_column(BigInteger)
    updated_at: Mapped[int] = mapped_column(BigInteger)
    __table_args__ = (Index("ix_schedule_delivery_due", "status", "next_attempt"),)
