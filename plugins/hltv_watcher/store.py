"""SQL-backed schedule. Discovery, queries and notification delivery share identities."""
from __future__ import annotations

import asyncio
import json
import time
import uuid
from dataclasses import replace

from sqlalchemy import delete, or_, select

from ..models import (ScheduleDelivery, ScheduleEvent, ScheduleFollow, ScheduleMatch,
                      ScheduleNotifyTarget, ScheduleTeam, ScheduleTeamAlias, ScheduleTeamSource)
from ..utils import async_session_factory
from .logic import EventInfo, compact, event_is_active
from .schedule_logic import ALIASES, result_key


def encode(value):
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


class ScheduleStore:
    def __init__(self, sessions=async_session_factory):
        self.sessions = sessions
        # Serialise discovery and user mutations within the single bot process.
        self.lock = asyncio.Lock()

    async def _team(self, session, ref):
        external_id = ref.get("external_id")
        if not external_id:
            return None
        source = await session.get(ScheduleTeamSource, ("5e", external_id))
        team = await session.get(ScheduleTeam, source.team_id) if source else None
        if team is None:
            team = ScheduleTeam(id=str(uuid.uuid4()), name=ref["name"], logo=ref.get("logo", ""), discovery_at=0)
            session.add(team)
            await session.flush()
            session.add(ScheduleTeamSource(source="5e", external_id=external_id, team_id=team.id))
        team.name = ref["name"]
        if ref.get("logo"):
            team.logo = ref["logo"]
        for alias in (team.name, *ALIASES.get(compact(team.name), ())):
            key = (compact(alias), team.id)
            if await session.get(ScheduleTeamAlias, key) is None:
                session.add(ScheduleTeamAlias(alias=key[0], team_id=key[1]))
        await session.flush()
        return team

    async def save_events(self, events):
        async with self.lock, self.sessions() as session, session.begin():
            for event in events:
                stored = await session.get(ScheduleEvent, event.event_id)
                if stored:
                    previous = EventInfo.from_dict(json.loads(stored.payload))
                    refs = {r["external_id"]: r for r in (*previous.team_refs, *event.team_refs)}
                    event = replace(event, team_refs=tuple(refs.values()), teams=tuple(r["name"] for r in refs.values()))
                for ref in event.team_refs:
                    await self._team(session, ref)
                await session.merge(ScheduleEvent(id=event.event_id, payload=encode(event.to_dict()), updated_at=int(time.time())))

    async def search(self, term: str):
        term = term.strip()
        normalized = compact(term)
        async with self.sessions() as session:
            teams = (await session.scalars(select(ScheduleTeam).order_by(ScheduleTeam.name))).all()
            sources = (await session.scalars(select(ScheduleTeamSource))).all()
            aliases = (await session.scalars(select(ScheduleTeamAlias))).all()
        exact_ids = {a.team_id for a in aliases if a.alias == normalized}
        exact_ids |= {s.team_id for s in sources if s.external_id == term}
        exact_ids |= {t.id for t in teams if t.id == term}
        if not term:
            return []
        matched = [t for t in teams if t.id in exact_ids] if exact_ids else [t for t in teams if normalized in compact(t.name)]
        return [{"id": t.id, "name": t.name, "logo": t.logo,
                 "external_ids": [s.external_id for s in sources if s.team_id == t.id],
                 "exact": t.id in exact_ids} for t in matched[:20]]

    async def default_ids(self, names):
        ids = set()
        for name in names:
            choices = [t for t in await self.search(name) if t["exact"]]
            if len(choices) == 1:
                ids.add(choices[0]["id"])
        return ids

    async def watched(self, defaults, manual_ids=()):
        default_ids = await self.default_ids(defaults)
        async with self.sessions() as session:
            followed = set((await session.scalars(select(ScheduleFollow.team_id).distinct())).all())
            refs = {s.external_id: s.team_id for s in (await session.scalars(select(ScheduleTeamSource))).all()}
            events = (await session.scalars(select(ScheduleEvent))).all()
        ids = default_ids | followed
        now = int(time.time())
        selected = []
        for row in events:
            event = EventInfo.from_dict(json.loads(row.payload))
            if event_is_active(event, now) and (event.is_major_tier or event.event_id in manual_ids or
                    any(refs.get(ref["external_id"]) in ids for ref in event.team_refs)):
                selected.append(event)
        known = {e.event_id for e in selected}
        catalog_ids = {e.id for e in events}
        selected += [EventInfo(event_id=eid, name=f"赛事 {eid}", status="live")
                     for eid in manual_ids if eid not in known and eid not in catalog_ids]
        # Match IDs are globally unique within 5E; parent/stage copies are deduped at ingestion.
        return sorted(selected, key=lambda e: (e.start_ts, e.event_id)), default_ids

    async def has_catalog(self):
        async with self.sessions() as session:
            return await session.scalar(select(ScheduleEvent.id).limit(1)) is not None

    async def due_team(self, default_names):
        ids = await self.default_ids(default_names)
        async with self.sessions() as session:
            ids |= set((await session.scalars(select(ScheduleFollow.team_id).distinct())).all())
            if not ids:
                return None
            row = (await session.execute(select(ScheduleTeam, ScheduleTeamSource)
                .join(ScheduleTeamSource, ScheduleTeam.id == ScheduleTeamSource.team_id)
                .where(ScheduleTeam.id.in_(ids), ScheduleTeam.discovery_at <= int(time.time()))
                .order_by(ScheduleTeam.discovery_at, ScheduleTeam.id).limit(1))).first()
            return (row[0].id, row[1].external_id) if row else None

    async def delay_team(self, team_id, seconds):
        async with self.lock, self.sessions() as session, session.begin():
            team = await session.get(ScheduleTeam, team_id)
            if team:
                team.discovery_at = int(time.time()) + seconds

    async def follow(self, uid, team_id, gid):
        async with self.lock, self.sessions() as session, session.begin():
            team = await session.get(ScheduleTeam, team_id)
            if team is None:
                raise ValueError("队伍不存在，请先搜索队伍")
            now = int(time.time())
            if await session.get(ScheduleFollow, (uid, team_id)) is None:
                session.add(ScheduleFollow(user_id=uid, team_id=team_id, created_at=now))
            if await session.get(ScheduleNotifyTarget, (uid, team_id, gid)) is None:
                session.add(ScheduleNotifyTarget(user_id=uid, team_id=team_id, group_id=gid, created_at=now))
            team.discovery_at = -now  # Newly requested teams precede routine refreshes.

    async def unfollow(self, uid, team_id, *, group_only=None):
        async with self.lock, self.sessions() as session, session.begin():
            condition = [ScheduleNotifyTarget.user_id == uid, ScheduleNotifyTarget.team_id == team_id]
            if group_only is not None:
                condition.append(ScheduleNotifyTarget.group_id == group_only)
            await session.execute(delete(ScheduleNotifyTarget).where(*condition))
            if group_only is None:
                await session.execute(delete(ScheduleFollow).where(ScheduleFollow.user_id == uid, ScheduleFollow.team_id == team_id))

    async def follows(self, uid):
        async with self.sessions() as session:
            teams = (await session.scalars(select(ScheduleTeam).join(ScheduleFollow)
                .where(ScheduleFollow.user_id == uid).order_by(ScheduleTeam.name))).all()
            targets = (await session.scalars(select(ScheduleNotifyTarget).where(ScheduleNotifyTarget.user_id == uid))).all()
        return [{"id": t.id, "name": t.name, "logo": t.logo,
                 "groups": sorted({n.group_id for n in targets if n.team_id == t.id})} for t in teams]

    async def _queue_delivery(self, session, item, revision, text, groups, now):
        for gid, route in groups.items():
            key = (item["id"], gid, revision)
            if await session.get(ScheduleDelivery, key) is None:
                session.add(ScheduleDelivery(match_id=item["id"], group_id=gid, revision=revision,
                    text=text, recipients=encode(sorted(route["users"])), default_push=route["default"],
                    status="pending", attempts=0, next_attempt=now, updated_at=now))

    async def ingest(self, event, matches, default_team_ids, default_groups, legacy_known=()):
        now = int(time.time())
        async with self.lock, self.sessions() as session, session.begin():
            targets = (await session.scalars(select(ScheduleNotifyTarget))).all()
            seen = set()
            for item in matches:
                if item["id"] in seen:
                    continue
                seen.add(item["id"])
                for side in ("team1", "team2"):
                    team = await self._team(session, item[side])
                    item[side]["team_id"] = team.id if team else None
                ids = {item["team1"]["team_id"], item["team2"]["team_id"]} - {None}
                old = await session.get(ScheduleMatch, item["id"])
                previous = json.loads(old.payload) if old else None
                started = item["status"] == "live" and (old is None or old.status != "live")
                finished = item["status"] == "finished"
                changed = finished and (old is None or old.status != "finished" or result_key(previous) != result_key(item))
                # A stale replica must not roll a completed match back to live/upcoming.
                if old and old.status == "finished" and not finished:
                    continue
                revision = (old.revision if old else 0) + int(changed)
                correction = bool(old and old.status == "finished" and changed)
                await session.merge(ScheduleMatch(id=item["id"], event_id=event.event_id,
                    team1_id=item["team1"]["team_id"], team2_id=item["team2"]["team_id"],
                    start_ts=item["start_ts"], status=item["status"], payload=encode(item), revision=revision, updated_at=now))
                if started:
                    groups = {}
                    # Revision zero is reserved for the start reminder; results start at one.
                    # Major tier alone does not subscribe every match to start notifications.
                    if old and ids & default_team_ids:
                        groups.update({str(g): {"default": True, "users": set()} for g in default_groups})
                    for target in targets:
                        eligible = bool(old) or item["start_ts"] >= target.created_at
                        if target.team_id in ids and eligible and target.created_at <= now:
                            groups.setdefault(target.group_id, {"default": False, "users": set()})["users"].add(target.user_id)
                    text = f"【比赛开始】{event.name}\n{item['team1']['name']} vs {item['team2']['name']}"
                    if item["format"]:
                        text += f" · BO{item['format']}"
                    await self._queue_delivery(session, item, 0, text, groups, now)
                if not changed:
                    continue
                groups = {}
                default_push = event.is_major_tier or bool(ids & default_team_ids)
                if default_push and (item["id"] not in legacy_known or correction):
                    groups.update({str(g): {"default": True, "users": set()} for g in default_groups})
                for target in targets:
                    # Unknown historical results are baselined, not blasted on first follow.
                    eligible = bool(old) or item["start_ts"] >= target.created_at
                    if target.team_id in ids and eligible and target.created_at <= now:
                        groups.setdefault(target.group_id, {"default": False, "users": set()})["users"].add(target.user_id)
                title = f"【{'赛果更正' if correction else '比赛结束'}】{event.name}"
                text = f"{title}\n{item['team1']['name']} {item['score1']}:{item['score2']} {item['team2']['name']}"
                if item["maps"]:
                    text += "\n" + " · ".join(f"{m['name']} {m['score1']}:{m['score2']}" for m in item["maps"]
                        if m["status"] == "finished" and m["score1"] is not None and m["score2"] is not None)
                await self._queue_delivery(session, item, revision, text, groups, now)

    async def claim_delivery(self, default_groups, personal_groups):
        now = int(time.time())
        async with self.lock, self.sessions() as session, session.begin():
            # A process crash after dispatch has an unknown outcome. Never blindly resend it.
            stale = (await session.scalars(select(ScheduleDelivery).where(ScheduleDelivery.status == "sending",
                ScheduleDelivery.updated_at < now - 300))).all()
            for row in stale:
                row.status = "unknown"
            rows = (await session.scalars(select(ScheduleDelivery).where(ScheduleDelivery.status == "pending",
                ScheduleDelivery.next_attempt <= now).order_by(ScheduleDelivery.next_attempt).limit(30))).all()
            for row in rows:
                match = await session.get(ScheduleMatch, row.match_id)
                if match is None or row.revision != match.revision or (row.revision == 0 and match.status != "live"):
                    row.status = "cancelled"
                    continue
                ids = {match.team1_id, match.team2_id} - {None}
                targets = (await session.scalars(select(ScheduleNotifyTarget).where(
                    ScheduleNotifyTarget.group_id == row.group_id, ScheduleNotifyTarget.team_id.in_(ids)))).all()
                users = sorted(set(json.loads(row.recipients)) & {t.user_id for t in targets}) if row.group_id in personal_groups else []
                if not users and not (row.default_push and row.group_id in default_groups):
                    row.status = "cancelled"
                    continue
                row.status, row.updated_at = "sending", now
                row.attempts += 1
                return {"key": (row.match_id, row.group_id, row.revision), "text": row.text,
                        "users": users, "group_id": row.group_id, "attempts": row.attempts}
        return None

    async def delivery_result(self, key, status):
        async with self.lock, self.sessions() as session, session.begin():
            row = await session.get(ScheduleDelivery, key)
            if row:
                row.status = "failed" if status == "pending" and row.attempts >= 5 else status
                row.updated_at = int(time.time())
                row.next_attempt = row.updated_at + min(1800, 30 * 2 ** row.attempts)

    async def schedule(self, watched_ids, uid=None, page=1, event_id=None):
        async with self.sessions() as session:
            query = select(ScheduleMatch).where(ScheduleMatch.status.in_(["live", "upcoming", "unknown"]),
                ScheduleMatch.event_id.in_(watched_ids))
            if uid:
                ids = (await session.scalars(select(ScheduleFollow.team_id).where(ScheduleFollow.user_id == uid))).all()
                query = query.where(or_(ScheduleMatch.team1_id.in_(ids), ScheduleMatch.team2_id.in_(ids)))
            if event_id:
                query = query.where(ScheduleMatch.event_id == event_id)
            rows = (await session.scalars(query)).all()
            events = (await session.scalars(select(ScheduleEvent).where(ScheduleEvent.id.in_(watched_ids)))).all()
        rows.sort(key=lambda m: (m.status != "live", m.start_ts == 0, m.start_ts, m.id))
        total = len(rows)
        selected = rows[(page - 1) * 10:page * 10]
        return {"uid": uid, "page": page, "page_size": 10, "total": total,
                "matches": [{**json.loads(row.payload), "updated_at": row.updated_at} for row in selected],
                "follows": [{k: v for k, v in t.items() if k != "groups"} for t in await self.follows(uid)] if uid else [],
                "live_count": sum(row.status == "live" for row in rows),
                "events": [{"id": e.id, "name": json.loads(e.payload)["name"]} for e in events],
                "generated_at": int(time.time())}


schedule_store = ScheduleStore()
