"""Register schedule APIs on the existing authenticated FastAPI app."""
import re

from fastapi import Body, Depends, HTTPException

from .store import schedule_store


def register_schedule_api(app, get_current_user, get_current_admin, runtime_config):
    @app.post("/api/schedule")
    async def schedule_data(uid: str | None = Body(None), page: int = Body(1, ge=1, le=10000),
                            eventId: int | None = Body(None, ge=1), _=Depends(get_current_user)):
        if uid is not None and not re.fullmatch(r"[0-9]{1,20}", uid):
            raise HTTPException(400, "无效的 QQ 号")
        watched, _ = await schedule_store.watched(await runtime_config.get("hltv_followed_teams"),
                                                 await runtime_config.get("hltv_manual_event_ids"))
        return await schedule_store.schedule([e.event_id for e in watched], uid, page, eventId)

    @app.post("/api/schedule/teams")
    async def search_teams(query: str = Body(..., embed=True, min_length=1, max_length=150), _=Depends(get_current_user)):
        return {"teams": await schedule_store.search(query)}

    @app.post("/api/schedule/follows")
    async def my_follows(info=Depends(get_current_user)):
        return {"teams": await schedule_store.follows(str(info.user_id))}

    @app.post("/api/schedule/follow")
    async def follow(teamId: str = Body(...), enabled: bool = Body(True), info=Depends(get_current_user)):
        # The request cannot choose the writer's QQ or notification group.
        if not info.user_id or not info.group_id:
            raise HTTPException(400, "请先绑定 QQ 和群")
        try:
            if enabled:
                await schedule_store.follow(str(info.user_id), teamId, str(info.group_id))
            else:
                await schedule_store.unfollow(str(info.user_id), teamId)
        except ValueError as exc:
            raise HTTPException(400, str(exc)) from exc
        return {"success": True}

    @app.post("/api/schedule/team-alias")
    async def add_alias(teamId: str = Body(...), alias: str = Body(..., min_length=1, max_length=150), _=Depends(get_current_admin)):
        from ..models import ScheduleTeam, ScheduleTeamAlias
        from .logic import compact
        async with schedule_store.lock, schedule_store.sessions() as session, session.begin():
            if await session.get(ScheduleTeam, teamId) is None or not compact(alias):
                raise HTTPException(400, "无效的队伍或别名")
            await session.merge(ScheduleTeamAlias(alias=compact(alias), team_id=teamId))
        return {"success": True}
