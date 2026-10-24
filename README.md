# csbot

## How to start

1. `uv sync` .
4. `uv run nb run` .

## Runtime configuration

Administrators (`SUPERUSERS`) can edit database-backed configuration at `/admin/config`.
The registered keys include `hltv_event_id_list`, `hltv_followed_teams`, `cs_season_id`
(current season), `cs_last_season_id` (previous season), `cs_time_locations`, and
`cs_ai_model`. The live-stream monitor list is available as `live_watch_list`.
Season values are JSON strings such as `"S21"` and `"S20"`; these are the initial
defaults from the example configuration, not automatically detected seasons. Set
the appropriate values in the admin page. The old season environment variables are
no longer read. The model, time-location, and live-monitor environment values seed
their database rows on first startup; later changes are made through the admin page.

`hltv_event_id_list` is maintained automatically. Every 30 minutes the watcher asks
5EPlay for events in a bounded window (30 days back, 30 days ahead), keeps active
events whose `teams` include a followed team or whose grade is Major/S/S+, drops
finished events, and writes the resulting IDs back to this key. The lookback covers
events that started days ago but are still running. A manual edit is
overwritten by the next refresh. `hltv_followed_teams` defaults to
`["TYLOO", "LVG", "FaZe", "BC.Game"]`; matching ignores case and punctuation, so
`bcg` also matches `BC.Game`.

Only live events are polled for results (every 5 minutes); upcoming events are kept
in the list but are not requested until they start. For Major/S/S+ events every new
result is pushed. For other watched events, only matches involving a followed team
are pushed, while all results are still stored for `/更新比赛`, `major_hw`, and the
admin pages.

Request budget: discovery is one list request per 30 minutes (two pages at most).
The scheduler wakes once a minute, but each live event is polled only about once
per 5 minutes; new/restarted schedules are spread across 30–290 seconds and at most
two events are polled per minute, so results are not requested in one batch.
Requests are spaced by two seconds. A 403/429 response pauses polling for 30
minutes, and discovery failures back off from 5 to 60 minutes. The watcher never
requests a full event history page or an upcoming event.

Startup inserts missing configuration rows without overwriting saved values; adding
registered keys does not require a schema migration. Subsequent queries and refresh
jobs read the database. An already-running player refresh or AI request retains its
configuration snapshot until it finishes, so one operation cannot mix values.

## 部署与交付验收

部署文档统一维护在 [csbot-depoly](https://github.com/juruocjl/csbot-depoly) 根目录：

- [部署手册](https://github.com/juruocjl/csbot-depoly/blob/main/DEPLOY.md)
- [服务器服务清单](https://github.com/juruocjl/csbot-depoly/blob/main/SERVER_SERVICES.md)
- [本地交付验收](https://github.com/juruocjl/csbot-depoly/blob/main/DELIVERY_CHECK.md)
- [DSH/Mneme AI 架构、权限和发布](https://github.com/juruocjl/csbot-depoly/blob/main/AI_RUNTIME.md)
- [执行约束](https://github.com/juruocjl/csbot-depoly/blob/main/AGENTS.md)

在该部署仓库的子模块布局中，上述文件位于本目录的上一层；独立克隆请使用上面的仓库链接。

## 预览生产图片

无需同步生产图片到本地，直接启动按需加载的本地画廊：

```bash
uv run python tools/production_image_gallery.py
```

访问 <http://127.0.0.1:43123>。本地服务只读取生产 Nginx 的目录索引，网格使用生产端按需生成并缓存的 WebP 缩略图；只有点开大图时才加载原图，不会复制整个 `imgs/pic` 目录。
