# Major Stage Metadata

Use `generate_major_stage.py` when a new Swiss stage contains teams that advanced
from the previous stage.

The script carries forward the previous stage's live ratings by replaying that
stage's finished match results with the same model used by simulation:

- recent matches count more;
- upsets receive extra rating movement;
- clean BO3 wins receive a small extra boost;
- `valve` and `hltv` use their own K factors.

## Template

Create a next-stage template first. Teams already present in the previous stage
only need `seed` and `alias`; their `valve` and `hltv` values will be overwritten.
Newly added teams must include manually checked `valve` and `hltv` values.

```json
{
  "systems": {
    "valve": "lambda x: x",
    "hltv": "lambda x: x"
  },
  "teams": {
    "Advanced Team": {
      "seed": 1,
      "alias": []
    },
    "New Invite": {
      "seed": 2,
      "valve": 1900,
      "hltv": 420,
      "alias": ["New Invite Full Name"]
    }
  }
}
```

## From A Local Results File

The results file should be the same shape as `hltvresult{event_id}`:

```json
[
  ["Winner", "Loser", "2:0", "match_id"],
  ["Winner 2", "Loser 2", "1:0", "match_id"]
]
```

By default the script treats the list as newest-to-oldest, matching current
`hltvresult` storage. Add `--oldest-first` if the file is already chronological.

```powershell
cd C:\Users\cjlqwq\Documents\csbot\backend
uv run python .\scripts\generate_major_stage.py `
  --previous .\assets\2026-cologne-stage2.json `
  --template .\assets\2026-cologne-stage3.template.json `
  --results .\stage2-results.json `
  --output .\assets\2026-cologne-stage3.json
```

## From Server Storage

Run this on the server, where production `.env` and database access are
available:

```bash
cd /home/ubuntu/csbot
ENVIRONMENT=prod /home/ubuntu/.local/bin/uv run python scripts/generate_major_stage.py \
  --previous assets/2026-cologne-stage2.json \
  --template assets/2026-cologne-stage3.template.json \
  --from-storage 9029 \
  --output assets/2026-cologne-stage3.json
```

Then commit the generated asset and deploy.

## 2026 Singapore Major Stage Plan

5E event IDs for `2026-singapore` (also kept in `runtime_config.hltv_event_id_list`):

| Asset stage | 5E event id | Window | Notes |
| --- | --- | --- | --- |
| `2026-singapore-stage1` | `9303` | 2026-11-24 ~ 2026-11-27 | 16 teams, Swiss BO1 with BO3 at 2-x |
| `2026-singapore-stage2` | `9304` | 2026-11-28 ~ 2026-12-01 | 8 invites + 8 stage 1 advancers |
| `2026-singapore-stage3` | `8302` | 2026-12-02 ~ 2026-12-13 | 8 invites + 8 stage 2 advancers, all BO3 Swiss |
| `2026-singapore-playoffs` | `8302` | 2026-12-02 ~ 2026-12-13 | Uses the same event id as stage 3 |

Invite templates `assets/2026-singapore-stage2.invites.json` and
`assets/2026-singapore-stage3.invites.json` hold the 8 directly invited teams
(seeds `1..8`) plus their initial `valve`/`hltv` values. Fill the advancers into
seeds `9..16`, then run `generate_major_stage.py` to produce
`assets/2026-singapore-stage2.json` / `assets/2026-singapore-stage3.json`.
`2026-singapore-stage3.invites.json` already carries `"match_format": "bo3"`
because the whole stage 3 Swiss round is best-of-three.

Before a stage opens (0 finished matches), regenerate the initial matrix,
`result.txt` and the match-count 0 snapshot:

```bash
cd /home/ubuntu/csbot
ENVIRONMENT=prod /home/ubuntu/.local/bin/uv run python scripts/recalculate_major_stage_results.py \
  --max-match-count 0 --threads 2
```

All four configuration arguments (`--database`, `--major-name`, `--major-stage`,
`--event-id`) are optional now: when omitted the script calls `nonebot.init()`
itself and reads `CS_DATABASE`, `MAJOR_NAME`, `MAJOR_STAGE` and
`MAJOR_EVENT_ID` from the process environment/`.env.prod`, so no credential has
to appear on the command line. Passing any of them still overrides that single
value.

