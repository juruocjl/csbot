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
