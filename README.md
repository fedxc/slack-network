# slack-network

A self-maintaining graph of working relationships derived from Slack activity.
It maps who actually works with whom — from @mentions, thread replies, reactions,
DMs, and ambient co-presence — then computes influence (PageRank), bridges
(betweenness), and community clusters. First run bootstraps the graph; every later
run is a decayed delta patch.

The agent does the Slack I/O; the engine does the math. `network_ops.py` never
calls Slack — it only reads/writes JSON.

## Files

| File | What it is |
|------|-----------|
| `network_ops.py` | Graph compute engine — channel scoring, bootstrap, delta, query, validate. `--help` for the CLI. |
| `network_viz.html` | Self-contained interactive visualizer. Open it and drop a state JSON on the window. |
| `schema.md` | Full `network_state.json` (v2.0) schema + the visualizer contract. |
| `SKILL.md` | The agent playbook: Slack MCP tools, bootstrap/delta phases, reporting. |
| `demo_6k_interaction.py` | Runnable demo — fabricates a theorized 6,000-message interaction and drives the engine end-to-end. |
| `favicon.*`, `apple-touch-icon.png` | Site icons (also embedded inline in the visualizer). |

## Quick start

```bash
pip install networkx numpy scipy        # python-louvain optional (falls back to label propagation)

# See both headline features end-to-end on synthetic data:
python demo_6k_interaction.py           # writes artifacts to ./demo_out
```

Then open `network_viz.html` and drop `demo_out/bootstrap_state.json` onto it.

## The demo showcases

- **Learned-yield channel selection** (`score-channels --prior`) — observed
  results, not just metadata, drive which channels get crawled. A cryptically
  named channel that proves out (dense, reciprocated ties) rises; a great-by-name
  one-way status feed gains far less; an unproven channel sinks below everything
  that has earned its place.
- **Ambient co-presence** — same-channel/same-day presence creates a visible edge
  but never manufactures influence (it's excluded from centrality and discounted
  by channel size).

## Engine CLI

```bash
python network_ops.py --mode score-channels --channels channels.json --users users.json --output crawl_plan.json [--prior prev_state.json]
python network_ops.py --mode bootstrap      --input raw.json --users users.json [--channels channels.json] --output state.json
python network_ops.py --mode delta          --input delta.json --state prev.json --output new.json [--decay-halflife 30]
python network_ops.py --mode query          --state state.json --user1 U123 --user2 U456
python network_ops.py --mode validate       --state state.json
```

See `SKILL.md` for the full agent workflow and `schema.md` for the state format.
