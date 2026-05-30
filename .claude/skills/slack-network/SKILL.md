---
name: slack-network
description: >
  Daily Slack network analysis agent. Maps relationships between users based on message
  activity, shared channels, @mentions, thread co-participation, and reactions. First run
  performs full network bootstrap — discovers channels and users, scores channels to crawl
  the ones that actually carry working relationships, builds a weighted DIRECTED relationship
  graph, computes PageRank and betweenness centrality, and identifies community clusters.
  Every subsequent run performs a delta update: loads the previous snapshot from Google Drive,
  crawls only new activity since the last run timestamp, decays stale edges, recomputes
  affected metrics, and emits a concise change report. Produces a self-contained interactive
  HTML visualization (network_viz.html). Trigger on: "run network analysis", "analyze slack
  network", "update the graph", "who's connected to whom", "map team relationships", "daily
  network run", "/network", "bootstrap slack network", "slack graph delta". Also trigger
  proactively when the user asks anything about influence, collaboration patterns, team
  clusters, or communication health in Slack.
---

# Slack Network Analysis Skill

Builds and maintains a living graph of user relationships derived from Slack activity.
Persists state to Google Drive. First run = full bootstrap. All subsequent runs = delta patch.

All compute lives in `network_ops.py` (this directory). The script never calls Slack —
it only reads/writes JSON. The agent does the Slack I/O; the script does the math.

---

## Slack MCP Server

All Slack I/O goes through Slack's **official hosted MCP server**:

- **Endpoint:** `https://mcp.slack.com/mcp` (remote HTTP MCP — nothing to install).
- **Auth:** OAuth — connect once in your client ("Connect to Slack"). The server acts
  **as the authenticated user**, so the agent only sees channels, DMs, threads, and files
  that user belongs to. Private channels the user isn't in are invisible (handled by the
  403-skip path in Error Handling).

### Tools this skill uses

| Tool | Used in | Purpose |
|------|---------|---------|
| `slack_search_channels` | A1 | discover channels (metadata only) |
| `slack_search_public_and_private` | A1.5, B2 | find recently-active channels / the delta window |
| `slack_search_users` | A3 | enumerate workspace members |
| `slack_read_channel` | A4, B2 | read a channel's recent top-level messages |
| `slack_read_thread` | A4, B2 | read a thread's replies (the thread-reply signal) |
| `slack_read_user_profile` | A3 (optional) | backfill a user's title/profile |
| `slack_send_message` | Output (optional) | post the run report to a channel |
| `slack_create_canvas` / `slack_update_canvas` | Output (optional) | publish the report as a Slack canvas |

The server also exposes `slack_search_public` (public-only search) and `slack_read_canvas`,
which this skill doesn't need.

### Connector behavior to plan around

- **Cursor pagination.** Tools return **bounded pages** with a `cursor`, not unlimited
  result sets. The message caps in Configuration (100/channel bootstrap, 50/channel delta)
  may take **several chained calls** per channel — page until you hit the cap or run dry.
- **Search needs real queries.** Don't assume an empty query dumps the whole workspace; use
  explicit filters (`after:`, name/description terms) and paginate. Treat any one response
  as a page, not the universe.
- **Rate limits.** Search is the tightest tier — use it for *targeting* (which channels are
  alive), then spend channel/thread reads on the scorer-selected set. This budget discipline
  is the whole reason the scorer exists.

---

## Pre-Flight: Determine Run Mode

Before anything else, check for an existing state file in Google Drive.

```
Tool: Google Drive:search_files
Query: name = "slack_network_state.json"
```

- **File not found** → **BOOTSTRAP MODE** (go to Phase A)
- **File found** → load it, read `meta.last_run` → **DELTA MODE** (go to Phase B)
- **File found but `meta.run_count` = 0 or file is corrupt** → re-run bootstrap

Store the Drive file ID when found — you'll need it to update in place later.

---

## Phase A — Bootstrap (First Run)

### A1. Discover Channels (metadata only)

```
Tool: slack_search_channels
Query: "" (broad listing of accessible channels)
Limit: 200   ← per page; chain the returned cursor to cover the rest of the workspace
```

For each channel returned, record the metadata only (no message reads yet):
`channel_id`, `name`, `member_count`, `is_private`, `is_archived`, `last_message_ts`.
Write the list to `channels.json`.

> **Do NOT just take the top channels by member count.** The highest-membership
> channels are almost always `#general`, `#announcements`, HR, and social — they carry
> the least relationship signal and the most noise. Worse, every pair of people who post
> in a 500-member channel looks "co-present," which fabricates O(n²) phantom edges. Pick
> channels by *what they are*, not how many people are in them.

### A1.5. (Optional, 1 call) Harvest recently-active channels

One cheap search surfaces channels with activity in the crawl window, which is a strong
"this channel is alive" signal for the scorer:

```
Tool: slack_search_public_and_private
query: "after:<YYYY-MM-DD>"   ← e.g. 14 days ago
sort_by: timestamp
```

Collect the distinct `channel_id`s that appear and write them as a JSON array to
`recent.json` (e.g. `["C123","C456"]`). Skip this step if search isn't available — the
scorer degrades gracefully without it.

### A2. Score & select channels to crawl

```bash
python network_ops.py --mode score-channels \
  --channels channels.json \
  --users users.json \
  --recent recent.json \        # optional; omit if you skipped A1.5
  --channel-cap 60 \
  --output crawl_plan.json
```

The scorer ranks every channel on cheap metadata using a composite of:

| Signal | Effect |
|--------|--------|
| **Name pattern** | `proj-`, `team-`, `eng-`, `incident-`, `squad-`, `support-`, … → boost. `general`, `random`, `announcements`, `hr-`, `memes`, `*-bot`, `alerts` → penalty. `general`/`random`/`announcements` are hard-**vetoed**. |
| **Size band** | Peaks at ~3–25 members (a working group). Penalizes channels holding >35% / >60% of the workspace (broadcast). |
| **Recency** | Active in window (from `recent.json` or `last_message_ts`) → boost; stale → penalty. |
| **Private** | Small bonus (private channels skew toward real working groups). |

`crawl_plan.json` contains `{ ranked, crawl, workspace_size, channel_cap }`. **Crawl only
the `crawl` list** (already capped and veto-filtered). This is the single biggest lever on
graph quality — spend your rate-limit budget on channels that carry working relationships.

### A3. Enumerate Users

```
Tool: slack_search_users
Query: "" (broad listing of workspace members; paginate via the returned cursor)
```

For each user: record `user_id`, `display_name`, `real_name`, `title`. Exclude bots
(`is_bot`) and deactivated users. Write `users.json` as `{ user_id → { name, real_name, title } }`.
If search results omit a title you care about, backfill it with `slack_read_user_profile`.

(Run this before A2 if you want the scorer to know `workspace_size` precisely; it falls
back to `member_count` of the largest channel otherwise.)

### A4. Crawl Channel Activity

For each channel in `crawl_plan.json.crawl`:

```
Tool: slack_read_channel
channel_id: <channel_id>
limit: 100   ← most recent 100 messages per channel on bootstrap (page via cursor to reach it)
```

Extract **directed** interaction signals. Direction matters — "A mentions B" is not the
same as "B mentions A," and reciprocity is one of the most informative things in the graph.

| Signal | How to detect | Direction | Raw weight |
|--------|---------------|-----------|-----------|
| **Direct message** | Channel ID starts with `D` | from → to | 4.0 |
| **@mention** | `<@UXXXX>` in text | author → mentioned | 3.0 |
| **Thread reply** | `thread_ts` present, sender ≠ thread author | replier → author | 1.5 |
| **Reaction given** | `reactions` array on a message | reactor → author | 0.5 |
| **Co-presence** | Two users posting in same channel, same day | symmetric | 0.3 |

Emit one raw record per interaction (note `from` = actor, `to` = target):
```json
{ "from": "U123", "to": "U456", "signal": "mention", "weight": 3.0,
  "channel": "C789", "ts": 1718000000.0 }
```

> **Threads need a second call.** `slack_read_channel` returns top-level messages only.
> For any message that has replies (`reply_count > 0`), call `slack_read_thread`
> (`channel_id` + `thread_ts`) to enumerate the repliers, then emit
> `replier → thread_author` records (weight 1.5). Without this step the thread-reply
> signal is missing entirely. Count thread replies against the per-channel message budget.

Co-presence is **ambient context, not endorsement** — the engine keeps it on a separate
channel-size-discounted track and excludes it from centrality (see A5). Accumulate all
records into `raw_interactions.json`.

### A5. Compute the Graph

```bash
pip install networkx python-louvain --break-system-packages -q
```

```bash
python network_ops.py --mode bootstrap \
  --input raw_interactions.json \
  --users users.json \
  --channels channels.json \      # gives the engine channel sizes + names
  --output network_state.json
```

Passing `--channels` lets the engine (a) size-discount co-presence so big channels don't
dominate, and (b) label communities with human-readable channel names. What the engine
computes:

- A weighted graph with a **directed** core. Each edge stores `interaction_weight`
  (the four directed signals), `co_presence_weight` (ambient), directional weights
  `dir_uv_weight` / `dir_vu_weight`, and `reciprocity` (0 = one-way, 1 = perfectly mutual).
- **Centrality runs on `interaction_weight`, not raw weight** — so co-presence can't
  manufacture influence. PageRank, betweenness, eigenvector (undirected) plus
  `pagerank_directed` (who receives attention) and `in_/out_strength`.
- **Betweenness uses inverse-weight distances** (strong tie = short path). This is the one
  thing the naive version got backwards.
- Deterministic **Louvain** communities (seeded + label-stabilized across runs so cluster
  IDs don't churn).
- A **bipartite user×channel affiliation projection** (`affiliation_top`) surfacing pairs
  who co-participate in the same working channels but rarely DM — latent teams.

See `schema.md` for the full structure.

### A5.5. Validate Before Persisting (always)

```bash
python network_ops.py --mode validate --state network_state.json
```

`bootstrap` and `delta` run this automatically and print any findings, but run it
explicitly whenever you hand the JSON to a human or to `network_viz.html`. It
checks the file against the **visualizer's contract** and surfaces *actionable*
problems instead of letting the frontend fail silently — e.g. edges that point at
user_ids missing from `nodes` (the visualizer drops them without a word),
community members that don't resolve, non-numeric metrics, or a non-2.0 schema.
Exit code is `0` when clean (warnings are non-blocking), `1` when there are errors
to resolve. See **Validating Before Handoff** below.

### A6. Persist State

```
Tool: Google Drive:create_file
filename: slack_network_state.json
content: <contents of network_state.json>
folder: "Slack Network Analysis"  ← create if missing
```

Note the returned file ID for future updates.

---

## Phase B — Delta Update (Subsequent Runs)

### B1. Load Previous State

```
Tool: Google Drive:download_file_content
file_id: <state_file_id>
```

Write to `prev_state.json`. Parse `meta.last_run` (ISO timestamp).

### B2. Crawl New Activity Only

Time-bounded search first, then targeted reads:

```
Tool: slack_search_public_and_private
query: "after:<YYYY-MM-DD>"   ← date derived from last_run
sort_by: timestamp
```

```
Tool: slack_read_channel
channel_id: <channel_id>
limit: 50   ← sufficient for a 24h delta (page via cursor if needed)
```

Crawl channels that appear in search results plus the persisted `crawl` set from bootstrap.
Skip quiet channels. As in A4, follow up any message with `reply_count > 0` using
`slack_read_thread` to capture new thread replies. Accumulate new signals into
`delta_interactions.json` (same record format as A4).

### B3. Run Delta Computation

```bash
python network_ops.py --mode delta \
  --input delta_interactions.json \
  --state prev_state.json \
  --channels channels.json \
  --output new_state.json \
  --decay-halflife 30
```

The script will:
1. Apply time-decay to **every** weight component (30-day half-life by default).
2. Merge new directed interactions, updating per-signal counts, directional weights, and reciprocity.
3. Recompute all centralities on the corrected interaction-weight graph.
4. Re-run community detection and **match clusters to the previous run** (Jaccard) so IDs stay stable; record renames/merges/splits.
5. Populate `meta.delta_summary`: `new_edges`, `strengthened_edges`, `cold_edges` (decayed below the floor and dropped), `new_nodes`, `community_changes`.
6. Auto-run `validate` on the output and print any findings (see **Validating Before Handoff**).

### B4. Update Persisted State

```
Tool: Google Drive:copy_file        ← archive previous as backup
  new_name: slack_network_state_YYYY-MM-DD.json

Tool: Google Drive:create_file (overwrite)
  filename: slack_network_state.json
  content: <contents of new_state.json>
```

---

## Output Report

After either phase, emit a structured report. Format as a Slack message draft if the user
has a preferred reporting channel; otherwise output inline. To deliver it in Slack, post
with `slack_send_message`, or publish the full snapshot as a Slack **canvas** via
`slack_create_canvas` (and update it in place on later runs with `slack_update_canvas`).

### Bootstrap Report Template

```
🗺️  SLACK NETWORK — INITIAL SNAPSHOT
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
📊 Graph stats
   Nodes (users):      XXX
   Edges (pairs):      XXX
   Channels crawled:   XX  (of XX discovered; YY vetoed as broadcast/social)
   Messages processed: XXXXX

🏆 Top 5 by PageRank (most influential connectors)
   1. @alice    PR=0.082   cluster: Engineering
   ...

🌉 Top 5 by Betweenness (bridges between groups)
   1. @frank    BT=0.21    connects: Engineering ↔ Growth
   ...

🤝 Notable latent affiliations (co-work, rarely DM)
   @bob ↔ @grace   share: #proj-atlas, #incident-room

🏘️  Communities detected: N clusters
   Cluster 0 (#proj-atlas, #eng-platform):  12 users, core: @alice, @bob
   ...

📅 Snapshot: YYYY-MM-DD HH:MM UTC
```

### Delta Report Template

```
🔄  SLACK NETWORK — DELTA UPDATE  (+24h)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
📊 Activity window: YYYY-MM-DD → YYYY-MM-DD
   New messages: XXX
   New edges: XX  |  Strengthened: XX  |  Gone cold: XX

🔥 Strongest new connections
   @alice → @frank   w=12.4  (3 @mentions + 8 thread replies, ↔62% mutual)

📈 Relationships strengthened (>20% interaction-weight increase)
   @bob ↔ @diana    +34%

📉 Relationships gone cold (decayed below floor, dropped)
   @charlie ↔ @grace   last interaction 18 days ago

🏘️  Community shifts
   <from meta.delta_summary.community_changes — renamed / merged / split / new>
   e.g. "Cluster 0 reshuffled (Jaccard 0.22 vs last run)"  — or  "No structural changes"

📅 Updated: YYYY-MM-DD HH:MM UTC  |  Run #N
```

Pull the cold/strengthened/community lines straight from `meta.delta_summary`; do not
recompute them by hand.

---

## Two-User Relationship Query

If the user asks about a specific pair `(u1, u2)`, load (or refresh) state, then:

```bash
python network_ops.py --mode query \
  --state network_state.json \
  --user1 <user_id_or_name> \
  --user2 <user_id_or_name>
```

Output: direct edge weight + direction split + reciprocity, weighted shortest path
(inverse-weight, so it follows strong ties), common neighbors, shared communities,
Jaccard similarity, interaction timeline.

---

## Visualizing the Graph

`network_state.json` is directly viewable in `network_viz.html` — a single self-contained
file (no CDN, no build step, no network access; safe to open inside a locked-down corporate
browser). Open it and drop the JSON on the window, or use the file picker / paste box. It
ships with an embedded sample so it's never empty.

Three views: **Force** (force-directed graph, node size by any centrality, color by
community), **Matrix** (community-ordered adjacency, cells tinted by reciprocity), and
**Clusters** (community cards + latent-affiliation pairs + last-delta summary). It reads
the v2.0 schema (directionality, reciprocity, affiliation) and falls back gracefully on
older v1 states. To share a snapshot, hand the user both `network_state.json` and
`network_viz.html`.

**Interacting with communities.** The bottom legend is a horizontal strip of bold
cluster pills. Hover a pill for a popover of derived insight (what unites the members,
internal vs. outward ties, mutuality, who bridges out). Click a pill to *isolate* that
community — its nodes light up and everything else greys out; click again (or click
empty space / press Esc) to release.

**Colors & theme.** Community colors come from a 10-hue curated palette that extends
automatically (golden-angle generation) to any cluster count, so 12–20+ clusters stay
visually distinct instead of wrapping/colliding. **Appearance → Cluster colors…** opens a
picker to override any cluster's color; overrides and the light/dark choice persist to
`localStorage` (key `slack-network-viz`).

---

## Validating Before Handoff

```bash
python network_ops.py --mode validate --state <state.json>
```

`network_ops.validate_state(state)` checks a state file against everything
`network_viz.html` assumes when it loads JSON, and returns
`{ ok, errors, warnings, stats }` (the CLI prints it and exits non-zero on errors).
It is the parser/validator to call **before handing JSON to the visualizer or the
user**. It catches the failure modes the frontend hides:

- **errors** (block handoff — the frontend loses or misrenders data):
  `nodes`/`edges`/`communities` of the wrong container type; edges whose `u`/`v`
  reference user_ids absent from `nodes` (silently dropped); community members that
  don't resolve to a node; missing `u`/`v` on an edge.
- **warnings** (non-blocking, worth knowing): non-2.0 schema (compat mode), missing
  `meta`, non-numeric metrics (coerced to 0), nodes referencing a community with no
  card, `affiliation_top` pairs pointing at unknown users.

`bootstrap` and `delta` call it automatically after writing and print any findings.

---

## Configuration

| Parameter | Default | Override |
|-----------|---------|----------|
| Bootstrap channel cap | 60 | `--channel-cap N` |
| Decay half-life | 30 days | `--decay-halflife N` |
| Edge weight floor (prune) | 0.5 | `--min-weight N` |
| Bootstrap messages/channel | 100 | agent-side (Slack `limit`, paged via cursor) |
| Delta messages/channel | 50 | agent-side (Slack `limit`, paged via cursor) |
| Slack MCP endpoint | `https://mcp.slack.com/mcp` | fixed (OAuth, acts as the connected user) |
| Drive folder name | "Slack Network Analysis" | edit in this file |
| State filename | `slack_network_state.json` | fixed |
| Backup prefix | `slack_network_state_` | fixed |

Message caps are enforced by the agent when calling Slack (the `limit` argument), not by
the script — the script only ever sees the JSON you hand it.

---

## Error Handling

| Situation | Action |
|-----------|--------|
| Slack MCP not connected / OAuth expired | Prompt the user to (re)connect Slack at `https://mcp.slack.com/mcp`; don't fabricate data |
| Slack rate limit hit (429) | Back off and retry; if persistent, lower `--channel-cap` or the `limit` and note partial coverage in the report |
| Drive file not found | Re-run bootstrap, don't error |
| Channel read fails (403) | Skip channel (user not a member / no access), log name in report |
| No messages in delta window | Emit "quiet period" report, still apply decay |
| User in edges but not in users map | Add as anonymous node `{user_id, name: "unknown"}` |
| networkx not available | `pip install networkx python-louvain --break-system-packages` |
| `score-channels` has no `recent.json` | Fine — it scores on name + size + `last_message_ts` |
| State file corrupt/unparseable | Rename `.bak`, re-run bootstrap |
| Old v1 state file | `delta`/`query`/viz all read it; it upgrades to v2.0 on next write |
| `validate` reports errors | Resolve them before handoff — they mark data the visualizer will silently drop/misrender |

---

## Files in this skill

- `network_ops.py` — graph compute engine (all algorithms + `validate` mode; `--help` for CLI).
- `schema.md` — full JSON schema for `network_state.json` (v2.0) + the visualizer contract.
- `network_viz.html` — self-contained interactive visualizer for the state JSON.
- `favicon.svg` / `favicon.png` / `apple-touch-icon.png` — site icons (also embedded
  inline in `network_viz.html`, so the HTML stays self-contained). Same visual language as
  the masthead glyph: an amber tile with an inner ring and a hard offset shadow.

Read `schema.md` before interpreting or modifying state files. Read the `network_ops.py`
header for CLI usage and algorithm details.
