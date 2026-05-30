# network_state.json — Schema Reference

**Version**: 2.0
**Produced by**: `network_ops.py --mode bootstrap|delta`

v2.0 adds edge **directionality** + **reciprocity**, separates ambient **co-presence** from
real **interaction** weight, adds **directed PageRank** / in-/out-strength, a bipartite
**affiliation** projection, and deterministic, label-stable communities. v1.x state files
still load (`delta`, `query`, and the visualizer upgrade them on read).

---

## Top-level structure

```json
{
  "schema_version": "2.0",
  "meta":           { ... },
  "nodes":          { "USER_ID": { ... } },
  "edges":          { "UID1:UID2": { ... } },
  "communities":    { "COMMUNITY_ID": { ... } },
  "affiliation_top":[ { ... } ],
  "channel_names":  { "CHANNEL_ID": "channel-name" }
}
```

---

## `meta`

| Field | Type | Description |
|-------|------|-------------|
| `run_count` | int | How many times the agent has run (1 = first run) |
| `first_run` | ISO string | Timestamp of first bootstrap |
| `last_run` | ISO string | Timestamp of most recent run |
| `last_run_ts` | float | Unix timestamp of most recent run (used for decay) |
| `nodes_count` | int | Total nodes at last write |
| `edges_count` | int | Total edges at last write |
| `messages_processed` | int | Cumulative messages consumed across all runs |
| `algo` | object | How the numbers were produced (see below) |
| `delta_summary` | object | Empty `{}` on bootstrap; populated on delta runs |

### `meta.algo`

```json
{
  "schema_version": "2.0",
  "centrality_weight": "interaction_weight",
  "betweenness_distance": "1/interaction_weight",
  "community_method": "louvain",
  "rng_seed": 1312,
  "decay_halflife_days": 30.0
}
```

Records the two correctness-critical choices: centrality is computed on
`interaction_weight` (co-presence excluded), and betweenness treats **1/weight** as
distance (a strong tie is a *short* hop). `rng_seed` makes Louvain + approximate
betweenness deterministic run-to-run.

### `meta.delta_summary` (delta runs only)

```json
{
  "new_edges":          [ { "u": "U123", "v": "U456", "weight": 3.06 } ],
  "strengthened_edges": [ { "u": "U123", "v": "U789",
                            "old_weight": 1.73, "new_weight": 4.73, "pct_change": 173.9 } ],
  "weakened_edges":     [ { "u": "...", "v": "...", "old_weight": 0, "new_weight": 0, "pct_change": 0 } ],
  "cold_edges":         [ { "u": "U123", "v": "U999", "last_days": 7.4 } ],
  "new_nodes":          ["U999"],
  "community_changes":  { "renamed": [ { "id": 0, "jaccard": 0.22 } ],
                          "merged": [], "split": [], "new": [] }
}
```

- `cold_edges` — edges that decayed below the prune floor this run and were removed.
- `community_changes` — clusters matched to the previous run by Jaccard overlap.
  `renamed` = same cluster, drifted membership; `merged`/`split`/`new` describe topology
  changes. Stable IDs mean a report can say "Cluster 0 grew" instead of inventing new numbers.

---

## `nodes`

Keyed by Slack `user_id`.

```json
"U123456": {
  "name":             "alice",
  "real_name":        "Alice Smith",
  "title":            "Senior Engineer",
  "community_id":     2,
  "degree":           18,
  "weighted_degree":  234.5,
  "in_strength":      120.4,
  "out_strength":     98.1,
  "pagerank":         0.04821,
  "pagerank_directed":0.05203,
  "betweenness":      0.11203,
  "eigenvector":      0.08841
}
```

| Field | Description |
|-------|-------------|
| `community_id` | Integer cluster ID (stable across runs) |
| `degree` | Distinct neighbours |
| `weighted_degree` | Sum of incident **total** edge weights |
| `in_strength` / `out_strength` | Directed interaction weight received / sent |
| `pagerank` | Undirected PageRank (α=0.85) on `interaction_weight`. Influence |
| `pagerank_directed` | PageRank on the directed flow graph. High = receives attention |
| `betweenness` | Betweenness on inverse-weight distances. High = bridge/gatekeeper |
| `eigenvector` | Connected-to-the-well-connected |

---

## `edges`

Keyed `"USMALLER:ULARGER"` (lexicographic; `u` is always the smaller ID).

```json
"U123456:U789012": {
  "u": "U123456",
  "v": "U789012",
  "weight":              23.5,
  "interaction_weight":  21.0,
  "co_presence_weight":  2.5,
  "affiliation_weight":  0.42,
  "dir_uv_weight":       13.0,
  "dir_vu_weight":       8.0,
  "reciprocity":         0.76,
  "dm_count":            5,
  "mention_count":       8,
  "thread_reply_count":  12,
  "reaction_count":      3,
  "co_presence_count":   40,
  "shared_channels":     ["C001", "C002"],
  "last_interaction_ts": 1718500000.0,
  "first_interaction_ts":1700000000.0
}
```

| Field | Description |
|-------|-------------|
| `weight` | Total = `interaction_weight` + `co_presence_weight`. For display/degree only |
| `interaction_weight` | Directed signals only (DM/mention/reply/reaction). **Centrality uses this** |
| `co_presence_weight` | Ambient same-channel-same-day, **size-discounted**. Excluded from centrality |
| `affiliation_weight` | Bipartite user×channel co-participation (Newman 1/(k−1)). Bootstrap only |
| `dir_uv_weight` | Interaction weight flowing u→v |
| `dir_vu_weight` | Interaction weight flowing v→u |
| `reciprocity` | `1 − |uv − vu| / (uv + vu)`. 0 = one-way, 1 = perfectly mutual |
| `*_count` | Raw event tallies per signal |
| `shared_channels` | Channel IDs where the pair co-occurred |

### Weight formulae

```
interaction_weight = 4.0*dm + 3.0*mention + 1.5*thread_reply + 0.5*reaction   (directed)
co_presence_weight = Σ 0.3 / log2(channel_size + 1)   over co-presence events  (size-discounted)
weight             = interaction_weight + co_presence_weight

# every delta run, applied to all weight components:
decayed = w * 2^(-elapsed_days / halflife)

# edge pruned (and reported in cold_edges) if:
interaction_weight + co_presence_weight < min_weight   (default 0.5)
```

Centrality (`pagerank`, `betweenness`, `eigenvector`, `pagerank_directed`) is computed on
`interaction_weight`, so a 500-person broadcast channel cannot manufacture influence via
co-presence.

---

## `communities`

Keyed by community ID (string). IDs are stable across runs (label-matched by Jaccard).

```json
"2": {
  "size":              12,
  "members":           ["U123", "U456", ...],
  "core_members":      ["U123", "U456", "U789"],
  "top_channels":      ["C001", "C003"],
  "top_channel_names": ["proj-atlas", "eng-platform"]
}
```

| Field | Description |
|-------|-------------|
| `core_members` | Top 3 members by within-cluster PageRank |
| `top_channels` | Channel IDs most shared among intra-cluster edges |
| `top_channel_names` | Human-readable names (present when `--channels` was supplied) |

---

## `affiliation_top`

Bipartite **user×channel** projection — pairs who participate in the same working channels.
Surfaces latent teams: people who clearly work together but may rarely DM directly.
Computed at **bootstrap** (needs the participation matrix); empty `[]` on delta runs.

```json
[
  { "u": "U123", "v": "U456", "score": 0.438, "has_interaction": true }
]
```

| Field | Description |
|-------|-------------|
| `score` | Newman-weighted co-affiliation (rare-channel co-membership counts more) |
| `has_interaction` | Whether the pair also has a direct interaction edge. `false` = purely latent |

---

## `channel_names`

`{ channel_id → name }`, present when `--channels` was supplied. Used for readable
community labels and reporting.

---

## File lifecycle

```
First run:        Drive create  "slack_network_state.json"
Subsequent runs:  Drive copy    "slack_network_state.json" → "slack_network_state_YYYY-MM-DD.json"
                  Drive update  "slack_network_state.json"   (overwrite with new state)
```

Only the current file feeds delta computation; dated copies are for audit/rollback.

---

## Compatibility notes

- `schema_version` increments on breaking changes.
- v1.x states load: `state_to_graph` synthesizes `interaction_weight` from `weight` when
  the split isn't present, so `delta`, `query`, and `network_viz.html` all work; the file
  is rewritten as v2.0 on the next save.
- To reset, delete/rename the Drive file and re-run bootstrap.

---

## Validation & the visualizer contract

`network_ops.py --mode validate --state <file>` (function: `validate_state`) checks a
state file against the assumptions `network_viz.html`'s `normalize()` makes on load, and
returns `{ ok, errors, warnings, stats }`. Run it before handing JSON to the visualizer or
a human; `bootstrap`/`delta` also run it automatically. It exists because the frontend
fails *quietly* — it does not throw on bad data, it drops it.

**Contract the frontend relies on (and `validate` enforces):**

| Assumption | If violated, the visualizer… | Severity |
|------------|------------------------------|----------|
| `nodes` is an object keyed by `user_id` | renders an empty graph | error |
| `edges` is an object (or array) of edge objects, each with `u` & `v` | drops the malformed edge | error |
| every edge `u`/`v` exists in `nodes` (lookup is by field, **not** by the `"u:v"` key) | **silently drops the edge** | error |
| every `communities[*].members` id exists in `nodes` | omits the member; cluster card shrinks | error |
| node centrality fields are numeric | coerces to `0` (`a.pagerank||0`) | warning |
| every `node.community_id` has a `communities` entry | no legend/cluster card (color still renders via the palette) | warning |
| `affiliation_top[*].u/v` exist in `nodes` | skips the pair in the Clusters view | warning |
| `schema_version == "2.0"` | runs in compat mode (synthesizes interaction/directionality) | warning |
| `meta` present | header run-info / counts show `—` | warning |

`stats` reports `schema_version`, `nodes`, `edges`, `communities`, `affiliation_top`,
`dangling_edges`, and `orphan_community_members` for a quick shape check. Exit code is `0`
when there are no errors (warnings don't block), `1` otherwise.
