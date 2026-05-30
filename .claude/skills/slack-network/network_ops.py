"""
network_ops.py — Slack Network Graph Computation Engine
=========================================================
Standalone script. Reads/writes JSON only. No Slack API calls here.

CLI Modes
---------
  score-channels  Rank/filter channels for crawling (cheap; metadata + learned yield prior)
  bootstrap       Build a fresh graph from raw interaction records
  delta           Merge new interactions into existing state + apply decay
  query           Analyze relationship between two specific users
  report          Print a summary of an existing state file
  validate        Check a state file against the visualizer contract (pre-handoff)

Usage
-----
  python network_ops.py --mode score-channels --channels channels.json --users users.json --output crawl_plan.json [--channel-cap 60] [--recent recent.json] [--prior prev_state.json]
  python network_ops.py --mode bootstrap --input raw.json --users users.json [--channels channels.json] --output state.json
  python network_ops.py --mode delta     --input delta.json --state prev.json --output new.json [--decay-halflife 30]
  python network_ops.py --mode query     --state state.json --user1 U123 --user2 U456
  python network_ops.py --mode report    --state state.json
  python network_ops.py --mode validate  --state state.json

Input: raw_interactions.json
-----------------------------
List of DIRECTED interaction records (from = actor, to = target):
[
  { "from": "U123", "to": "U456", "signal": "mention", "weight": 3.0,
    "channel": "C789", "ts": 1718000000.0 },
  ...
]

Signal types, base weights, and directionality:
  dm              4.0   directed   (DM in a D-prefixed channel)
  mention         3.0   directed   (from @mentions to)
  thread_reply    1.5   directed   (from replies in to's thread)
  reaction        0.5   directed   (from reacts to to's message)
  co_presence     0.3   symmetric  (both posted in same channel/day — AMBIENT, not endorsement)

Only the four directed signals contribute to `interaction_weight`, which is what
centrality is computed on. `co_presence` contributes to total `weight` only.
This stops broadcast channels from manufacturing influence.

Input: users.json
-----------------
{ "U123": { "name": "alice", "real_name": "Alice Smith", "title": "Engineer" }, ... }

Input: channels.json  (optional but recommended)
-------------------------------------------------
[ { "channel_id": "C1", "name": "proj-atlas", "member_count": 9,
    "is_private": false, "is_archived": false,
    "last_message_ts": 1718000000.0 }, ... ]
Used for (a) channel scoring and (b) size-aware co-presence down-weighting
and (c) human-readable channel names in the output.

Output: network_state.json
---------------------------
See schema.md for the full v2.0 structure.

Algorithm Notes
---------------
  interaction_weight : sum of directed-signal weights between u and v (no co_presence)
  weight             : interaction_weight + co_presence weight (display/total)
  reciprocity        : 1 - |out_uv - out_vu| / (out_uv + out_vu)  in [0,1]
  Time decay         : W_decayed = W * 2^(-days_since_last_interaction / halflife)
  PageRank           : weighted, on interaction_weight; directed variant on flows
  Betweenness        : weighted by DISTANCE = 1/interaction_weight (strong tie = close),
                       approximate (k pivots, seeded) for graphs > 150 nodes
  Communities        : Louvain (seeded, deterministic); label-propagation fallback.
                       IDs are matched to the previous run by membership overlap so
                       cluster identity is stable across days.
  Affiliation        : bipartite user x channel co-participation, Newman-weighted
                       (1 / (channel_size - 1)) — surfaces latent teams independent
                       of direct messaging.
"""

import argparse
import json
import math
import re
import sys
import time
from collections import defaultdict, Counter
from datetime import datetime, timezone

try:
    import networkx as nx
except ImportError:
    print("ERROR: networkx not installed. Run: pip install networkx --break-system-packages")
    sys.exit(1)

try:
    import community as community_louvain
    HAS_LOUVAIN = True
except ImportError:
    HAS_LOUVAIN = False


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SCHEMA_VERSION = "2.0"
DEFAULT_DECAY_HALFLIFE = 30      # days
DEFAULT_MIN_WEIGHT = 0.5         # prune edges below this (on total weight)
DEFAULT_PAGERANK_ALPHA = 0.85
DEFAULT_BETWEENNESS_K = 64       # pivot samples for approximation
BETWEENNESS_EXACT_LIMIT = 150    # exact below this many nodes
RNG_SEED = 1312                  # determinism: same graph -> same communities/betweenness

DIRECTED_SIGNALS = {"dm", "mention", "thread_reply", "reaction"}
BASE_WEIGHTS = {"dm": 4.0, "mention": 3.0, "thread_reply": 1.5, "reaction": 0.5, "co_presence": 0.3}

WORK_PATTERNS = [
    r"^proj[-_]", r"^team[-_]", r"^eng[-_]", r"^squad[-_]", r"^pod[-_]",
    r"^wg[-_]", r"^working[-_]", r"^oncall", r"^on[-_]call", r"^incident",
    r"^inc[-_]", r"^launch[-_]", r"^ship[-_]", r"^design[-_]", r"^data[-_]",
    r"^ml[-_]", r"^infra[-_]", r"^sre[-_]", r"^ops[-_]", r"^sec[-_]",
    r"^prod[-_]", r"^dev[-_]", r"^feat[-_]", r"^bug[-_]", r"^triage",
    r"^standup", r"^sync[-_]", r"^review[-_]", r"^rfc[-_]", r"^sales[-_]",
    r"^support[-_]", r"^cs[-_]", r"^cust", r"^client[-_]", r"^acct[-_]",
]
BROADCAST_PATTERNS = [
    r"^general$", r"^random$", r"^announce", r"^company[-_]?wide", r"^all[-_]",
    r"^everyone", r"^town[-_]?hall", r"^hr[-_]?", r"^people[-_]?ops", r"^benefits",
    r"^payroll", r"^watercooler", r"^water[-_]cooler", r"^social", r"^off[-_]?topic",
    r"^lunch", r"^food", r"^pets", r"^memes", r"^fun", r"^kudos", r"^shoutout",
    r"^birthday", r"^celebrat", r"^newsletter", r"^digest", r"^bot[-_]", r"[-_]bots?$",
    r"^notif", r"^alerts?$", r"^feed[-_]", r"^rss",
]
_WORK_RE = re.compile("|".join(WORK_PATTERNS), re.I)
_BROADCAST_RE = re.compile("|".join(BROADCAST_PATTERNS), re.I)

# Looser, unanchored matches for channel topic/purpose text and cryptically-named
# channels (e.g. "c-ops-7" whose topic says "Project Atlas working group"). These are
# weighted below the anchored name patterns above so they only nudge, never dominate.
_WORK_LOOSE_RE = re.compile(
    r"\b(project|team|engineering|squad|incident|on[-_ ]?call|launch|design|"
    r"platform|backend|frontend|sprint|roadmap|standup|retro|planning|triage|"
    r"support|customer|client|deploy|release|rollout|working group|task ?force)\b", re.I)
_BROADCAST_LOOSE_RE = re.compile(
    r"\b(announce|company[-_ ]?wide|town[-_ ]?hall|all[-_ ]?hands|newsletter|"
    r"water[-_ ]?cooler|watercooler|social|off[-_ ]?topic)\b", re.I)


# ===========================================================================
# CHANNEL SCORING  (cheap, metadata-only — runs BEFORE any message crawl)
# ===========================================================================

def score_channels(channels, workspace_size, channel_cap=60, recently_active_ids=None,
                   prior_stats=None):
    recently_active_ids = set(recently_active_ids or [])
    now = time.time()
    workspace_size = max(workspace_size, 1)
    scored = []

    for ch in channels:
        if ch.get("is_archived"):
            continue
        cid = ch.get("channel_id") or ch.get("id")
        name = (ch.get("name") or "").strip()
        members = int(ch.get("member_count") or ch.get("num_members") or 0)
        reasons = []
        score = 0.0
        vetoed = False

        topic = (ch.get("topic") or "").strip()
        purpose = (ch.get("purpose") or "").strip()
        text = f"{name} {topic} {purpose}"

        if _BROADCAST_RE.search(name):
            score -= 6.0
            reasons.append("broadcast/social name (-6)")
            if re.match(r"^(general|random|announcements?)$", name, re.I):
                vetoed = True
                reasons.append("org-wide channel (veto)")
        if _WORK_RE.search(name):
            score += 4.0
            reasons.append("work-channel name (+4)")
        elif _WORK_LOOSE_RE.search(text):
            score += 2.0
            reasons.append("work topic/purpose match (+2)")
        if _BROADCAST_LOOSE_RE.search(f"{topic} {purpose}"):
            score -= 3.0
            reasons.append("broadcast topic/purpose match (-3)")

        frac = members / workspace_size
        if members < 3:
            score -= 3.0
            reasons.append(f"near-empty ({members} members, -3)")
        elif members <= 25:
            score += 3.0
            reasons.append(f"small-team size ({members}, +3)")
        elif members <= 60:
            score += 1.0
            reasons.append(f"mid size ({members}, +1)")
        if frac >= 0.6:
            score -= 6.0
            vetoed = True            # org-wide by membership → phantom-edge factory, hard veto
            reasons.append(f"~{frac:.0%} of workspace — org-wide (veto)")
        elif frac >= 0.35:
            score -= 3.0
            reasons.append(f"~{frac:.0%} of workspace (-3)")

        # Liveness is capped at +3 total (recent-search and last-message are the same fact),
        # so an active broadcast channel can't out-score its name/size penalties.
        last_ts = ch.get("last_message_ts") or ch.get("latest_ts") or 0.0
        recency_bonus = 0.0
        if cid in recently_active_ids:
            recency_bonus = 3.0
            reasons.append("active in recent search (+3)")
        if last_ts:
            days = (now - float(last_ts)) / 86400.0
            if days <= 7:
                recency_bonus = max(recency_bonus, 3.0); reasons.append("active <=7d (+3)")
            elif days <= 30:
                recency_bonus = max(recency_bonus, 1.0); reasons.append("active <=30d (+1)")
            elif days > 90:
                score -= 2.0; reasons.append("stale >90d (-2)")
        score += recency_bonus

        if ch.get("is_private"):
            score += 0.5
            reasons.append("private (+0.5)")

        # Learned prior: reward channels that actually produced relationship signal on a
        # previous run, penalize ones we crawled but that yielded nothing. This is the
        # feedback loop that lets observed yield, not just metadata, drive selection.
        st = (prior_stats or {}).get(cid)
        if st is not None:
            p = float(st.get("ema_pairs", st.get("pairs", 0)) or 0)
            if p > 0:
                bonus = min(5.0, 1.6 * math.log2(p + 1))
                score += bonus
                reasons.append(f"prior yield ~{p:.0f} pairs (+{bonus:.1f})")
                pr = st.get("pairs", 0) or 0
                rr = (st.get("reciprocal_pairs", 0) / pr) if pr else 0.0
                if rr > 0:
                    score += 2.0 * rr
                    reasons.append(f"prior reciprocity {rr:.0%} (+{2.0 * rr:.1f})")
            else:
                score -= 2.0
                reasons.append("crawled before, no signal (-2)")

        scored.append({
            "channel_id": cid, "name": name, "member_count": members,
            "is_private": bool(ch.get("is_private")), "score": round(score, 2),
            "vetoed": vetoed, "reasons": reasons,
        })

    scored.sort(key=lambda c: (-c["score"], c["member_count"]))
    crawl = [c["channel_id"] for c in scored if not c["vetoed"]][:channel_cap]
    return {"ranked": scored, "crawl": crawl,
            "workspace_size": workspace_size, "channel_cap": channel_cap}


# ===========================================================================
# GRAPH BUILDING
# ===========================================================================

def _empty_edge():
    return {
        "weight": 0.0, "interaction_weight": 0.0, "co_presence_weight": 0.0,
        "dir_uv_weight": 0.0, "dir_vu_weight": 0.0,
        "dm_count": 0, "mention_count": 0, "thread_reply_count": 0,
        "reaction_count": 0, "co_presence_count": 0,
        "shared_channels": set(),
        "last_interaction_ts": 0.0, "first_interaction_ts": float("inf"),
    }


def build_graph(raw_interactions, users, channel_sizes=None):
    channel_sizes = channel_sizes or {}
    G = nx.Graph()
    for uid, info in users.items():
        G.add_node(uid, **info)

    edge_data = defaultdict(_empty_edge)
    channel_participants = defaultdict(set)

    for rec in raw_interactions:
        u, v = rec.get("from"), rec.get("to")
        if not u or not v or u == v:
            continue
        signal = rec.get("signal", "co_presence")
        w = rec.get("weight", BASE_WEIGHTS.get(signal, 0.3))
        ts = rec.get("ts", 0.0)
        ch = rec.get("channel", "")

        if signal == "co_presence" and ch and ch in channel_sizes:
            size = max(channel_sizes[ch], 2)
            w = w / math.log2(size + 1)

        a, b = sorted([u, v])
        e = edge_data[(a, b)]
        e["weight"] += w
        if signal in DIRECTED_SIGNALS:
            e["interaction_weight"] += w
            if u == a:
                e["dir_uv_weight"] += w
            else:
                e["dir_vu_weight"] += w
        else:
            e["co_presence_weight"] += w

        e["last_interaction_ts"] = max(e["last_interaction_ts"], ts)
        e["first_interaction_ts"] = min(e["first_interaction_ts"], ts)
        if ch:
            e["shared_channels"].add(ch)
            channel_participants[ch].add(u)
            channel_participants[ch].add(v)

        ck = f"{signal}_count"
        if ck in e:
            e[ck] += 1

        for node in (u, v):
            if node not in G:
                G.add_node(node, name=node, real_name="Unknown", title="")

    affiliation = _affiliation_weights(channel_participants, channel_sizes)

    for (a, b), data in edge_data.items():
        if data["weight"] < DEFAULT_MIN_WEIGHT:
            continue
        data["shared_channels"] = sorted(data["shared_channels"])
        if data["first_interaction_ts"] == float("inf"):
            data["first_interaction_ts"] = 0.0
        data["affiliation_weight"] = round(affiliation.get((a, b), 0.0), 4)
        data["reciprocity"] = _reciprocity(data["dir_uv_weight"], data["dir_vu_weight"])
        G.add_edge(a, b, **data)

    G.graph["affiliation"] = affiliation
    return G


def _reciprocity(a, b):
    s = a + b
    if s <= 0:
        return 0.0
    return round(1.0 - abs(a - b) / s, 3)


def _affiliation_weights(channel_participants, channel_sizes):
    aff = defaultdict(float)
    for ch, parts in channel_participants.items():
        parts = sorted(parts)
        k = len(parts)
        if k < 2:
            continue
        contrib = 1.0 / (k - 1)
        for i in range(k):
            for j in range(i + 1, k):
                aff[(parts[i], parts[j])] += contrib
    return aff


# ===========================================================================
# CENTRALITY
# ===========================================================================

def _distance_view(G, weight_attr="interaction_weight"):
    for u, v, d in G.edges(data=True):
        s = d.get(weight_attr) or d.get("weight", 0.0)
        d["_dist"] = 1.0 / (s + 1e-9)


def compute_centrality(G):
    if G.number_of_nodes() == 0:
        return {}
    metrics = {n: {"weighted_degree": 0.0, "degree": 0,
                   "in_strength": 0.0, "out_strength": 0.0} for n in G.nodes()}

    for u, v, d in G.edges(data=True):
        w = d.get("interaction_weight", d.get("weight", 0.0))
        metrics[u]["weighted_degree"] += w
        metrics[v]["weighted_degree"] += w
        metrics[u]["degree"] += 1
        metrics[v]["degree"] += 1
        a, b = (u, v) if u < v else (v, u)
        uv = d.get("dir_uv_weight", 0.0)
        vu = d.get("dir_vu_weight", 0.0)
        metrics[a]["out_strength"] += uv
        metrics[a]["in_strength"] += vu
        metrics[b]["out_strength"] += vu
        metrics[b]["in_strength"] += uv

    try:
        pr = nx.pagerank(G, alpha=DEFAULT_PAGERANK_ALPHA, weight="interaction_weight", max_iter=300)
    except Exception:
        try:
            pr = nx.pagerank(G, alpha=DEFAULT_PAGERANK_ALPHA, weight="weight", max_iter=300)
        except Exception:
            pr = {n: 0.0 for n in G.nodes()}
    for n, val in pr.items():
        metrics[n]["pagerank"] = round(val, 6)

    D = nx.DiGraph()
    D.add_nodes_from(G.nodes())
    for u, v, d in G.edges(data=True):
        a, b = (u, v) if u < v else (v, u)
        if d.get("dir_uv_weight", 0.0) > 0:
            D.add_edge(a, b, weight=d["dir_uv_weight"])
        if d.get("dir_vu_weight", 0.0) > 0:
            D.add_edge(b, a, weight=d["dir_vu_weight"])
    try:
        dpr = nx.pagerank(D, alpha=DEFAULT_PAGERANK_ALPHA, weight="weight", max_iter=300)
    except Exception:
        dpr = {n: 0.0 for n in G.nodes()}
    for n in G.nodes():
        metrics[n]["pagerank_directed"] = round(dpr.get(n, 0.0), 6)

    _distance_view(G)
    n = G.number_of_nodes()
    try:
        if n > BETWEENNESS_EXACT_LIMIT:
            bc = nx.betweenness_centrality(G, weight="_dist",
                                           k=min(DEFAULT_BETWEENNESS_K, n),
                                           normalized=True, seed=RNG_SEED)
        else:
            bc = nx.betweenness_centrality(G, weight="_dist", normalized=True)
    except Exception:
        bc = {nn: 0.0 for nn in G.nodes()}
    for nn, val in bc.items():
        metrics[nn]["betweenness"] = round(val, 6)

    ec = None
    for fn in (lambda: nx.eigenvector_centrality_numpy(G, weight="interaction_weight"),
               lambda: nx.eigenvector_centrality(G, weight="interaction_weight", max_iter=1000)):
        try:
            ec = fn(); break
        except Exception:
            continue
    if ec is None:
        ec = {nn: 0.0 for nn in G.nodes()}
    for nn in G.nodes():
        metrics[nn]["eigenvector"] = round(ec.get(nn, 0.0), 6)

    for nn in G.nodes():
        metrics[nn]["in_strength"] = round(metrics[nn]["in_strength"], 4)
        metrics[nn]["out_strength"] = round(metrics[nn]["out_strength"], 4)
    return metrics


# ===========================================================================
# COMMUNITY DETECTION
# ===========================================================================

def detect_communities(G):
    if G.number_of_nodes() == 0:
        return {}
    if not nx.is_connected(G):
        partition, next_id = {}, 0
        for comp in sorted(nx.connected_components(G), key=len, reverse=True):
            if len(comp) == 1:
                partition[next(iter(comp))] = next_id
                next_id += 1
                continue
            sub = _detect_on_subgraph(G.subgraph(comp).copy(), offset=next_id)
            partition.update(sub)
            if sub:
                next_id = max(sub.values()) + 1
        return partition
    return _detect_on_subgraph(G, offset=0)


def _detect_on_subgraph(G, offset=0):
    if HAS_LOUVAIN:
        try:
            part = community_louvain.best_partition(G, weight="interaction_weight",
                                                    random_state=RNG_SEED)
            return {n: c + offset for n, c in part.items()}
        except Exception:
            pass
    comms = nx.algorithms.community.label_propagation_communities(G)
    part = {}
    for cid, comm in enumerate(sorted(comms, key=lambda c: -len(c))):
        for n in comm:
            part[n] = cid + offset
    return part


def stabilize_labels(new_partition, prev_communities):
    if not prev_communities:
        return new_partition, {"renamed": [], "merged": [], "split": [], "new": []}

    prev_sets = {int(cid): set(c.get("members", [])) for cid, c in prev_communities.items()}
    new_sets = defaultdict(set)
    for node, cid in new_partition.items():
        new_sets[cid].add(node)

    mapping, changes = {}, {"renamed": [], "merged": [], "split": [], "new": []}
    pairs = []
    for ncid, nmembers in new_sets.items():
        for pcid, pmembers in prev_sets.items():
            inter = len(nmembers & pmembers)
            if inter:
                union = len(nmembers | pmembers)
                pairs.append((inter / union, ncid, pcid))
    pairs.sort(reverse=True)
    assigned_prev = set()
    for jac, ncid, pcid in pairs:
        if ncid in mapping or pcid in assigned_prev:
            continue
        mapping[ncid] = pcid
        assigned_prev.add(pcid)
        if jac < 0.5:
            changes["renamed"].append({"id": pcid, "jaccard": round(jac, 2)})

    next_free = (max(prev_sets) + 1) if prev_sets else 0
    for ncid in new_sets:
        if ncid not in mapping:
            mapping[ncid] = next_free
            changes["new"].append(next_free)
            next_free += 1

    remapped = {node: mapping[cid] for node, cid in new_partition.items()}
    return remapped, changes


def summarize_communities(G, partition, users, channel_names=None):
    channel_names = channel_names or {}
    communities = defaultdict(list)
    for node, cid in partition.items():
        communities[cid].append(node)
    try:
        pr = nx.pagerank(G, weight="interaction_weight", max_iter=300)
    except Exception:
        pr = {n: 0.0 for n in G.nodes()}

    result = {}
    for cid, members in communities.items():
        core = sorted(members, key=lambda m: -pr.get(m, 0.0))[:3]
        channel_counts = Counter()
        for u, v, data in G.subgraph(members).edges(data=True):
            for ch in data.get("shared_channels", []):
                channel_counts[ch] += 1
        top = [ch for ch, _ in channel_counts.most_common(3)]
        result[str(cid)] = {
            "size": len(members), "members": sorted(members), "core_members": core,
            "top_channels": top, "top_channel_names": [channel_names.get(c, c) for c in top],
        }
    return result


# ===========================================================================
# TIME DECAY
# ===========================================================================

def apply_decay(G, last_run_ts, halflife_days=DEFAULT_DECAY_HALFLIFE):
    now = time.time()
    remove, cold = [], []
    for u, v, d in G.edges(data=True):
        last_ts = d.get("last_interaction_ts", last_run_ts)
        days = (now - last_ts) / 86400.0
        factor = math.pow(2.0, -days / halflife_days)
        for k in ("weight", "interaction_weight", "co_presence_weight",
                  "dir_uv_weight", "dir_vu_weight"):
            if k in d:
                d[k] = round(d[k] * factor, 4)
        d["decay_factor_applied"] = round(factor, 4)
        if d["weight"] < DEFAULT_MIN_WEIGHT:
            remove.append((u, v))
            cold.append({"u": u, "v": v, "last_days": round(days, 1)})
    G.remove_edges_from(remove)
    return G, cold


# ===========================================================================
# DELTA MERGE
# ===========================================================================

def merge_delta(G_prev, new_interactions, users, channel_sizes=None):
    G_new = build_graph(new_interactions, users, channel_sizes=channel_sizes)
    delta = {"new_edges": [], "strengthened_edges": [], "weakened_edges": [],
             "new_nodes": [], "cold_edges": []}

    for node in G_new.nodes():
        if node not in G_prev:
            delta["new_nodes"].append(node)
            G_prev.add_node(node, **G_new.nodes[node])

    for u, v, nd in G_new.edges(data=True):
        if G_prev.has_edge(u, v):
            pd = G_prev[u][v]
            old = pd.get("weight", 0.0)
            for k in ("weight", "interaction_weight", "co_presence_weight",
                      "dir_uv_weight", "dir_vu_weight",
                      "dm_count", "mention_count", "thread_reply_count",
                      "reaction_count", "co_presence_count"):
                pd[k] = round(pd.get(k, 0) + nd.get(k, 0), 4)
            pd["shared_channels"] = sorted(set(pd.get("shared_channels", [])) |
                                           set(nd.get("shared_channels", [])))
            pd["last_interaction_ts"] = max(pd.get("last_interaction_ts", 0),
                                            nd.get("last_interaction_ts", 0))
            pd["reciprocity"] = _reciprocity(pd.get("dir_uv_weight", 0.0),
                                             pd.get("dir_vu_weight", 0.0))
            new = pd["weight"]
            pct = (new - old) / old if old > 0 else 1.0
            if pct >= 0.20:
                delta["strengthened_edges"].append(
                    {"u": u, "v": v, "old_weight": round(old, 2),
                     "new_weight": round(new, 2), "pct_change": round(pct * 100, 1)})
        else:
            G_prev.add_edge(u, v, **nd)
            delta["new_edges"].append({"u": u, "v": v, "weight": round(nd["weight"], 2)})

    return G_prev, delta


# ===========================================================================
# SERIALIZATION
# ===========================================================================

def channel_yield(raw_interactions):
    """Per-channel signal yield for a run (directed signals only, co-presence excluded).

    Feeds the scorer's learning loop: how many distinct interacting pairs a channel
    surfaced, and how many were reciprocated. Channels that produced real, mutual ties
    float up next run; channels we crawled but that stayed silent get penalized."""
    per = defaultdict(lambda: {"interactions": 0, "dirs": defaultdict(set)})
    for rec in raw_interactions:
        if rec.get("signal", "co_presence") not in DIRECTED_SIGNALS:
            continue
        u, v, ch = rec.get("from"), rec.get("to"), rec.get("channel", "")
        if not u or not v or u == v or not ch:
            continue
        d = per[ch]
        d["interactions"] += 1
        a, b = sorted([u, v])
        d["dirs"][(a, b)].add(u == a)        # records which direction(s) we've seen
    stats = {}
    for ch, d in per.items():
        pairs = len(d["dirs"])
        recip = sum(1 for seen in d["dirs"].values() if len(seen) == 2)
        stats[ch] = {"interactions": d["interactions"], "pairs": pairs,
                     "reciprocal_pairs": recip, "ema_pairs": float(pairs)}
    return stats


def merge_channel_stats(prev, new, alpha=0.5):
    """EMA-merge per-channel yield across runs so the prior tracks *recent* signal.

    A channel absent from this run (not crawled / went quiet) decays toward zero rather
    than keeping a stale high score forever."""
    prev = prev or {}
    new = new or {}
    merged = {}
    for ch in set(prev) | set(new):
        p, n = prev.get(ch), new.get(ch)
        prev_ema = float((p or {}).get("ema_pairs", (p or {}).get("pairs", 0.0)) or 0.0)
        if n:
            new_pairs = float(n.get("pairs", 0.0) or 0.0)
            ema = alpha * prev_ema + (1 - alpha) * new_pairs if p else new_pairs
            merged[ch] = {"interactions": n.get("interactions", 0),
                          "pairs": n.get("pairs", 0),
                          "reciprocal_pairs": n.get("reciprocal_pairs", 0),
                          "ema_pairs": round(ema, 2)}
        else:
            merged[ch] = {**p, "ema_pairs": round(alpha * prev_ema, 2)}
    return merged


def graph_to_state(G, users, meta, channel_names=None, prev_communities=None,
                   channel_stats=None):
    channel_names = channel_names or {}
    centrality = compute_centrality(G)
    raw_partition = detect_communities(G)
    partition, comm_changes = stabilize_labels(raw_partition, prev_communities)
    communities = summarize_communities(G, partition, users, channel_names)
    if prev_communities is not None:
        meta.setdefault("delta_summary", {})["community_changes"] = comm_changes

    nodes = {}
    for node, data in G.nodes(data=True):
        c = centrality.get(node, {})
        nodes[node] = {
            "name": data.get("name", node), "real_name": data.get("real_name", ""),
            "title": data.get("title", ""), "community_id": partition.get(node, -1),
            "degree": c.get("degree", 0),
            "weighted_degree": round(c.get("weighted_degree", 0.0), 4),
            "in_strength": c.get("in_strength", 0.0),
            "out_strength": c.get("out_strength", 0.0),
            "pagerank": c.get("pagerank", 0.0),
            "pagerank_directed": c.get("pagerank_directed", 0.0),
            "betweenness": c.get("betweenness", 0.0),
            "eigenvector": c.get("eigenvector", 0.0),
        }

    edges = {}
    for u, v, data in G.edges(data=True):
        a, b = (u, v) if u < v else (v, u)
        edges[f"{a}:{b}"] = {
            "u": a, "v": b,
            "weight": round(data.get("weight", 0.0), 4),
            "interaction_weight": round(data.get("interaction_weight", 0.0), 4),
            "co_presence_weight": round(data.get("co_presence_weight", 0.0), 4),
            "affiliation_weight": round(data.get("affiliation_weight", 0.0), 4),
            "dir_uv_weight": round(data.get("dir_uv_weight", 0.0), 4),
            "dir_vu_weight": round(data.get("dir_vu_weight", 0.0), 4),
            "reciprocity": data.get("reciprocity", 0.0),
            "dm_count": data.get("dm_count", 0),
            "mention_count": data.get("mention_count", 0),
            "thread_reply_count": data.get("thread_reply_count", 0),
            "reaction_count": data.get("reaction_count", 0),
            "co_presence_count": data.get("co_presence_count", 0),
            "shared_channels": data.get("shared_channels", []),
            "last_interaction_ts": data.get("last_interaction_ts", 0.0),
            "first_interaction_ts": data.get("first_interaction_ts", 0.0),
        }

    aff = G.graph.get("affiliation", {})
    top_aff = sorted(aff.items(), key=lambda kv: -kv[1])[:50]
    affiliation_top = [{"u": a, "v": b, "score": round(s, 4),
                        "has_interaction": G.has_edge(a, b)} for (a, b), s in top_aff]

    meta["algo"] = {
        "schema_version": SCHEMA_VERSION,
        "centrality_weight": "interaction_weight",
        "betweenness_distance": "1/interaction_weight",
        "community_method": "louvain" if HAS_LOUVAIN else "label_propagation",
        "rng_seed": RNG_SEED,
        "decay_halflife_days": meta.get("algo", {}).get("decay_halflife_days"),
    }
    return {
        "schema_version": SCHEMA_VERSION, "meta": meta, "nodes": nodes, "edges": edges,
        "communities": communities, "affiliation_top": affiliation_top,
        "channel_names": channel_names, "channel_stats": channel_stats or {},
    }


def state_to_graph(state):
    G = nx.Graph()
    users = {}
    for uid, data in state.get("nodes", {}).items():
        G.add_node(uid, **data)
        users[uid] = {k: data[k] for k in ("name", "real_name", "title") if k in data}
    for key, data in state.get("edges", {}).items():
        u, v = data["u"], data["v"]
        attrs = {k: val for k, val in data.items() if k not in ("u", "v")}
        attrs.setdefault("interaction_weight", attrs.get("weight", 0.0))
        G.add_edge(u, v, **attrs)
    return G, users


# ===========================================================================
# STATE VALIDATION  (pre-handoff contract check against network_viz.html)
# ===========================================================================
#
# network_viz.html's normalize() makes a number of structural assumptions that
# this engine does not strictly enforce while building. When those assumptions
# are violated the visualizer does not crash — it silently drops edges whose
# endpoints are unknown, omits community members it can't resolve, and coerces
# non-numeric metrics to 0. The result is a graph that *looks* fine but is
# quietly missing data. validate_state() makes those failures loud and
# actionable so the agent can catch them BEFORE handing JSON to the user.
#
# Severity model:
#   error   — the frontend will silently lose or misrender data (referential
#             integrity breaks, wrong container type). Block handoff.
#   warning — degraded/compat behaviour the frontend tolerates (missing meta,
#             compat schema, isolated community ids). Surface, don't block.

# Fields normalize() reads off each node, with the JS default it falls back to.
_NODE_NUMERIC_FIELDS = (
    "degree", "weighted_degree", "in_strength", "out_strength",
    "pagerank", "pagerank_directed", "betweenness", "eigenvector",
)
# Numeric fields normalize() reads off each edge.
_EDGE_NUMERIC_FIELDS = (
    "weight", "interaction_weight", "co_presence_weight", "affiliation_weight",
    "dir_uv_weight", "dir_vu_weight", "dm_count", "mention_count",
    "thread_reply_count", "reaction_count", "co_presence_count",
)


def _is_number(x):
    return isinstance(x, (int, float)) and not isinstance(x, bool)


def validate_state(state):
    """Validate a network_state object against the network_viz.html contract.

    Returns a structured report instead of raising, so the agent can decide
    how to act on it:

        {
          "ok": bool,                 # True iff there are zero errors
          "errors":   [str, ...],     # frontend will lose/misrender data
          "warnings": [str, ...],     # tolerated, but worth knowing
          "stats":    { ... },        # quick shape summary
        }

    Each message is phrased to be actionable: it names the offending key and
    explains what the visualizer will do with it.
    """
    errors, warnings = [], []

    # ---- top level -------------------------------------------------------
    if not isinstance(state, dict):
        return {"ok": False,
                "errors": [f"top level is {type(state).__name__}, expected a JSON object"],
                "warnings": [], "stats": {}}

    version = str(state.get("schema_version", "")) or "(missing)"
    if version not in ("2.0",):
        warnings.append(
            f"schema_version is {version!r}, not '2.0' — the visualizer will run "
            f"in compatibility mode (synthesizing interaction_weight / directionality)."
        )

    nodes = state.get("nodes")
    if nodes is None:
        errors.append("'nodes' is missing — the visualizer renders an empty graph.")
        nodes = {}
    elif not isinstance(nodes, dict):
        errors.append(f"'nodes' is a {type(nodes).__name__}; the visualizer iterates "
                      f"Object.keys(nodes) and expects an object keyed by user_id.")
        nodes = {}
    elif not nodes:
        warnings.append("'nodes' is empty — the visualizer will show an empty graph.")

    node_ids = set(nodes.keys()) if isinstance(nodes, dict) else set()

    # ---- nodes -----------------------------------------------------------
    bad_community_type = 0
    missing_metric_nodes = 0
    for nid, nd in (nodes.items() if isinstance(nodes, dict) else []):
        if not isinstance(nd, dict):
            errors.append(f"node {nid!r} is {type(nd).__name__}, expected an object.")
            continue
        cid = nd.get("community_id", -1)
        if cid is not None and not isinstance(cid, int):
            bad_community_type += 1
        for f in _NODE_NUMERIC_FIELDS:
            if f in nd and not _is_number(nd[f]):
                warnings.append(f"node {nid!r} field {f!r} is non-numeric ({nd[f]!r}); "
                                f"the visualizer will coerce it to 0.")
            elif f not in nd:
                missing_metric_nodes += 1
                break
    if bad_community_type:
        warnings.append(f"{bad_community_type} node(s) have a non-integer community_id; "
                        f"color/cluster grouping keys on integers.")
    if missing_metric_nodes:
        warnings.append(f"{missing_metric_nodes} node(s) are missing one or more centrality "
                        f"metrics (pagerank/betweenness/…); the visualizer defaults them to 0.")

    # ---- edges -----------------------------------------------------------
    edges = state.get("edges", {})
    if isinstance(edges, dict):
        edge_iter = list(edges.items())
    elif isinstance(edges, list):
        edge_iter = list(enumerate(edges))
        warnings.append("'edges' is an array; the visualizer accepts this, but the schema "
                        "and this engine emit an object keyed 'USMALLER:ULARGER'.")
    else:
        errors.append(f"'edges' is a {type(edges).__name__}; expected an object or array.")
        edge_iter = []

    dangling = []
    for key, ed in edge_iter:
        if not isinstance(ed, dict):
            errors.append(f"edge {key!r} is {type(ed).__name__}, expected an object.")
            continue
        u, v = ed.get("u"), ed.get("v")
        if u is None or v is None:
            errors.append(f"edge {key!r} is missing 'u' and/or 'v'; the visualizer looks up "
                          f"endpoints by these fields (not by the key) and silently drops it.")
            continue
        if node_ids and (u not in node_ids or v not in node_ids):
            missing = [x for x in (u, v) if x not in node_ids]
            dangling.append((key, missing))
        for f in _EDGE_NUMERIC_FIELDS:
            if f in ed and not _is_number(ed[f]):
                warnings.append(f"edge {key!r} field {f!r} is non-numeric ({ed[f]!r}); "
                                f"the visualizer will coerce it to 0.")
        sc = ed.get("shared_channels")
        if sc is not None and not isinstance(sc, list):
            warnings.append(f"edge {key!r} 'shared_channels' is {type(sc).__name__}, "
                            f"expected a list (the visualizer reads .length).")
    if dangling:
        sample = ", ".join(f"{k} (unknown: {', '.join(m)})" for k, m in dangling[:5])
        more = "" if len(dangling) <= 5 else f" (+{len(dangling)-5} more)"
        errors.append(f"{len(dangling)} edge(s) reference user_ids absent from 'nodes' and "
                      f"will be SILENTLY DROPPED by the visualizer: {sample}{more}.")

    # ---- communities -----------------------------------------------------
    communities = state.get("communities", {})
    comm_ids = set()
    if communities and not isinstance(communities, dict):
        errors.append(f"'communities' is a {type(communities).__name__}; expected an object "
                      f"keyed by community_id.")
        communities = {}
    orphan_members = 0
    for cid, c in (communities.items() if isinstance(communities, dict) else []):
        comm_ids.add(str(cid))
        if not isinstance(c, dict):
            errors.append(f"community {cid!r} is {type(c).__name__}, expected an object.")
            continue
        members = c.get("members", [])
        if not isinstance(members, list):
            errors.append(f"community {cid!r} 'members' is {type(members).__name__}, expected a list.")
            continue
        for m in members:
            if node_ids and m not in node_ids:
                orphan_members += 1
    if orphan_members:
        errors.append(f"{orphan_members} community member reference(s) point at user_ids absent "
                      f"from 'nodes'; the visualizer drops them, shrinking cluster cards silently.")

    # referential integrity: every node's community_id should have a card
    if comm_ids and node_ids:
        used = {str(nd.get("community_id")) for nd in nodes.values()
                if isinstance(nd, dict) and nd.get("community_id") not in (None, -1)}
        missing_cards = sorted(used - comm_ids)
        if missing_cards:
            warnings.append(f"nodes reference community id(s) {missing_cards} with no entry in "
                            f"'communities' — the legend/clusters view won't list them "
                            f"(colors still render).")

    # ---- affiliation_top -------------------------------------------------
    aff = state.get("affiliation_top", [])
    if aff and not isinstance(aff, list):
        warnings.append(f"'affiliation_top' is {type(aff).__name__}, expected a list; "
                        f"the clusters view will ignore it.")
        aff = []
    aff_orphans = 0
    for a in (aff if isinstance(aff, list) else []):
        if not isinstance(a, dict):
            continue
        if node_ids and (a.get("u") not in node_ids or a.get("v") not in node_ids):
            aff_orphans += 1
    if aff_orphans:
        warnings.append(f"{aff_orphans} affiliation_top pair(s) reference unknown user_ids; "
                        f"the clusters view skips them.")

    # ---- meta ------------------------------------------------------------
    meta = state.get("meta")
    if meta is None:
        warnings.append("'meta' is missing — header run-info / message counts show '—'.")
    elif not isinstance(meta, dict):
        warnings.append(f"'meta' is {type(meta).__name__}, expected an object.")

    cn = state.get("channel_names")
    if cn is not None and not isinstance(cn, dict):
        warnings.append(f"'channel_names' is {type(cn).__name__}, expected an object "
                        f"{{channel_id: name}}.")

    stats = {
        "schema_version": version,
        "nodes": len(node_ids),
        "edges": len(edge_iter),
        "communities": len(comm_ids),
        "affiliation_top": len(aff) if isinstance(aff, list) else 0,
        "dangling_edges": len(dangling),
        "orphan_community_members": orphan_members,
    }
    return {"ok": not errors, "errors": errors, "warnings": warnings, "stats": stats}


def _print_validation_report(report, path):
    """Human-readable, agent-parseable rendering of a validate_state() result."""
    s = report["stats"]
    print(f"\nVALIDATE {path}")
    print(f"   schema {s.get('schema_version','?')} | "
          f"{s.get('nodes',0)} nodes | {s.get('edges',0)} edges | "
          f"{s.get('communities',0)} communities | "
          f"{s.get('affiliation_top',0)} affiliation pairs")
    if report["errors"]:
        print(f"\n  ERRORS ({len(report['errors'])}) — frontend will lose/misrender data:")
        for e in report["errors"]:
            print(f"   ✗ {e}")
    if report["warnings"]:
        print(f"\n  WARNINGS ({len(report['warnings'])}) — tolerated, worth knowing:")
        for w in report["warnings"]:
            print(f"   ! {w}")
    if report["ok"] and not report["warnings"]:
        print("\n  OK — clean. Safe to hand off to network_viz.html.")
    elif report["ok"]:
        print("\n  OK (with warnings) — safe to hand off; warnings are non-blocking.")
    else:
        print("\n  NOT OK — resolve the errors above before handing this file to the visualizer.")
    return report["ok"]


# ===========================================================================
# TWO-USER QUERY
# ===========================================================================

def query_relationship(G, state, u1, v1):
    nodes = state.get("nodes", {})
    name = lambda uid: nodes.get(uid, {}).get("name", uid)
    L = [f"Relationship: @{name(u1)} <-> @{name(v1)}", ""]

    if G.has_edge(u1, v1):
        e = G[u1][v1]
        L.append(f"Direct connection: YES  (total {e.get('weight',0):.2f}, "
                 f"interaction {e.get('interaction_weight',0):.2f})")
        L.append(f"  DMs {e.get('dm_count',0)} | mentions {e.get('mention_count',0)} | "
                 f"replies {e.get('thread_reply_count',0)} | reactions {e.get('reaction_count',0)}")
        a, b = (u1, v1) if u1 < v1 else (v1, u1)
        L.append(f"  Directional: @{name(a)}->@{name(b)} {e.get('dir_uv_weight',0):.1f}  |  "
                 f"@{name(b)}->@{name(a)} {e.get('dir_vu_weight',0):.1f}  "
                 f"(reciprocity {e.get('reciprocity',0):.2f})")
        L.append(f"  Shared channels: {len(e.get('shared_channels',[]))} | "
                 f"affiliation {e.get('affiliation_weight',0):.2f}")
        ts = e.get("last_interaction_ts", 0)
        if ts:
            L.append(f"  Last interaction: {datetime.fromtimestamp(ts, tz=timezone.utc):%Y-%m-%d}")
    else:
        L.append("Direct connection: NONE")
        aff = state.get("affiliation_top", [])
        hit = next((a for a in aff if {a['u'], a['v']} == {u1, v1}), None)
        if hit:
            L.append(f"  ...but they co-participate in channels (affiliation {hit['score']:.2f})")

    L.append("")
    _distance_view(G)
    try:
        path = nx.shortest_path(G, u1, v1, weight="_dist")
        L.append(f"Shortest path ({len(path)-1} hops): " + " -> ".join(f"@{name(p)}" for p in path))
    except (nx.NetworkXNoPath, nx.NodeNotFound):
        L.append("Shortest path: DISCONNECTED")

    L.append("")
    if G.has_node(u1) and G.has_node(v1):
        common = list(nx.common_neighbors(G, u1, v1))
        L.append(f"Common neighbors ({len(common)}): " +
                 (", ".join("@" + name(c) for c in common[:5]) or "none"))
        jac = list(nx.jaccard_coefficient(G, [(u1, v1)]))
        if jac:
            L.append(f"Jaccard (neighbor overlap): {jac[0][2]:.3f}")

    c1 = nodes.get(u1, {}).get("community_id", -1)
    c2 = nodes.get(v1, {}).get("community_id", -1)
    L.append("")
    L.append("Community: " + (f"SAME cluster {c1}" if c1 == c2 and c1 != -1
                              else f"DIFFERENT ({c1} vs {c2})"))
    return "\n".join(L)


# ===========================================================================
# CLI
# ===========================================================================

def _channel_meta_maps(path):
    sizes, names = {}, {}
    if not path:
        return sizes, names
    with open(path) as f:
        chans = json.load(f)
    for ch in chans:
        cid = ch.get("channel_id") or ch.get("id")
        if not cid:
            continue
        sizes[cid] = int(ch.get("member_count") or ch.get("num_members") or 0)
        if ch.get("name"):
            names[cid] = ch["name"]
    return sizes, names


def main():
    global DEFAULT_MIN_WEIGHT
    p = argparse.ArgumentParser(description="Slack Network Ops")
    p.add_argument("--mode", required=True,
                   choices=["score-channels", "bootstrap", "delta", "query", "report", "validate"])
    p.add_argument("--input")
    p.add_argument("--users")
    p.add_argument("--channels")
    p.add_argument("--state")
    p.add_argument("--output")
    p.add_argument("--user1")
    p.add_argument("--user2")
    p.add_argument("--recent")
    p.add_argument("--prior", help="previous network_state.json; its channel_stats seed "
                                    "the scorer's learned yield prior (score-channels)")
    p.add_argument("--decay-halflife", type=float, default=DEFAULT_DECAY_HALFLIFE)
    p.add_argument("--channel-cap", type=int, default=60)
    p.add_argument("--min-weight", type=float, default=DEFAULT_MIN_WEIGHT)
    args = p.parse_args()

    DEFAULT_MIN_WEIGHT = args.min_weight
    now_iso = datetime.now(tz=timezone.utc).isoformat()
    now_ts = time.time()

    if args.mode == "score-channels":
        if not args.channels or not args.output:
            p.error("score-channels requires --channels and --output")
        with open(args.channels) as f:
            channels = json.load(f)
        workspace_size = 0
        if args.users:
            with open(args.users) as f:
                workspace_size = len(json.load(f))
        recent = None
        if args.recent:
            with open(args.recent) as f:
                recent = json.load(f)
        prior_stats = None
        if args.prior:
            with open(args.prior) as f:
                prior_stats = json.load(f).get("channel_stats", {})
        plan = score_channels(channels, workspace_size,
                              channel_cap=args.channel_cap, recently_active_ids=recent,
                              prior_stats=prior_stats)
        with open(args.output, "w") as f:
            json.dump(plan, f, indent=2)
        print(f"Scored {len(plan['ranked'])} channels -> crawl {len(plan['crawl'])} "
              f"(cap {args.channel_cap}). Top 5:")
        for c in plan["ranked"][:5]:
            print(f"   {c['score']:+.1f}  #{c['name']:<22} ({c['member_count']} mem)  "
                  f"{'VETO ' if c['vetoed'] else ''}{c['reasons'][:2]}")
        return

    if args.mode == "bootstrap":
        if not args.input or not args.users or not args.output:
            p.error("bootstrap requires --input, --users, --output")
        with open(args.input) as f: raw = json.load(f)
        with open(args.users) as f: users = json.load(f)
        sizes, names = _channel_meta_maps(args.channels)
        G = build_graph(raw, users, channel_sizes=sizes)
        meta = {"run_count": 1, "first_run": now_iso, "last_run": now_iso,
                "last_run_ts": now_ts, "nodes_count": G.number_of_nodes(),
                "edges_count": G.number_of_edges(), "messages_processed": len(raw),
                "algo": {"decay_halflife_days": args.decay_halflife}}
        cstats = channel_yield(raw)
        state = graph_to_state(G, users, meta, channel_names=names, channel_stats=cstats)
        with open(args.output, "w") as f: json.dump(state, f, indent=2)
        print(f"Bootstrap: {G.number_of_nodes()} nodes, {G.number_of_edges()} edges -> {args.output}")
        rep = validate_state(state)
        if not rep["ok"] or rep["warnings"]:
            _print_validation_report(rep, args.output)

    elif args.mode == "delta":
        if not args.input or not args.state or not args.output:
            p.error("delta requires --input, --state, --output")
        with open(args.input) as f: new_interactions = json.load(f)
        with open(args.state) as f: prev_state = json.load(f)
        sizes, names = _channel_meta_maps(args.channels)
        names = {**prev_state.get("channel_names", {}), **names}
        G, users = state_to_graph(prev_state)
        last_run_ts = prev_state.get("meta", {}).get("last_run_ts", now_ts - 86400)

        G, cold = apply_decay(G, last_run_ts, halflife_days=args.decay_halflife)
        G, delta = merge_delta(G, new_interactions, users, channel_sizes=sizes)
        delta["cold_edges"] = cold
        for rec in new_interactions:
            for uid in (rec.get("from"), rec.get("to")):
                if uid and uid not in users:
                    users[uid] = {"name": uid, "real_name": "", "title": ""}

        pm = prev_state.get("meta", {})
        meta = {"run_count": pm.get("run_count", 1) + 1,
                "first_run": pm.get("first_run", now_iso), "last_run": now_iso,
                "last_run_ts": now_ts, "nodes_count": G.number_of_nodes(),
                "edges_count": G.number_of_edges(),
                "messages_processed": pm.get("messages_processed", 0) + len(new_interactions),
                "delta_summary": delta, "algo": {"decay_halflife_days": args.decay_halflife}}
        cstats = merge_channel_stats(prev_state.get("channel_stats", {}),
                                     channel_yield(new_interactions))
        state = graph_to_state(G, users, meta, channel_names=names,
                               prev_communities=prev_state.get("communities", {}),
                               channel_stats=cstats)
        with open(args.output, "w") as f: json.dump(state, f, indent=2)
        print(f"Delta run #{meta['run_count']}: +{len(delta['new_edges'])} new, "
              f"{len(delta['strengthened_edges'])} strengthened, "
              f"{len(cold)} cold, edges now {G.number_of_edges()}")
        rep = validate_state(state)
        if not rep["ok"] or rep["warnings"]:
            _print_validation_report(rep, args.output)

    elif args.mode == "query":
        if not args.state or not args.user1 or not args.user2:
            p.error("query requires --state, --user1, --user2")
        with open(args.state) as f: state = json.load(f)
        G, _ = state_to_graph(state)
        def resolve(ref):
            if ref in G.nodes(): return ref
            for uid, d in state.get("nodes", {}).items():
                if d.get("name") == ref or d.get("real_name") == ref:
                    return uid
            return ref
        print(query_relationship(G, state, resolve(args.user1), resolve(args.user2)))

    elif args.mode == "report":
        if not args.state:
            p.error("report requires --state")
        with open(args.state) as f: state = json.load(f)
        meta, nodes, comms = state.get("meta", {}), state.get("nodes", {}), state.get("communities", {})
        print(f"\nSLACK NETWORK — Run #{meta.get('run_count','?')}  ({meta.get('last_run','?')})")
        print(f"   Nodes {meta.get('nodes_count')} | Edges {meta.get('edges_count')} | "
              f"Messages {meta.get('messages_processed',0)}\n")
        print("Top 10 by PageRank")
        for i, (uid, d) in enumerate(sorted(nodes.items(), key=lambda x: -x[1].get("pagerank", 0))[:10], 1):
            print(f"   {i:2}. @{d.get('name','?'):<18} PR={d.get('pagerank',0):.4f} "
                  f"btw={d.get('betweenness',0):.3f} deg={d.get('degree',0)}")
        print(f"\nCommunities ({len(comms)})")
        for cid, c in sorted(comms.items(), key=lambda x: -x[1].get("size", 0)):
            core = [f"@{nodes.get(m,{}).get('name',m)}" for m in c.get("core_members", [])[:3]]
            print(f"   {cid}: {c.get('size')} members  core {', '.join(core)}")

    elif args.mode == "validate":
        if not args.state:
            p.error("validate requires --state")
        try:
            with open(args.state) as f:
                state = json.load(f)
        except json.JSONDecodeError as e:
            print(f"\nVALIDATE {args.state}\n   ✗ file is not valid JSON: {e}")
            sys.exit(1)
        rep = validate_state(state)
        ok = _print_validation_report(rep, args.state)
        sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
