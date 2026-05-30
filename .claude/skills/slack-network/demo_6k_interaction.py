#!/usr/bin/env python3
"""
demo_6k_interaction.py — a theorized 6,000-message Slack interaction
=====================================================================
A self-contained, deterministic showcase for two features of this skill:

  (1) THE LEARNED-YIELD LOOP  — `score-channels --prior`.
      Channel selection that learns from observed results, not just metadata.
      A cryptically-named channel (`c-ops-7`) that the metadata scorer can only
      reach via a topic match (+2) PROVES OUT on the first crawl — dense,
      reciprocated ties — and on the next scoring pass leaps up the ranking on
      its learned yield + reciprocity bonus. Meanwhile `proj-ghost`, which looks
      like a top-tier work channel by name (+4) but is really a one-way status
      feed, surfaces lots of pairs yet zero reciprocity, so it gains far less.

  (2) AMBIENT CO-PRESENCE — the symmetric, size-discounted `co_presence` signal.
      Two people who only ever co-post in #general get a visible *edge* but no
      *influence*: their `interaction_weight` is 0 and centrality ignores it.
      The same 0.3 base event is worth far less in a 20-person broadcast channel
      than in a 6-person working group (size discount).

This file does NO Slack I/O. It fabricates the JSON the agent would normally
collect from Slack, writes it to ./demo_out, then drives the REAL engine
(`network_ops.py`, sitting next to this file) through the actual pipeline and
narrates what each feature did.

    python demo_6k_interaction.py

Outputs land in ./demo_out:
    users.json  channels.json  recent.json
    raw_interactions.json        (exactly 6,000 records — the "6k interaction")
    delta_interactions.json
    crawl_plan_bootstrap.json    (score-channels, no prior)
    crawl_plan_learned.json      (score-channels --prior bootstrap_state.json)
    bootstrap_state.json  delta_state.json
"""

import json
import math
import os
import random
import subprocess
import sys
import time
from collections import defaultdict

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "demo_out")
ENGINE = os.path.join(HERE, "network_ops.py")
SEED = 1312                                  # same seed the engine uses — fully deterministic
NOW = time.time()
DAY = 86400.0
BOOT_WINDOW = (NOW - 35 * DAY, NOW - 3 * DAY)   # the 6k-message bootstrap history
DELTA_WINDOW = (NOW - 2 * DAY, NOW)             # the follow-on delta crawl

BASE = {"dm": 4.0, "mention": 3.0, "thread_reply": 1.5, "reaction": 0.5, "co_presence": 0.3}

rng = random.Random(SEED)


# ---------------------------------------------------------------------------
# The theorized workspace
# ---------------------------------------------------------------------------

USERS = {
    "U001": ("alice",   "Alice Ng",       "Staff Engineer"),
    "U002": ("bob",     "Bob Reyes",      "Backend Engineer"),
    "U003": ("carol",   "Carol Tan",      "Site Reliability Eng"),
    "U004": ("dave",    "Dave Olsen",     "Engineer"),
    "U005": ("erin",    "Erin Park",      "Platform Lead"),
    "U006": ("frank",   "Frank Doyle",    "Platform Engineer"),   # the bridge
    "U007": ("grace",   "Grace Liu",      "Engineer"),
    "U008": ("heidi",   "Heidi Vance",    "Engineer"),
    "U009": ("ivan",    "Ivan Petrov",    "Growth PM"),
    "U010": ("judy",    "Judy Cole",      "Data Scientist"),
    "U011": ("mallory", "Mallory Quinn",  "Product Designer"),
    "U012": ("niaj",    "Niaj Rahman",    "Support Engineer"),
    "U013": ("olivia",  "Olivia Brooks",  "Customer Success Lead"),
    "U014": ("peggy",   "Peggy Mraz",     "Program Manager"),     # the broadcaster
    "U015": ("trent",   "Trent Kessler",  "New Hire"),            # co-presence only
    "U016": ("oscar",   "Oscar Mendez",   "New Hire"),            # co-presence only
    "U017": ("sybil",   "Sybil Adams",    "Recruiter"),
    "U018": ("victor",  "Victor Hahn",    "Office Ops"),
    "U019": ("wendy",   "Wendy Foss",     "People Ops"),
    "U020": ("craig",   "Craig Bauer",    "Finance"),
}
ALL_IDS = list(USERS)

# channel_id -> (name, member_count, is_private, topic, purpose)
CHANNELS = {
    # ---- working channels (the ones that should be crawled) ----------------
    "C_ATLAS":  ("proj-atlas",       8,  True,  "Atlas launch working group",        "Ship Atlas GA"),
    "C_OPS7":   ("c-ops-7",          6,  True,  "Atlas incident & on-call rotation", "who's on call"),
    "C_PLAT":   ("eng-platform",     10, False, "Platform team",                     "infra + tooling"),
    "C_GROWTH": ("team-growth",      7,  False, "Growth experiments",                "activation funnel"),
    "C_CS":     ("cust-success",     6,  False, "Customer escalations",              "enterprise accounts"),
    # ---- looks great by name, but it's a one-way status feed ---------------
    "C_GHOST":  ("proj-ghost",       7,  False, "Project Ghost delivery squad",      "weekly status"),
    # ---- broadcast / social (should be vetoed) -----------------------------
    "C_GEN":    ("general",          20, False, "Company-wide",                      "announcements & chatter"),
    "C_RAND":   ("random",           19, False, "Watercooler",                       "off-topic"),
    "C_ANN":    ("announcements",    20, False, "Company announcements",             "town-hall, all-hands"),
    "C_WATER":  ("watercooler",      17, False, "Coffee chat",                       "social"),
}
CH_SIZE = {cid: meta[1] for cid, meta in CHANNELS.items()}


def ts():
    return round(rng.uniform(*BOOT_WINDOW), 1)


def ts_delta():
    return round(rng.uniform(*DELTA_WINDOW), 1)


def rec(frm, to, signal, channel, t):
    return {"from": frm, "to": to, "signal": signal,
            "weight": BASE[signal], "channel": channel, "ts": t}


def directed(channel, members, mode, intensity, when=ts, lead=None,
             signals=("mention", "thread_reply", "reaction"), dm_pairs=()):
    """Emit directed records among `members` in `channel`.

    mode:
      "mutual"  every collaborating pair is seen in BOTH directions (high reciprocity)
      "one_way" only `lead`->other records exist (reciprocity 0 — the proj-ghost trap)
      "mixed"   each pair gets a random reciprocity split
    intensity: (lo, hi) interactions per pair.
    """
    out = []
    sig_w = [4, 3, 2]   # mention-heavy, then replies, then reactions
    for i in range(len(members)):
        for j in range(i + 1, len(members)):
            a, b = members[i], members[j]
            if mode == "one_way" and lead not in (a, b):
                continue
            n = rng.randint(*intensity)
            for _ in range(n):
                sig = rng.choices(signals, weights=sig_w[:len(signals)])[0]
                if mode == "one_way":
                    frm, to = lead, (b if a == lead else a)
                elif mode == "mutual":
                    frm, to = (a, b) if rng.random() < 0.5 else (b, a)
                else:  # mixed — biased toward one side per pair, occasionally flipped
                    fwd = rng.random() < 0.78
                    frm, to = (a, b) if fwd else (b, a)
                out.append(rec(frm, to, sig, channel, when()))
            # a sprinkle of 1:1 DMs between named close collaborators
            if (a, b) in dm_pairs or (b, a) in dm_pairs:
                for _ in range(rng.randint(2, 5)):
                    frm, to = (a, b) if rng.random() < 0.5 else (b, a)
                    out.append(rec(frm, to, "dm", channel, when()))
    return out


def copresence(channel, members, pair_events):
    """Ambient co-presence: `pair_events` records per sampled unordered pair.

    The engine size-discounts these by the channel's member_count, so the SAME
    0.3 base event is worth far less in #general than in a 6-person working group.
    """
    out = []
    for i in range(len(members)):
        for j in range(i + 1, len(members)):
            for _ in range(pair_events):
                a, b = members[i], members[j]
                out.append(rec(a, b, "co_presence", channel, ts()))
    return out


# ---------------------------------------------------------------------------
# Build the 6,000-record bootstrap interaction
# ---------------------------------------------------------------------------

def build_bootstrap():
    raw = []

    # --- proven working channels: dense, reciprocated directed signal --------
    raw += directed("C_ATLAS",  ["U001", "U002", "U003", "U004", "U006"],
                    "mutual", (3, 7), dm_pairs=[("U001", "U002")])
    # c-ops-7: the cryptic channel that PROVES OUT — densest, fully reciprocal
    raw += directed("C_OPS7",   ["U001", "U003", "U006", "U002", "U012", "U013"],
                    "mutual", (5, 9), dm_pairs=[("U003", "U006")])
    raw += directed("C_PLAT",   ["U005", "U006", "U007", "U008", "U002"],
                    "mutual", (3, 6))
    raw += directed("C_GROWTH", ["U009", "U010", "U011", "U013"],
                    "mixed", (3, 6))
    raw += directed("C_CS",     ["U012", "U013", "U009", "U004"],
                    "mixed", (3, 6))

    # --- proj-ghost: looks like a top work channel, but it's a status feed ---
    # Peggy broadcasts; nobody talks back. Many pairs, ZERO reciprocity.
    raw += directed("C_GHOST",  ["U014", "U001", "U002", "U005", "U009", "U010"],
                    "one_way", (4, 7), lead="U014",
                    signals=("mention", "thread_reply"))

    # --- ambient co-presence in the working channels (latent affiliation) ----
    raw += copresence("C_ATLAS",  ["U001", "U002", "U003", "U004", "U006"], 2)
    raw += copresence("C_OPS7",   ["U001", "U003", "U006", "U002", "U012", "U013"], 2)
    raw += copresence("C_PLAT",   ["U005", "U006", "U007", "U008"], 2)
    raw += copresence("C_GROWTH", ["U009", "U010", "U011"], 2)

    # --- ambient co-presence in BROADCAST channels (the size-discount story) -
    broadcast_crowd = ALL_IDS[:14]
    raw += copresence("C_RAND",  broadcast_crowd[:10], 1)
    raw += copresence("C_WATER", broadcast_crowd[2:11], 1)

    # Trent & Oscar ONLY ever co-post in #general — a pure co-presence edge.
    # Stack enough events that their (size-discounted) total clears the 0.5
    # prune floor, so the edge survives with interaction_weight == 0.
    for _ in range(14):
        raw.append(rec("U015", "U016", "co_presence", "C_GEN", ts()))

    # Pad to EXACTLY 6,000 with ambient #general co-presence among the crowd
    # (this is the realistic bulk of a big channel: everyone "present", nobody
    # actually interacting). These are precisely the O(n^2) phantom edges the
    # engine refuses to turn into influence.
    target = 6000
    crowd = ALL_IDS[:14]
    while len(raw) < target:
        a, b = rng.sample(crowd, 2)
        raw.append(rec(a, b, "co_presence", "C_GEN", ts()))
    raw = raw[:target]
    rng.shuffle(raw)
    return raw


def build_delta():
    """A follow-on crawl: a fresh incident fires, Atlas/ops stay hot, the rest cools."""
    raw = []
    raw += directed("C_INC", ["U003", "U006", "U001", "U002", "U012"],
                    "mutual", (4, 8), when=ts_delta)
    raw += directed("C_OPS7", ["U001", "U003", "U006", "U012"],
                    "mutual", (2, 4), when=ts_delta)
    raw += directed("C_ATLAS", ["U001", "U002", "U006"],
                    "mutual", (2, 4), when=ts_delta)
    # incident channel metadata only shows up now (it didn't exist at bootstrap)
    CHANNELS["C_INC"] = ("incident-2026-05", 5, True,
                         "SEV2 — db latency", "war room")
    CH_SIZE["C_INC"] = 5
    rng.shuffle(raw)
    return raw


# ---------------------------------------------------------------------------
# Write the inputs the agent would normally have collected from Slack
# ---------------------------------------------------------------------------

def write_inputs(raw, delta):
    os.makedirs(OUT, exist_ok=True)
    users = {uid: {"name": n, "real_name": rn, "title": t}
             for uid, (n, rn, t) in USERS.items()}
    channels = [{"channel_id": cid, "name": m[0], "member_count": m[1],
                 "is_private": m[2], "is_archived": False,
                 "topic": m[3], "purpose": m[4],
                 "last_message_ts": NOW - 1 * DAY}
                for cid, m in CHANNELS.items()]
    # channels active in the recent search window (the A1.5 liveness hint)
    recent = ["C_ATLAS", "C_OPS7", "C_PLAT", "C_GHOST", "C_INC", "C_GEN"]

    _dump("users.json", users)
    _dump("channels.json", channels)
    _dump("recent.json", recent)
    _dump("raw_interactions.json", raw)
    _dump("delta_interactions.json", delta)
    return users, channels


def _dump(name, obj):
    with open(os.path.join(OUT, name), "w") as f:
        json.dump(obj, f, indent=2)


def _path(name):
    return os.path.join(OUT, name)


def run(*args):
    """Invoke the real engine and stream its output."""
    cmd = [sys.executable, ENGINE, *args]
    shown = [a.replace(OUT + os.sep, "") for a in args]
    print("    $ python network_ops.py " + " ".join(shown))
    res = subprocess.run(cmd, capture_output=True, text=True)
    if res.returncode not in (0,):
        sys.stdout.write(res.stdout)
        sys.stderr.write(res.stderr)
    return res.stdout


# ---------------------------------------------------------------------------
# Narration helpers
# ---------------------------------------------------------------------------

def hr(title):
    print("\n" + "=" * 74)
    print(title)
    print("=" * 74)


def rank_table(plan, stats=None):
    """Print the scorer's ranking with rank #, score, and (optionally) prior yield."""
    name = {cid: m[0] for cid, m in CHANNELS.items()}
    rows = []
    for i, c in enumerate(plan["ranked"], 1):
        nm = c["name"] or name.get(c["channel_id"], c["channel_id"])
        flag = "VETO" if c["vetoed"] else ("crawl" if c["channel_id"] in plan["crawl"] else "  -  ")
        y = ""
        if stats and c["channel_id"] in stats:
            s = stats[c["channel_id"]]
            recip = (s.get("reciprocal_pairs", 0) / s["pairs"]) if s.get("pairs") else 0
            y = f"  yield: {s.get('pairs',0)} pairs, {recip:.0%} mutual"
        rows.append((i, c["score"], nm, flag, y))
    for i, score, nm, flag, y in rows:
        print(f"   #{i:<2} {score:+6.1f}  {flag:<5}  #{nm:<16}{y}")
    return {c["name"]: i for i, c in enumerate(plan["ranked"], 1)}


def main():
    print(__doc__.split("\n\n")[0])
    raw = build_bootstrap()
    delta = build_delta()
    write_inputs(raw, delta)

    # quick provenance of the 6,000 records
    by_signal = defaultdict(int)
    for r in raw:
        by_signal[r["signal"]] += 1
    hr("THE THEORIZED 6,000-MESSAGE INTERACTION")
    print(f"   {len(raw):,} records written to demo_out/raw_interactions.json")
    for s in ("dm", "mention", "thread_reply", "reaction", "co_presence"):
        print(f"      {s:<13} {by_signal[s]:>5}")
    print(f"   {len(USERS)} users, {len(CHANNELS)} channels")
    print("   note: co_presence is AMBIENT — it never feeds centrality, only edge totals.")

    # ---- STEP 1: score channels on metadata alone (no prior) ---------------
    hr("STEP 1 — score-channels  (BOOTSTRAP: metadata only, no learned prior)")
    print("Cryptic `c-ops-7` can only be reached by its topic match (+2); the obvious")
    print("work-named channels sit on top. `proj-ghost` looks like a great work channel.\n")
    run("--mode", "score-channels", "--channels", _path("channels.json"),
        "--users", _path("users.json"), "--recent", _path("recent.json"),
        "--channel-cap", "8", "--output", _path("crawl_plan_bootstrap.json"))
    plan0 = json.load(open(_path("crawl_plan_bootstrap.json")))
    rank0 = rank_table(plan0)

    # ---- STEP 2: bootstrap the graph from the 6k records -------------------
    hr("STEP 2 — bootstrap  (build the graph + record per-channel YIELD)")
    out = run("--mode", "bootstrap", "--input", _path("raw_interactions.json"),
              "--users", _path("users.json"), "--channels", _path("channels.json"),
              "--output", _path("bootstrap_state.json"))
    print("   " + out.strip().replace("\n", "\n   "))
    state = json.load(open(_path("bootstrap_state.json")))
    stats = state["channel_stats"]
    print("\n   Observed yield (channel_stats — directed signal only, co-presence excluded):")
    for cid, s in sorted(stats.items(), key=lambda kv: -kv[1]["pairs"]):
        nm = CHANNELS.get(cid, (cid,))[0]
        recip = (s["reciprocal_pairs"] / s["pairs"]) if s["pairs"] else 0
        print(f"      #{nm:<16} {s['pairs']:>2} pairs  {recip:>4.0%} mutual  "
              f"({s['interactions']} interactions)")

    # ---- STEP 3: re-score WITH the learned prior — the headline ------------
    hr("STEP 3 — score-channels --prior  (THE LEARNED-YIELD LOOP)")
    print("Feed the bootstrap state back in. The scorer now rewards channels that")
    print("actually produced reciprocated ties and discounts the rest.\n")
    run("--mode", "score-channels", "--channels", _path("channels.json"),
        "--users", _path("users.json"), "--recent", _path("recent.json"),
        "--prior", _path("bootstrap_state.json"),
        "--channel-cap", "8", "--output", _path("crawl_plan_learned.json"))
    plan1 = json.load(open(_path("crawl_plan_learned.json")))
    rank1 = rank_table(plan1, stats)

    s0 = {c["name"]: c["score"] for c in plan0["ranked"]}
    s1 = {c["name"]: c["score"] for c in plan1["ranked"]}
    print("\n   Score movement (metadata-only  ->  learned), proven channels:")
    for nm in ("c-ops-7", "proj-ghost", "incident-2026-05", "eng-platform", "cust-success"):
        d = s1[nm] - s0[nm]
        print(f"      #{nm:<16} {s0[nm]:+6.1f} -> {s1[nm]:+6.1f}  ({d:+.1f})")
    gap = s1["c-ops-7"] - s1["proj-ghost"]
    print(f"\n   => `c-ops-7` (15 pairs, 100% mutual) gets the full yield + reciprocity bonus and")
    print(f"      now leads `proj-ghost` (5 pairs, 0% mutual) by {gap:.1f} pts — the one-way status")
    print( "      feed earns the yield bonus but NO reciprocity bonus, despite its stronger NAME.")
    print( "   => `incident-2026-05` had no prior data, so it gets +0 learned and drops below every")
    print( "      channel that has PROVEN itself. Observed results now drive selection, not metadata.")

    # ---- STEP 4: ambient co-presence audit ---------------------------------
    hr("STEP 4 — AMBIENT CO-PRESENCE  (edge without influence + size discount)")
    edges, nodes = state["edges"], state["nodes"]

    e = edges.get("U015:U016")
    if e:
        print("Trent & Oscar only ever co-posted in #general — never a mention/reply/reaction:")
        print(f"   edge U015:U016  weight={e['weight']}  "
              f"interaction_weight={e['interaction_weight']}  "
              f"co_presence_weight={e['co_presence_weight']}")
        print(f"   co_presence_count={e['co_presence_count']}  "
              f"-> a real EDGE survives the prune floor, but interaction_weight is 0.")
        hub = max(nodes, key=lambda n: nodes[n]["pagerank"])
        print(f"   trent  pagerank={nodes['U015']['pagerank']:.5f}  "
              f"betweenness={nodes['U015']['betweenness']:.5f}  "
              f"interaction-degree={nodes['U015']['degree']}  (only the co-presence edge)")
        print(f"   @{nodes[hub]['name']:<6} pagerank={nodes[hub]['pagerank']:.5f}  "
              f"betweenness={nodes[hub]['betweenness']:.5f}   <- a real connector, for contrast")
        print("   => co-presence built a tie but manufactured NO influence. As designed:")
        print("      centrality runs on interaction_weight, which co-presence never touches.")

    # size discount: same 0.3 base event, two different channel sizes
    print("\n   Size discount — the SAME 0.3 co-presence event, by channel size:")
    for cid in ("C_OPS7", "C_GEN"):
        nm, size = CHANNELS[cid][0], CH_SIZE[cid]
        per_event = round(0.3 / math.log2(size + 1), 4)
        print(f"      #{nm:<16} ({size:>2} members):  0.3 / log2({size}+1) = {per_event} per event")
    print("   => an hour 'present' in a 20-person broadcast channel is worth a fraction")
    print("      of the same presence in a 6-person working group.")

    # affiliation: latent teams from co-presence, independent of DMs
    if state["affiliation_top"]:
        print("\n   Latent affiliations surfaced by co-participation (top 3):")
        for a in state["affiliation_top"][:3]:
            print(f"      @{nodes[a['u']]['name']} <-> @{nodes[a['v']]['name']}  "
                  f"score={a['score']}  direct_tie={a['has_interaction']}")

    # ---- STEP 5: delta to show the loop + decay persist across runs --------
    hr("STEP 5 — delta  (new incident fires; yield + decay carry forward)")
    out = run("--mode", "delta", "--input", _path("delta_interactions.json"),
              "--state", _path("bootstrap_state.json"),
              "--channels", _path("channels.json"),
              "--output", _path("delta_state.json"))
    print("   " + out.strip().replace("\n", "\n   "))
    dstate = json.load(open(_path("delta_state.json")))
    print("\n   EMA-smoothed yield after the delta (what the NEXT score-channels --prior sees):")
    for cid, s in sorted(dstate["channel_stats"].items(),
                         key=lambda kv: -kv[1].get("ema_pairs", 0))[:6]:
        nm = CHANNELS.get(cid, (cid,))[0]
        print(f"      #{nm:<16} ema_pairs={s.get('ema_pairs',0):<5}  "
              f"(this-run pairs={s.get('pairs',0)})")
    print("   => channels that went quiet this run decay toward 0; the hot incident +")
    print("      ops channels stay high. The prior tracks RECENT signal, not all-time.")

    hr("DONE")
    print(f"   All artifacts in: {OUT}")
    print("   View the graph:  open network_viz.html and drop demo_out/bootstrap_state.json")


if __name__ == "__main__":
    main()
