"""Automated end-to-end test for Party Game Hub v2.2.

Starts the real server on port 8001, then over real WebSockets:
  - creates a room, joins a 2nd human, adds 2 bots via the host
  - verifies the lobby list and bot editing
  - starts and plays full games of Reaction Test (3 rounds),
    Speed Math (5 questions) and Speed Trivia (5 questions)
  - checks scores / results are produced for each game
  - covers the v2.2 contracts: the uniform end flow (results ->
    end-of-game NEXT vote -> end_choice -> replay vote), turn order,
    UNO house rules, President roles + exchange, the Secret Word
    multi-round loop, interpolated Reaction points, team scoring
    styles and Championship mode
  - covers the v2.3 contracts: the item-13 Secret scoring repro
    (round-1 impostor catch -> 200/200/200/0), the one-pass
    President rotation, turn_order in UNO/Secret state messages,
    the Secret results banner fields, Memory round_start before
    memory_show, UNO any-card-may-lead multi-play, UNO highlighting
    inputs, the final President state with the Peasant title,
    end-vote denominators, mid-game session reclaim, Elimination
    until 1 remains with no finale, Elimination forcing FFA, and
    the end flow (NEXT vote -> end_choice -> replay "again" AND
    "lobby") for all 8 games
  - covers the v2.3 item-14 Group Elimination contracts: group
    availability (<6 rejected, 6+ accepted, Normal default),
    host-set group assignment via update_settings (bad values and
    unknown ids ignored), the lobby payload (elimination_mode /
    elim_groups / elim_phase, president_roles even when President
    is not the selected game), the group wire (challenge / break
    extras / eliminated), the deterministic group pick / balance /
    rebalance helpers, a full 6-player Group series (worst team
    loses its worst player, merge at 5 into Black, Black phase
    until 1 remains, winner 1000 / others 0), and a 7-player
    6-vs-1 series whose first break rebalances with transfers,
    and Reaction challenges assign interpolated points (verified
    in-process with all bots, and live from the round results
    whenever a Reaction challenge comes up)

Run:  .venv/bin/python -m pytest tests/test_hub.py -x -q
  or:  .venv/bin/python tests/test_hub.py
Exit code 0 = all checks passed.
"""
import asyncio
import json
import random
import re
import subprocess
import sys
import time
from pathlib import Path

import httpx
import websockets

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from questions import QUESTIONS  # noqa: E402
from secret_clues import SECRET_CLUES, SECRET_CATEGORY, VAGUE_CLUES_80  # noqa: E402
from server import make_math_question, record_answer, Room, Player, team_standings  # noqa: E402
from server import (GAMES, ELIM_POOL, SECRET_WORDS, VAGUE_CLUES, secret_tally,  # noqa: E402
                    build_president_deck, president_legal_play,  # noqa: E402
                    president_illegal_reason,  # noqa: E402
                    president_lowest_legal, president_scores_for,  # noqa: E402
                    _president_apply_play, _president_apply_pass,  # noqa: E402
                    _next_president_turn,  # noqa: E402
                    pres_rank, compute_turn_order,  # noqa: E402
                    president_roles_for, president_ordered_ids,  # noqa: E402
                    president_titles_for_finish,  # noqa: E402
                    build_uno_deck, uno_match, uno_bot_choice,  # noqa: E402
                    uno_best_colour, uno_value, uno_validate_play,  # noqa: E402
                    _uno_handle_draw, _uno_apply_play_multi,  # noqa: E402
                    memory_bot_correct,  # noqa: E402
                    _reaction_points, run_reaction_game,  # noqa: E402
                    _elim_balance_groups, _elim_pick_worst_team,  # noqa: E402
                    _elim_pick_worst_player, _elim_rebalance_groups)  # noqa: E402
from fractions import Fraction  # noqa: E402

PORT = 8001
BASE = f"http://127.0.0.1:{PORT}"
WS_URL = f"ws://127.0.0.1:{PORT}/ws"

PASS = []


def check(name, cond, detail=""):
    status = "PASS" if cond else "FAIL"
    print(f"[{status}] {name}" + (f" — {detail}" if detail else ""))
    PASS.append(cond)
    if not cond:
        raise AssertionError(f"{name} {detail}")


class Client:
    def __init__(self, name):
        self.name = name
        self.ws = None
        self.queue = asyncio.Queue()
        self._task = None

    async def connect(self):
        self.ws = await websockets.connect(WS_URL)
        self._task = asyncio.create_task(self._pump())

    async def _pump(self):
        try:
            async for raw in self.ws:
                await self.queue.put(json.loads(raw))
        except Exception:
            pass

    async def send(self, msg):
        await self.ws.send(json.dumps(msg))

    async def wait_for(self, mtype, timeout=20):
        """Return the next message of the given type (buffering others)."""
        deadline = time.monotonic() + timeout
        buf = []
        while time.monotonic() < deadline:
            try:
                msg = await asyncio.wait_for(self.queue.get(),
                                             timeout=max(0.1, deadline - time.monotonic()))
            except asyncio.TimeoutError:
                break
            if msg.get("type") == mtype:
                for b in buf:
                    await self.queue.put(b)
                return msg
            buf.append(msg)
        for b in buf:
            await self.queue.put(b)
        raise AssertionError(f"{self.name}: timed out waiting for {mtype}")

    async def wait_lobby(self, timeout=10):
        return await self.wait_for("lobby", timeout)

    async def wait_lobby_count(self, count, timeout=15):
        """Wait for a lobby broadcast listing exactly `count` players.

        Lobby broadcasts are queued, so the next one in the queue may be
        stale (from before the last join/add). Keep reading until the
        count matches.
        """
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            lobby = await self.wait_for("lobby", timeout=deadline - time.monotonic())
            if len(lobby["room"]["players"]) == count:
                return lobby
        raise AssertionError(f"{self.name}: no lobby with {count} players in {timeout}s")

    async def wait_lobby_phase(self, phase, count, timeout=15):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            lobby = await self.wait_for("lobby", timeout=deadline - time.monotonic())
            if (lobby["room"]["phase"] == phase
                    and len(lobby["room"]["players"]) == count):
                return lobby
        raise AssertionError(f"{self.name}: no {phase} lobby with {count} players")

    async def wait_lobby_where(self, pred, timeout=15):
        """Wait for a lobby broadcast whose room satisfies `pred`.

        Lobby broadcasts queue up, so a bare wait_for("lobby") often
        returns a stale one (from before the last settings change or
        game end). This loops until the room actually matches.
        """
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            lobby = await self.wait_for("lobby", timeout=deadline - time.monotonic())
            if pred(lobby["room"]):
                return lobby
        raise AssertionError(f"{self.name}: no lobby matching predicate in {timeout}s")

    async def close(self):
        if self.ws:
            await self.ws.close()

    async def drain(self, mtype):
        """Drop all currently-queued messages of the given type.

        Lobby broadcasts queue up (join, settings, game end), so a
        timed wait for the NEXT lobby must first discard the stale
        ones — otherwise it returns immediately on an old broadcast.
        """
        kept = []
        while not self.queue.empty():
            msg = self.queue.get_nowait()
            if msg.get("type") != mtype:
                kept.append(msg)
        for m in kept:
            await self.queue.put(m)


async def wait_any(client, types, timeout=20):
    """Wait for the next message whose type is one of `types`,
    buffering other messages back into the client's queue (like
    Client.wait_for does for a single type)."""
    deadline = time.monotonic() + timeout
    buf = []
    while time.monotonic() < deadline:
        try:
            msg = await asyncio.wait_for(
                client.queue.get(),
                timeout=max(0.1, deadline - time.monotonic()))
        except asyncio.TimeoutError:
            break
        if msg.get("type") in types:
            for b in buf:
                await client.queue.put(b)
            return msg
        buf.append(msg)
    for b in buf:
        await client.queue.put(b)
    raise AssertionError(f"{client.name}: timed out waiting for {types}")


async def drive_end_flow(host, voters, choice="lobby", *, method="skip",
                         count=None, timeout=15, expect_votes_needed=None):
    """v2.2 uniform end flow, driven after a `results` message.

    Passes the end-of-game NEXT vote (phase "results", flagged
    `end_of_game`), waits for `end_choice` (the FINAL standings),
    then votes `choice` ("again" | "lobby") via `replay_vote` from
    each voter in `voters` (>= half of connected humans decides it).
    `method="skip"` uses the host's host_skip; `method="vote"` has
    every voter send vote_next instead (covers vote_next working in
    phase "results"). Returns the `end_choice` message. For "lobby"
    it also waits for the lobby (when `count` is given); for "again"
    it waits for the fresh `game_started` and returns it as well.
    """
    deadline = time.monotonic() + timeout
    vb = None
    while time.monotonic() < deadline:
        cand = await host.wait_for("vote_break",
                                   timeout=deadline - time.monotonic())
        if cand.get("end_of_game"):
            vb = cand
            break
        # A stale round break queued ahead of the end-of-game vote:
        # vote it through so the game can proceed.
        await host.send({"type": "vote_next", "index": cand["index"]})
    check("v2.2 end-of-game vote break is flagged end_of_game",
          vb is not None and vb.get("end_of_game") is True, str(vb))
    if expect_votes_needed is not None:
        check("v2.3 end-of-game vote break needs half of HUMANS only "
              "(item 10)",
              vb is not None
              and vb.get("votes_needed") == expect_votes_needed,
              str(vb))
    if method == "skip":
        await host.send({"type": "host_skip"})
    else:
        for v in voters:
            await v.send({"type": "vote_next", "index": vb["index"]})
    end = await host.wait_for("end_choice", timeout=timeout)
    check("v2.2 end_choice carries final standings",
          len(end.get("rankings", [])) > 0
          and all("score" in r for r in end["rankings"]),
          str(end.get("rankings")))
    for v in voters:
        await v.send({"type": "replay_vote", "choice": choice})
    if choice == "lobby":
        lobby_msg = None
        if count is not None:
            lobby_msg = await host.wait_lobby_phase("lobby", count,
                                                    timeout=timeout)
        return end, lobby_msg
    started = await host.wait_for("game_started", timeout=timeout)
    return end, started


async def secret_driver(client, my_id, store, vote_for):
    """v2.2 Secret Word driver: gives one clue per round (a real
    non-vague clue for citizens, "thing" for the impostor), votes
    per `vote_for(round, my_id, alive_ids, clues_by_round)`, votes
    NEXT on round breaks, and records everything in `store`
    (words, clue_order, clues by round, round_results, vote_breaks,
    violations if the server ever asks an eliminated player to play).
    Returns the `secret_result` message."""
    my_word = None
    eliminated = False
    store["words"] = {}
    store["clue_order"] = []
    store["clues"] = {}
    store["round_results"] = []
    store["vote_breaks"] = []
    store["violations"] = []
    store["turn_orders"] = []  # v2.3 item 3: (type, turn_order) seen
    while True:
        msg = await wait_any(client, ("secret_word", "secret_clue_turn",
                                      "secret_clue_shown", "secret_vote_start",
                                      "secret_round_result", "secret_result",
                                      "vote_break"), timeout=60)
        t = msg["type"]
        if t in ("secret_word", "secret_clue_turn", "secret_clue_shown"):
            store["turn_orders"].append((t, msg.get("turn_order")))
        if t == "secret_word":
            store["words"][my_id] = msg
            my_word = msg.get("word")
        elif t == "secret_clue_turn":
            store["clue_order"].append((msg["round"], msg["player_id"]))
            if msg["player_id"] == my_id:
                if eliminated:
                    store["violations"].append(
                        "clue turn after elimination")
                else:
                    clue = "thing"
                    if my_word:
                        # A real clue: the first one that is not a
                        # generic vague word (so a vague clue always
                        # marks the impostor).
                        clue = next((c for c in SECRET_CLUES.get(my_word, [])
                                     if c not in VAGUE_CLUES_80), "thing")
                    await client.send({"type": "secret_clue", "clue": clue})
        elif t == "secret_clue_shown":
            store["clues"].setdefault(msg["round"], {})[msg["player_id"]] = msg["clue"]
        elif t == "secret_vote_start":
            alive_ids = [p["player_id"] for p in msg["players"]]
            if my_id not in alive_ids:
                eliminated = True
                continue
            if eliminated:
                store["violations"].append(
                    "vote start includes me after elimination")
                continue
            target = vote_for(msg["round"], my_id, alive_ids,
                              store["clues"])
            if target and target != my_id:
                await client.send({"type": "secret_vote",
                                   "player_id": target})
        elif t == "secret_round_result":
            store["round_results"].append(msg)
            if msg.get("eliminated") == my_id:
                eliminated = True
        elif t == "secret_result":
            store["result"] = msg
            return msg
        elif t == "vote_break":
            store["vote_breaks"].append(msg)
            await client.send({"type": "vote_next", "index": msg["index"]})


async def any_game_driver(client, my_id, state, mem_correct=False):
    """Plays whatever game is current (used by the random re-roll
    test): Reaction, Math, Trivia, Memory, UNO, President, Secret.
    Votes NEXT on every vote break. On `end_choice` the host votes
    "again" while state["want_again"] and state["replays"] < 3, else
    "lobby". Records game_started games in state["games"] and sets
    state["lobby"] on reaching the lobby. Memory: only the host
    answers correctly (mem_correct), so the game ends after round 1;
    Secret: everyone votes for state["secret_target"] (its owner
    falls back to the first other alive player), so the vote resolves
    in round 1."""
    number = None
    my_word = None
    # Drop stale lobby broadcasts (join / settings) queued before the
    # game started, or the lobby branch below would return at once.
    await client.drain("lobby")
    while True:
        msg = await wait_any(client, (
            "game_started", "question", "reaction_wait", "reaction_go",
            "memory_show", "memory_recall", "uno_state", "president_state",
            "president_notice", "secret_word", "secret_clue_turn",
            "secret_vote_start", "secret_round_result", "secret_result",
            "vote_break", "end_choice", "lobby", "error"), timeout=120)
        t = msg["type"]
        if t == "game_started":
            if msg["game"] != "elimination":
                state["games"].append(msg["game"])
        elif t == "question":
            if msg.get("options"):
                await client.send({"type": "answer", "answer": 0,
                                   "index": msg["index"]})
            else:
                ans = solve_math(msg["text"])
                await client.send({"type": "answer", "answer": str(ans),
                                   "index": msg["index"]})
        elif t == "reaction_go":
            await client.send({"type": "reaction_click"})
        elif t == "memory_show":
            number = msg["number"]
        elif t == "memory_recall":
            await client.send({"type": "memory_answer",
                               "answer": number if mem_correct else "0000"})
        elif t == "uno_state":
            if msg["turn_id"] != my_id:
                continue
            choice = uno_bot_choice(
                msg["hand"], msg["top"], msg["current_colour"],
                pending_type=msg.get("pending_type"),
                multi_play=msg.get("multi_play", False))
            if choice:
                colour = None
                if any(c.startswith("W") for c in choice):
                    colour = uno_best_colour(
                        [c for c in msg["hand"] if c not in choice]) or "R"
                await client.send({"type": "uno_play", "cards": choice,
                                   "colour": colour})
            else:
                await client.send({"type": "uno_draw"})
        elif t == "president_state":
            if msg.get("exchange") and msg["exchange"]["giver_id"] == my_id:
                n = msg["exchange"]["count"]
                low = sorted(msg["hand"], key=pres_rank)[:n]
                await client.send({"type": "president_exchange",
                                   "cards": low})
                continue
            if msg["turn_id"] != my_id:
                continue
            hand, trick = msg["hand"], msg["trick"]
            if not hand: continue  # finished: the v2.3 final president_state is informational, not a turn
            if trick is None:
                by_rank = {}
                for c in hand:
                    by_rank.setdefault(pres_rank(c), []).append(c)
                play = by_rank[min(by_rank)]
            else:
                play = president_lowest_legal(
                    hand, {"size": trick["size"], "rank": trick["rank"]})
            if play:
                await client.send({"type": "president_play", "cards": play})
            else:
                await client.send({"type": "president_pass"})
        elif t == "secret_word":
            my_word = msg.get("word")
        elif t == "secret_clue_turn":
            if msg["player_id"] == my_id:
                clue = "thing"
                if my_word:
                    clue = next((c for c in SECRET_CLUES.get(my_word, [])
                                 if c not in VAGUE_CLUES_80), "thing")
                await client.send({"type": "secret_clue", "clue": clue})
        elif t == "secret_vote_start":
            players = msg["players"]
            alive_ids = [p["player_id"] for p in players]
            if my_id not in alive_ids:
                continue
            target = state.get("secret_target")
            if target is None or target == my_id \
                    or target not in alive_ids:
                target = next((pid for pid in alive_ids if pid != my_id),
                              None)
            if target:
                await client.send({"type": "secret_vote",
                                   "player_id": target})
        elif t == "vote_break":
            await client.send({"type": "vote_next", "index": msg["index"]})
        elif t == "end_choice":
            # Every human votes the same way (the replay needs >= half
            # of connected humans; the host alone cannot carry it).
            # Only the host counts the replay against the 3-replay cap.
            if state["want_again"] and state["replays"] < 3:
                if client is state.get("host_client"):
                    state["replays"] += 1
                await client.send({"type": "replay_vote",
                                   "choice": "again"})
            else:
                await client.send({"type": "replay_vote",
                                   "choice": "lobby"})
        elif t == "lobby":
            if msg["room"]["phase"] == "lobby" and state["games"]:
                state["lobby"] = True
                return
        # reaction_wait, secret_round_result, secret_result,
        # president_notice, error: nothing to do


async def run_end_flow_game(game, settings, n_bots):
    """v2.3 item 12: play one full game of `game` with
    any_game_driver (2 humans + `n_bots` bots), vote through the
    end-of-game NEXT vote, take end_choice "again" (same game
    replays), play it a second time, then take end_choice "lobby"
    and end back in the lobby. Asserts every game played was the
    same game and the room ends in the lobby. (Elimination is
    covered separately by its own end vote in the main Elimination
    test.)"""
    names = ["EFHost", "EFP2"]
    colours = ["#e74c3c", "#3498db"]
    clients = []
    ids = []
    code = None
    for i in range(2):
        c = Client(f"ef-{game}-{i}")
        await c.connect()
        if i == 0:
            await c.send({"type": "create_room", "nickname": names[i],
                          "colour": colours[i]})
        else:
            await c.send({"type": "join_room", "code": code,
                          "nickname": names[i], "colour": colours[i]})
        j = await c.wait_for("joined")
        if i == 0:
            code = j["code"]
        ids.append(j["player_id"])
        clients.append(c)
    host = clients[0]
    for k in range(1, n_bots + 1):
        await host.send({"type": "add_bot"})
        await host.wait_lobby_count(2 + k)
    await host.send({"type": "update_settings", "game": game,
                     "game_settings": settings})
    await host.wait_lobby_where(lambda r: r.get("game") == game)
    await host.send({"type": "start_game"})
    started = await host.wait_for("game_started", timeout=5)
    check(f"v2.3 item 12 end flow ({game}): game starts",
          started["game"] == game, str(started))
    state = {"games": [], "want_again": True, "replays": 0,
             "lobby": False, "host_client": host, "secret_target": None}

    async def _stop_replaying():
        # Flip to "lobby" as soon as the replay has started, so the
        # second game's end_choice returns to the lobby. Poll fast:
        # a 1-question quiz can reach its second end_choice in ~2s.
        while not state["lobby"]:
            if len(state["games"]) >= 2:
                state["want_again"] = False
            await asyncio.sleep(0.05)

    tasks = [asyncio.create_task(any_game_driver(
                 c, pid, state, mem_correct=(i == 0)))
             for i, (c, pid) in enumerate(zip(clients, ids))]
    stopper = asyncio.create_task(_stop_replaying())
    try:
        await asyncio.wait_for(asyncio.gather(*tasks), timeout=600)
    finally:
        stopper.cancel()
        await asyncio.gather(stopper, return_exceptions=True)
    check(f"v2.3 item 12 end flow ({game}): replayed the SAME game "
          "via 'again', then returned to the lobby via 'lobby'",
          len(state["games"]) >= 2
          and all(g == game for g in state["games"])
          and state["lobby"] is True,
          str(state["games"]))
    for c in clients:
        await c.close()


def solve_math(text):
    # Simple a op b questions (Easy/Medium), e.g. "12 + 5 = ?",
    # "-12 + 5 = ?", "8 - (-7) = ?", "8 \u00d7 3 = ?", "20 \u00f7 4 = ?"
    expr = text.split("=")[0].strip()
    expr = expr.replace("\u2212", "-").replace("\u00d7", "*").replace("\u00f7", "/")
    expr = "".join(ch for ch in expr if ch.isdigit() or ch in "+-*/(). ")
    return eval(expr)  # noqa: S307 - sanitised to digits/operators only


def eval_hard_math(text, answer):
    """Independently verify a Hard question's answer. Returns True if
    the supplied answer matches the text, recomputed from scratch."""
    t = text
    if "Find f(" in t:
        m = re.match(r"f\(x\) = (-?\d+)x(?: \+ (-?\d+)| - (\d+))\. Find f\((-?\d+)\)", t)
        a = int(m.group(1)); b = int(m.group(2)) if m.group(2) else -int(m.group(3)); x = int(m.group(4))
        return answer == a * x + b
    if t.startswith("Solve for x:"):
        m = re.match(r"Solve for x: (-?\d+)x(?: \+ (-?\d+)| - (\d+)) = (-?\d+), x =", t)
        a = int(m.group(1)); b = int(m.group(2)) if m.group(2) else -int(m.group(3)); c = int(m.group(4))
        return answer == (c - b) // a and (c - b) % a == 0
    if "maximum value of f(x)" in t or "minimum value of f(x)" in t:
        # Vertex form f(x) = a(x-h)^2 + k ; the max/min value is k.
        m = re.search(r"\)\^2(?: \+ (-?\d+)| - (\d+))$", t)
        k = int(m.group(1)) if m.group(1) else -int(m.group(2))
        return answer == k
    if "/" in t and "decimal to 2" in t:
        m = re.match(r"(\d+)/(\d+) ([+-]) (\d+)/(\d+)", t)
        n1, d1, op, n2, d2 = int(m.group(1)), int(m.group(2)), m.group(3), int(m.group(4)), int(m.group(5))
        val = Fraction(n1, d1) + Fraction(n2, d2) if op == "+" else Fraction(n1, d1) - Fraction(n2, d2)
        return abs(float(answer) - round(float(val), 2)) <= 0.01
    # BEDMAS: parenthesised / mixed-operator arithmetic
    expr = t.split("=")[0].strip()
    expr = expr.replace("\u2212", "-").replace("\u00d7", "*").replace("\u00f7", "/")
    expr = "".join(ch for ch in expr if ch.isdigit() or ch in "+-*/(). ")
    return answer == eval(expr)  # noqa: S307 - sanitised


TRIVIA_ANSWER = {q["question"]: q["answer"] for q in QUESTIONS}


def _group_totals(scores, groups):
    """{group: total points} from a {player_id: score} map and a
    {player_id: group} map (group keys may be str after JSON)."""
    totals = {}
    for pid, s in scores.items():
        g = groups.get(pid, groups.get(str(pid)))
        if g is None:
            continue
        totals[g] = totals.get(g, 0) + s
    return totals


def _unique_min(mapping):
    """(key, value) of the unique minimum of a dict, or None if the
    minimum is tied or the dict is empty."""
    if not mapping:
        return None
    m = min(mapping.values())
    ks = [k for k, v in mapping.items() if v == m]
    return (ks[0], m) if len(ks) == 1 else None


async def group_elim_driver(client, my_id, is_host, state):
    """v2.3 item 14: drive one human through a Group Elimination
    series. The HOST plays every challenge perfectly (fast correct
    quiz answers, instant reaction clicks, correct Memory recall)
    and the other human scores ~0 (wrong quiz answers, no reaction
    clicks, wrong Memory recall), so in quiz challenges the host's
    team is far ahead and the other human's team is worst with a
    unique lowest player — the deterministic setup for verifying
    the server's worst-team / worst-player picks from the scores
    the server itself broadcasts. Both humans vote NEXT on every
    vote break and vote "lobby" at the end vote. Only the host
    records the shared events (challenges, eliminations, breaks,
    question results, Memory rounds, results) so nothing is
    double-counted. Returns when the room is back in the lobby."""
    number = None
    cur = 0  # challenges seen so far (tags every later event)
    while True:
        msg = await wait_any(client, (
            "question", "question_result", "reaction_go",
            "reaction_round_result",
            "memory_show", "memory_recall", "memory_round_result",
            "elimination_challenge", "elimination_eliminated",
            "vote_break", "results", "elimination_end_vote",
            "lobby"), timeout=180)
        t = msg["type"]
        if t == "question":
            if msg.get("options"):
                correct = TRIVIA_ANSWER.get(msg["text"])
                ans = correct if correct is not None else 0
                await client.send({"type": "answer",
                                   "answer": ans if is_host
                                   else (ans + 1) % 4,
                                   "index": msg["index"]})
            else:
                aq = solve_math(msg["text"])
                await client.send({"type": "answer",
                                   "answer": str(aq) if is_host
                                   else str(aq + 1),
                                   "index": msg["index"]})
        elif t == "question_result":
            if is_host:
                state["qresults"].append(
                    {"n": cur, "index": msg["index"],
                     "total": msg["total"],
                     "scores": {s["player_id"]: s["score"]
                                for s in msg["scores"]}})
        elif t == "reaction_go":
            if is_host:
                await client.send({"type": "reaction_click"})
        elif t == "reaction_round_result":
            if is_host:
                state["reaction_rounds"].append(
                    {"n": cur, "round": msg["round"],
                     "results": list(msg["results"])})
        elif t == "memory_show":
            number = msg["number"]
        elif t == "memory_recall":
            await client.send({"type": "memory_answer",
                               "answer": number if is_host else "0000"})
        elif t == "memory_round_result":
            if is_host:
                state["mem_rounds"].append(
                    {"n": cur, "round": msg["round"],
                     "survivors": list(msg["survivors"]),
                     "eliminated": list(msg["eliminated"])})
        elif t == "elimination_challenge":
            cur += 1
            if is_host:
                state["challenges"].append(
                    {"n": cur, "game": msg["game"],
                     "elimination_mode": msg.get("elimination_mode"),
                     "elim_phase": msg.get("elim_phase"),
                     "groups": dict(msg.get("groups") or {}),
                     "remaining": msg.get("remaining")})
        elif t == "elimination_eliminated":
            if is_host:
                state["eliminated"].append(
                    {"n": cur, "nickname": msg["nickname"],
                     "player_id": msg["player_id"],
                     "group": msg.get("group"),
                     "remaining": msg.get("remaining")})
        elif t == "vote_break":
            await client.send({"type": "vote_next",
                               "index": msg["index"]})
            if is_host and msg.get("elim_break"):
                state["breaks"].append(
                    {"n": cur, "eliminated": list(msg.get("eliminated") or []),
                     "remaining": msg.get("remaining"),
                     "group_totals": dict(msg.get("group_totals") or {}),
                     "worst_team": msg.get("worst_team"),
                     "transfers": list(msg.get("transfers") or []),
                     "merged_to_black": msg.get("merged_to_black"),
                     "scores": list(msg.get("scores") or [])})
        elif t == "results":
            if is_host:
                state["results"].append(msg)
        elif t == "elimination_end_vote":
            await client.send({"type": "elimination_vote",
                               "choice": "lobby"})
        elif t == "lobby":
            if is_host:
                state["last_lobby"] = msg["room"]
            if msg["room"]["phase"] == "lobby" and state["results"]:
                state["lobby"] = True
                return


async def run_group_series(tag, n_bots, make_groups, *, until_merge=False):
    """v2.3 item 14: run one Group Elimination series end to end.

    2 humans + `n_bots` Hard bots; the host sets Elimination Group
    mode with the host-set groups from make_groups(players) (the
    lobby player dicts) and a 3s vote timeout, then starts the
    series. Returns (state, players). With until_merge=True the
    drivers are stopped (and the clients closed) as soon as the
    3rd challenge is announced — i.e. just after the merge into
    Black — so a series that only needs its groups phase does not
    have to play out to a winner."""
    names = [f"{tag}Host", f"{tag}P2"]
    colours = ["#e74c3c", "#3498db"]
    clients = []
    ids = []
    code = None
    for i in range(2):
        c = Client(f"grp-{tag}-{i}")
        await c.connect()
        if i == 0:
            await c.send({"type": "create_room", "nickname": names[i],
                          "colour": colours[i]})
        else:
            await c.send({"type": "join_room", "code": code,
                          "nickname": names[i], "colour": colours[i]})
        j = await c.wait_for("joined")
        if i == 0:
            code = j["code"]
        ids.append(j["player_id"])
        clients.append(c)
    host = clients[0]
    lobby = None
    for k in range(1, n_bots + 1):
        await host.send({"type": "add_bot"})
        lobby = await host.wait_lobby_count(2 + k)
    players = list(lobby["room"]["players"])
    for p in players:
        if p["is_bot"]:
            await host.send({"type": "update_bot", "bot_id": p["id"],
                             "difficulty": "Hard"})
            await host.wait_lobby_count(2 + n_bots)
    groups = make_groups(players)
    await host.send({"type": "update_settings", "game": "elimination",
                     "game_settings": {"elimination": {"vote_timeout": 3}},
                     "elimination_mode": "group",
                     "elim_groups": groups})
    await host.wait_lobby_where(
        lambda r: r.get("elimination_mode") == "group"
        and r.get("elim_groups") == groups)
    state = {"challenges": [], "eliminated": [], "breaks": [],
             "qresults": [], "mem_rounds": [], "reaction_rounds": [],
             "results": [], "lobby": False, "last_lobby": None}
    tasks = [asyncio.create_task(group_elim_driver(
                 c, pid, i == 0, state))
             for i, (c, pid) in enumerate(zip(clients, ids))]
    await host.send({"type": "start_game"})
    deadline = time.monotonic() + 600
    try:
        while time.monotonic() < deadline:
            if until_merge and len(state["challenges"]) >= 3:
                break
            if not until_merge and state["lobby"]:
                break
            await asyncio.sleep(0.5)
        else:
            raise AssertionError(
                f"{tag}: group series did not finish in 600s: "
                f"challenges={state['challenges']} "
                f"eliminated={state['eliminated']} lobby={state['lobby']}")
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        for c in clients:
            await c.close()
    return state, players


def verify_group_series(tag, state, players, *, expect_full):
    """v2.3 item 14: verify one recorded Group series from the data
    the server itself broadcast. Checks the challenge wire, the
    elimination wire, every break's group extras, the rebalance /
    merge transitions, and — wherever the broadcast scores make the
    pick uniquely determined — that the eliminated player really
    was the worst player of the worst team. Reaction challenges
    assign interpolated points, and their round results let the
    scores be reconstructed (like the quiz question results);
    Memory challenges are only verifiable when they end with a
    single winner and no round was replayed after a tie. Returns
    the number of challenges whose victim was uniquely determined
    and matched."""
    by_id = {p["id"]: p["nickname"] for p in players}
    by_nick = {p["nickname"]: p["id"] for p in players}
    verified = 0
    skipped = []
    for ch in state["challenges"]:
        n = ch["n"]
        check(f"v2.3 item 14 ({tag}): challenge {n} carries group "
              "mode / phase / groups on the wire",
              ch["elimination_mode"] == "group"
              and ch["elim_phase"] in ("groups", "black")
              and isinstance(ch["groups"], dict)
              and len(ch["groups"]) == ch["remaining"],
              str(ch))
        if ch["remaining"] > 5:
            check(f"v2.3 item 14 ({tag}): challenge {n} is in the "
                  "groups phase above 5 alive",
                  ch["elim_phase"] == "groups"
                  and set(ch["groups"].values()) <= {0, 1},
                  str(ch))
        else:
            check(f"v2.3 item 14 ({tag}): challenge {n} is in the "
                  "Black phase at 5 or fewer alive",
                  ch["elim_phase"] == "black"
                  and set(ch["groups"].values()) == {2},
                  str(ch))
    for elim in state["eliminated"]:
        n = elim["n"]
        ch = state["challenges"][n - 1]
        check(f"v2.3 item 14 ({tag}): elimination {n} carries the "
              "eliminated player's group on the wire",
              elim["group"] == (2 if ch["elim_phase"] == "black"
                                else ch["groups"].get(elim["player_id"]))
              and elim["remaining"] == ch["remaining"] - 1,
              str(elim))
    # One break per elimination, in order; the merge fires exactly
    # on the break that leaves 5 alive.
    check(f"v2.3 item 14 ({tag}): one group break per elimination",
          len(state["breaks"]) == len(state["eliminated"])
          and [b["n"] for b in state["breaks"]]
          == [e["n"] for e in state["eliminated"]],
          f"breaks={[b['n'] for b in state['breaks']]} "
          f"eliminated={[e['n'] for e in state['eliminated']]}")
    for b in state["breaks"]:
        n = b["n"]
        elim = state["eliminated"][n - 1]
        ch = state["challenges"][n - 1]
        check(f"v2.3 item 14 ({tag}): break {n} carries group_totals "
              "/ worst_team / transfers / merged_to_black",
              isinstance(b["group_totals"], dict)
              and b["worst_team"] in (0, 1, 2)
              and isinstance(b["transfers"], list)
              and isinstance(b["merged_to_black"], bool)
              and b["eliminated"] == [elim["nickname"]]
              and b["remaining"] == elim["remaining"],
              str({k: b[k] for k in
                   ("group_totals", "worst_team", "transfers",
                    "merged_to_black", "eliminated", "remaining")}))
        check(f"v2.3 item 14 ({tag}): break {n} merges into Black "
              "exactly when 5 players remain",
              b["merged_to_black"] is (b["remaining"] == 5),
              str(b))
        # The break's score rows carry each surviving player's group.
        teams = {s["player_id"]: s["team"] for s in b["scores"]}
        if b["merged_to_black"] or ch["elim_phase"] == "black":
            check(f"v2.3 item 14 ({tag}): break {n} shows every "
                  "survivor in the Black Team after the merge",
                  teams and all(v == 2 for v in teams.values()),
                  str(teams))
        else:
            sizes = {0: 0, 1: 0}
            for v in teams.values():
                sizes[v] = sizes.get(v, 0) + 1
            check(f"v2.3 item 14 ({tag}): break {n} leaves the "
                  "groups balanced (gap <= 1) after any rebalance",
                  set(teams.values()) <= {0, 1}
                  and abs(sizes.get(0, 0) - sizes.get(1, 0)) <= 1,
                  str(teams))
        # Rebalance simulation: start from the challenge groups
        # minus the eliminated player, apply the reported transfers
        # in order — each must move a player from the currently
        # bigger group to the smaller — and end balanced. When the
        # break merged, the transfers must be empty instead.
        if b["merged_to_black"]:
            check(f"v2.3 item 14 ({tag}): merge break {n} has no "
                  "rebalance transfers (the merge replaces them)",
                  b["transfers"] == [], str(b["transfers"]))
        else:
            sim = {pid: g for pid, g in ch["groups"].items()
                   if pid != elim["player_id"]}
            for tr in b["transfers"]:
                check(f"v2.3 item 14 ({tag}): break {n} transfer "
                      "moves a player from the bigger group to the "
                      "smaller one",
                      tr["from"] in (0, 1) and tr["to"] in (0, 1)
                      and tr["from"] != tr["to"]
                      and sim.get(tr["player_id"]) == tr["from"]
                      and sum(1 for v in sim.values()
                              if v == tr["from"])
                      > sum(1 for v in sim.values()
                            if v == tr["to"]),
                      str(tr))
                sim[tr["player_id"]] = tr["to"]
            sizes = {0: sum(1 for v in sim.values() if v == 0),
                     1: sum(1 for v in sim.values() if v == 1)}
            check(f"v2.3 item 14 ({tag}): break {n} transfers end "
                  "with the groups balanced (gap <= 1)",
                  abs(sizes[0] - sizes[1]) <= 1, str(sim))
            nxt = next((c for c in state["challenges"]
                        if c["n"] == n + 1), None)
            if nxt is not None:
                check(f"v2.3 item 14 ({tag}): the next challenge "
                      f"({n + 1}) uses the post-transfer groups",
                      nxt["groups"] == sim, str(nxt["groups"]))
        # --- victim check, wherever the scores determine it ---
        game = ch["game"]
        scores = None
        if game in ("math", "trivia"):
            finals = [q for q in state["qresults"]
                      if q["n"] == n and q["index"] == q["total"]]
            if finals:
                scores = finals[-1]["scores"]
        elif game == "memory":
            rounds = [m for m in state["mem_rounds"] if m["n"] == n]
            rnd_nos = [m["round"] for m in rounds]
            if rounds and len(set(rnd_nos)) == len(rnd_nos) \
                    and len(rounds[-1]["survivors"]) == 1:
                scores = {rounds[-1]["survivors"][0]: 1000}
                for m in rounds:
                    for pid in m["eliminated"]:
                        scores[pid] = 100 * (m["round"] - 1)
        elif game == "reaction":
            # v2.3 item 14 fix: Reaction challenges assign the
            # interpolated points, so the scores can be rebuilt from
            # the per-round ms values exactly as the server does.
            rounds = [r for r in state.get("reaction_rounds", [])
                      if r["n"] == n]
            if rounds:
                times = {}
                pids = set()
                for r in rounds:
                    for row in r["results"]:
                        pids.add(row["player_id"])
                        if row.get("ms") is not None:
                            times.setdefault(row["player_id"],
                                             []).append(row["ms"])
                avgs = {pid: (round(sum(times[pid]) / len(times[pid]))
                              if pid in times else None)
                        for pid in pids}
                valid = [a for a in avgs.values() if a is not None]
                if valid:
                    best, worst = min(valid), max(valid)
                    scores = {}
                    for pid, avg in avgs.items():
                        if avg is None:
                            scores[pid] = 0
                        elif best == worst:
                            scores[pid] = 1000
                        else:
                            scores[pid] = round(
                                1000 * (worst - avg) / (worst - best))
                else:
                    scores = {pid: 0 for pid in pids}
        if scores is None:
            skipped.append((n, game, "scores not reconstructible"))
            continue
        totals = _group_totals(scores, ch["groups"])
        worst = _unique_min(totals)
        if worst is None:
            skipped.append((n, game, f"tied totals {totals}"))
            continue
        worst_team, _ = worst
        in_team = {pid: s for pid, s in scores.items()
                   if ch["groups"].get(pid) == worst_team}
        lowest = _unique_min(in_team)
        if lowest is None:
            skipped.append((n, game,
                            f"tied lowest in team {worst_team}: {in_team}"))
            continue
        check(f"v2.3 item 14 ({tag}): challenge {n} ({game}) break "
              "reports the true group totals and worst team",
              b["group_totals"] == {str(k): v
                                    for k, v in totals.items()}
              and b["worst_team"] == worst_team,
              f"break={b['group_totals']}/{b['worst_team']} "
              f"computed={totals}/{worst_team}")
        check(f"v2.3 item 14 ({tag}): challenge {n} ({game}) "
              "eliminates the worst player of the worst team",
              elim["player_id"] == lowest[0],
              f"eliminated={elim['nickname']} "
              f"lowest={by_id.get(lowest[0])} ({lowest[1]} pts) "
              f"team {worst_team} scores={in_team}")
        verified += 1
    if expect_full:
        check(f"v2.3 item 14 ({tag}): a full series eliminates one "
              "player per challenge until 1 remains",
              len(state["eliminated"]) == len(players) - 1
              and len(state["challenges"]) == len(players) - 1,
              f"challenges={len(state['challenges'])} "
              f"eliminated={len(state['eliminated'])} "
              f"players={len(players)}")
        check(f"v2.3 item 14 ({tag}): every player is eliminated "
              "exactly once except the winner",
              len({e["player_id"] for e in state["eliminated"]})
              == len(state["eliminated"]) == len(players) - 1,
              str([e["nickname"] for e in state["eliminated"]]))
        res = state["results"][0]
        fin = res.get("finishing_order", [])
        winner = fin[0]["player_id"] if fin else None
        expect_fin = [winner] + [e["player_id"]
                                 for e in reversed(state["eliminated"])]
        check(f"v2.3 item 14 ({tag}): finishing order is the winner "
              "then the eliminated in reverse order, winner 1000 / "
              "everyone else 0",
              len(fin) == len(players)
              and len({r["player_id"] for r in fin}) == len(players)
              and [r["player_id"] for r in fin] == expect_fin
              and res.get("victor") == winner
              and fin[0]["score"] == 1000
              and all(r["score"] == 0 for r in fin[1:]),
              str([(r["nickname"], r["score"]) for r in fin]))
    return verified, skipped


async def main():
    check("trivia bank has 144 questions (36 per category, no duplicates)",
          len(QUESTIONS) == 144
          and len({q["question"] for q in QUESTIONS}) == 144
          and all(len(q["options"]) == 4 and 0 <= q["answer"] <= 3
                  for q in QUESTIONS),
          f"{len(QUESTIONS)} questions")

    # --- start the server ---
    # v1.2: short disconnect/room graces so the stale-nickname and
    # room-deletion tests finish in seconds, not minutes.
    import os
    os.environ["PARTY_GRACE_SECONDS"] = "3"
    os.environ["PARTY_ROOM_GRACE_SECONDS"] = "6"
    proc = subprocess.Popen(
        [str(ROOT / ".venv" / "bin" / "uvicorn"), "server:app",
         "--host", "127.0.0.1", "--port", str(PORT)],
        cwd=str(ROOT), stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    try:
        # trust_env=False: the sandbox sets a proxy in env vars whose
        # IPv6 no_proxy entries httpx cannot parse; localhost needs no proxy.
        async with httpx.AsyncClient(trust_env=False) as http:
            for _ in range(50):
                try:
                    r = await http.get(f"{BASE}/api/health")
                    if r.status_code == 200:
                        break
                except Exception:
                    pass
                await asyncio.sleep(0.2)
            else:
                raise AssertionError("server did not start")
            r = await http.get(f"{BASE}/")
            check("home page serves (HTTP 200, contains title)",
                  r.status_code == 200 and "Party Game Hub" in r.text,
                  f"status={r.status_code}")
            check("health endpoint", (await http.get(f"{BASE}/api/health")).json().get("ok") is True)

        # --- create room + join 2nd human ---
        host = Client("host"); await host.connect()
        await host.send({"type": "create_room", "nickname": "Matthew",
                         "colour": "#e74c3c", "room_name": "Test Room"})
        joined = await host.wait_for("joined")
        code = joined["code"]
        check("room code is 6 chars", len(code) == 6, f"code={code}")
        check("host is host in lobby", joined["room"]["players"][0]["is_host"])

        p2 = Client("player2"); await p2.connect()
        await p2.send({"type": "join_room", "code": code,
                       "nickname": "Cousin1", "colour": "#3498db"})
        await p2.wait_for("joined")
        lobby = await host.wait_lobby_count(2)
        check("lobby lists 2 humans",
              len(lobby["room"]["players"]) == 2,
              str([p["nickname"] for p in lobby["room"]["players"]]))

        # --- non-host cannot add a bot ---
        await p2.send({"type": "add_bot"})
        err = await p2.wait_for("error")
        check("non-host add_bot is rejected", "Only the host" in err["message"])

        # --- host adds 2 bots, edits one ---
        await host.send({"type": "add_bot"})
        lobby = await host.wait_lobby_count(3)
        await host.send({"type": "add_bot"})
        lobby = await host.wait_lobby_count(4)
        bots = [p for p in lobby["room"]["players"] if p["is_bot"]]
        check("2 bots added, 4 players total",
              len(bots) == 2 and len(lobby["room"]["players"]) == 4,
              str([(p["nickname"], p["is_bot"]) for p in lobby["room"]["players"]]))
        check("bots are marked BOT", all(b["is_bot"] for b in bots))
        bot_id = bots[0]["id"]
        await host.send({"type": "update_bot", "bot_id": bot_id,
                         "nickname": "TestBot", "difficulty": "Hard"})
        lobby = await host.wait_lobby_count(4)
        edited = [p for p in lobby["room"]["players"] if p["id"] == bot_id][0]
        check("host can edit bot nickname + difficulty",
              edited["nickname"] == "TestBot" and edited["difficulty"] == "Hard")
        # Make both bots Hard so quiz rounds finish fast in the test.
        for b in bots:
            await host.send({"type": "update_bot", "bot_id": b["id"], "difficulty": "Hard"})
            await host.wait_lobby_count(4)

        # --- Reaction Test: 3 rounds ---
        await host.send({"type": "update_settings", "game": "reaction",
                         "game_settings": {"reaction": {"rounds": 3}}})
        await host.wait_lobby()
        await host.send({"type": "start_game"})
        started = await host.wait_for("game_started")
        check("Reaction Test starts", started["game"] == "reaction")
        for rnd in range(1, 4):
            await host.wait_for("reaction_wait", timeout=10)
            await host.wait_for("reaction_go", timeout=10)
            # Both humans click as soon as the light goes green.
            await host.send({"type": "reaction_click"})
            await p2.send({"type": "reaction_click"})
            res = await host.wait_for("reaction_round_result", timeout=10)
            check(f"reaction round {rnd} result has all 4 players",
                  len(res["results"]) == 4)
            if rnd < 3:
                # v2: NEXT vote break between rounds (not after the last).
                vb = await host.wait_for("vote_break", timeout=10)
                check(f"reaction round {rnd} vote break shown",
                      vb["votes_needed"] == 1, str(vb))
                if rnd == 1:
                    await host.send({"type": "vote_next", "index": rnd})
                    await p2.send({"type": "vote_next", "index": rnd})
                else:
                    # Round 2: the host's Skip override advances instead.
                    await host.send({"type": "host_skip"})
        results = await host.wait_for("results", timeout=10)
        check("Reaction Test produces results",
              len(results["rankings"]) == 4 and results["game"] == "reaction",
              str([(r["nickname"], r["avg_ms"]) for r in results["rankings"]]))
        check("reaction rankings have an average time",
              any(r["avg_ms"] is not None for r in results["rankings"]))
        # --- v2.2 §9: Reaction points are interpolated by average
        # time — best 1000, worst 0, all-equal 1000, no valid time 0.
        # Recompute the formula from the broadcast averages and
        # compare it exactly against the broadcast scores.
        _avgs = [r["avg_ms"] for r in results["rankings"]
                 if r["avg_ms"] is not None]
        _best, _worst = min(_avgs), max(_avgs)
        _formula_ok = True
        for r in results["rankings"]:
            if r["avg_ms"] is None:
                _exp = 0
            elif _best == _worst:
                _exp = 1000
            else:
                _exp = round(1000 * (_worst - r["avg_ms"]) / (_worst - _best))
            if r["score"] != _exp:
                _formula_ok = False
        check("v2.2 Reaction points match the interpolation formula "
              "(best 1000 / worst 0 / no valid time 0)",
              _formula_ok and results["rankings"][0]["score"] == 1000,
              str([(r["nickname"], r["avg_ms"], r["score"])
                   for r in results["rankings"]]))
        check("v2.2 Reaction rankings are ordered by average time",
              [r["avg_ms"] for r in results["rankings"]]
              == sorted([r["avg_ms"] for r in results["rankings"]],
                        key=lambda a: (a is None, a or 0)),
              str([(r["nickname"], r["avg_ms"]) for r in results["rankings"]]))

        # --- back to lobby, Speed Math: 5 questions ---
        # v2.2: play_again is gone — results -> end-of-game NEXT vote
        # -> end_choice -> replay_vote "lobby" -> lobby.
        await drive_end_flow(host, [host, p2], choice="lobby", count=4)
        await host.send({"type": "update_settings", "game": "math",
                         "game_settings": {"math": {"difficulty": "Easy",
                                                    "questions": 5,
                                                    "time_per_question": 10}}})
        await host.wait_lobby()
        await host.send({"type": "start_game"})
        started = await host.wait_for("game_started")
        check("Speed Math starts", started["game"] == "math")
        for q in range(1, 6):
            msg = await host.wait_for("question", timeout=15)
            check(f"math Q{q} received", "?" in msg["text"], msg["text"])
            ans = solve_math(msg["text"])
            await host.send({"type": "answer", "answer": str(ans)})
            await p2.send({"type": "answer", "answer": str(ans + 1)})  # wrong on purpose
            res = await host.wait_for("question_result", timeout=15)
            check(f"math Q{q} scored", any(p["points"] > 0 for p in res["points"]),
                  str(res["points"]))
            if q < 5:
                vb = await host.wait_for("vote_break", timeout=10)
                check(f"math Q{q} vote break shown, needs half of HUMANS only",
                      vb["votes_needed"] == 1, str(vb))
                await host.send({"type": "vote_next", "index": q})
                await p2.send({"type": "vote_next", "index": q})
        results = await host.wait_for("results", timeout=10)
        check("Speed Math produces results with scores",
              len(results["rankings"]) == 4 and results["rankings"][0]["score"] > 0,
              str([(r["nickname"], r["score"]) for r in results["rankings"]]))
        check("fast correct human outscores wrong human",
              results["rankings"][0]["nickname"] == "Matthew")

        # --- Speed Trivia: 5 questions ---
        # v2.2: the end-of-game vote is passed with vote_next here
        # (not host_skip) — vote_next must work in phase "results".
        await drive_end_flow(host, [host, p2], choice="lobby",
                             method="vote", count=4)
        await host.send({"type": "update_settings", "game": "trivia",
                         "game_settings": {"trivia": {"category": "Mixed",
                                                      "difficulty": "Easy",
                                                      "questions": 5,
                                                      "time_per_question": 10}}})
        await host.wait_lobby()
        await host.send({"type": "start_game"})
        started = await host.wait_for("game_started")
        check("Speed Trivia starts", started["game"] == "trivia")
        for q in range(1, 6):
            msg = await host.wait_for("question", timeout=15)
            check(f"trivia Q{q} has 4 options", len(msg.get("options") or []) == 4, msg["text"])
            await host.send({"type": "answer", "answer": 0})
            await p2.send({"type": "answer", "answer": 1})
            res = await host.wait_for("question_result", timeout=15)
            check(f"trivia Q{q} result has a correct answer",
                  res["correct_answer"] in (0, 1, 2, 3))
            if q < 5:
                vb = await host.wait_for("vote_break", timeout=10)
                check(f"trivia Q{q} vote break shown (humans only)",
                      vb["votes_needed"] == 1, str(vb))
                await host.send({"type": "vote_next", "index": q})
                await p2.send({"type": "vote_next", "index": q})
        results = await host.wait_for("results", timeout=10)
        check("Speed Trivia produces results",
              len(results["rankings"]) == 4,
              str([(r["nickname"], r["score"]) for r in results["rankings"]]))

        # --- back to lobby, same players still there ---
        _, lobby = await drive_end_flow(host, [host, p2], choice="lobby",
                                        count=4)
        check("v2.2 end flow returns to lobby with same 4 players",
              len(lobby["room"]["players"]) == 4 and lobby["room"]["phase"] == "lobby")

        # --- v1.1: ping/pong keep-alive ---
        await host.send({"type": "ping"})
        pong = await host.wait_for("pong", timeout=5)
        check("ping gets a pong", pong["type"] == "pong")

        # --- v1.1: lobby drop reclaims the same seat within the grace period ---
        p2_id = [p["id"] for p in lobby["room"]["players"]
                 if p["nickname"] == "Cousin1"][0]
        await p2.close()
        await asyncio.sleep(1)
        p2b = Client("player2-rejoin"); await p2b.connect()
        await p2b.send({"type": "join_room", "code": code,
                        "nickname": "Cousin1", "colour": "#3498db"})
        rejoined = await p2b.wait_for("joined", timeout=5)
        check("lobby rejoin reclaims the same player id",
              rejoined["player_id"] == p2_id,
              f"got {rejoined['player_id']}, expected {p2_id}")
        lobby = await host.wait_lobby_count(4)
        check("lobby still has 4 players after rejoin (no duplicate)",
              len(lobby["room"]["players"]) == 4)

        # --- v1.1: a stale/late answer must not leak into the next question ---
        await host.send({"type": "update_settings", "game": "math",
                         "game_settings": {"math": {"difficulty": "Easy",
                                                    "questions": 2,
                                                    "time_per_question": 10}}})
        await host.wait_lobby()
        await host.send({"type": "start_game"})
        await host.wait_for("game_started")
        msg = await host.wait_for("question", timeout=15)
        ans = solve_math(msg["text"])
        # Send a stale answer for a non-existent question first: it must
        # be ignored, not recorded, so the real answer still counts.
        await host.send({"type": "answer", "answer": str(ans + 1), "index": 99})
        await host.send({"type": "answer", "answer": str(ans), "index": msg["index"]})
        await p2b.send({"type": "answer", "answer": str(ans + 1), "index": msg["index"]})
        res = await host.wait_for("question_result", timeout=15)
        check("stale answer ignored, real answer still scores",
              any(p["points"] > 0 for p in res["points"]),
              str(res["points"]))
        # v1.2: bots no longer auto-vote, so a human must vote NEXT
        # to leave the break (1 of 2 humans is enough).
        await host.wait_for("vote_break", timeout=10)
        await host.send({"type": "vote_next", "index": 1})
        # Q2: an answer tagged with Q1's index must also be ignored.
        msg2 = await host.wait_for("question", timeout=15)
        ans2 = solve_math(msg2["text"])
        await host.send({"type": "answer", "answer": str(ans2), "index": 1})
        await host.send({"type": "answer", "answer": str(ans2), "index": msg2["index"]})
        await p2b.send({"type": "answer", "answer": str(ans2 + 1), "index": msg2["index"]})
        res2 = await host.wait_for("question_result", timeout=15)
        check("answer with previous question's index ignored, current scores",
              any(p["points"] > 0 for p in res2["points"]),
              str(res2["points"]))
        results = await host.wait_for("results", timeout=10)
        check("stale-answer game produces results",
              len(results["rankings"]) == 4)

        # ================= v1.1 tests =================
        # --- Fix 4: lobby profile editing (own nickname/colour only) ---
        # v2.2: play_again is gone — drive the end flow instead.
        _, lobby = await drive_end_flow(host, [host, p2b], choice="lobby",
                                        count=4)
        await p2b.send({"type": "update_profile", "nickname": "CousinOne",
                        "colour": "#2ecc71"})
        lobby = await host.wait_lobby_count(4)
        # Lobby broadcasts are queued, so the next one may be stale
        # (from before the edit). Keep reading until the new name appears.
        names = []
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            lb = await host.wait_for("lobby", timeout=deadline - time.monotonic())
            names = [pl["nickname"] for pl in lb["room"]["players"]]
            if "CousinOne" in names:
                lobby = lb
                break
        check("profile edit changes own nickname in lobby",
              "CousinOne" in names, str(names))
        # Nickname uniqueness: trying to take the host's name is rejected.
        await p2b.send({"type": "update_profile", "nickname": "Matthew"})
        err = await p2b.wait_for("error", timeout=5)
        check("profile edit rejects a taken nickname",
              "already taken" in err["message"], err["message"])
        await asyncio.sleep(0.5)
        check("taken nickname was not applied",
              [pl["nickname"] for pl in lobby["room"]["players"]].count("Matthew") == 1)

        # --- Fix 5: Speed Math difficulty generation (20 per level) ---
        for diff in ("Easy", "Medium", "Hard"):
            ok_count = 0
            kinds = set()
            for _ in range(20):
                text, ans = make_math_question(diff)
                if diff == "Easy":
                    val = solve_math(text)
                    check_ok = (val == ans and -30 <= ans <= 30)
                    # operands in -30..30: parse numbers in the text
                    nums = [int(n) for n in re.findall(r"-?\d+", text.split("=")[0])]
                    check_ok = check_ok and all(-30 <= n <= 30 for n in nums)
                    if check_ok:
                        ok_count += 1
                elif diff == "Medium":
                    val = solve_math(text)
                    if val == ans:
                        ok_count += 1
                else:
                    if eval_hard_math(text, ans):
                        ok_count += 1
                        kinds.add("function" if "Find f(" in text else
                                  "solve" if text.startswith("Solve") else
                                  "vertex" if "value of f(x)" in text else
                                  "fraction" if "decimal to 2" in text else "bedmas")
            check(f"Math {diff}: 20/20 generated questions verify independently",
                  ok_count == 20, f"{ok_count}/20")
            if diff == "Hard":
                check("Hard generation covers at least 3 question types in 20 draws",
                      len(kinds) >= 3, str(kinds))
        # Hard fraction scoring: decimal within 0.01 counts, 0.05 off does not.
        room_u = Room(code="TEST01")
        pl_u = Player(id="u1", nickname="Unit", colour="#fff")
        room_u.players.append(pl_u)
        room_u.phase = "playing"; room_u.current_game = "math"
        room_u.round_state = {"index": 1, "answers": {}, "start": time.monotonic(), "correct": 0.83}
        record_answer(room_u, pl_u, "0.83")
        check("Hard fraction answer 0.83 scores", pl_u.score > 0, f"score={pl_u.score}")
        pl_u2 = Player(id="u2", nickname="Unit2", colour="#fff")
        room_u.players.append(pl_u2)
        room_u.round_state["answers"] = {}
        record_answer(room_u, pl_u2, "0.88")
        check("Hard fraction answer 0.88 (0.05 off) scores 0", pl_u2.score == 0, f"score={pl_u2.score}")

        # --- Fix 2: one answer must NOT advance the round ---
        h2 = Client("fix2-host"); await h2.connect()
        await h2.send({"type": "create_room", "nickname": "Fix2Host", "colour": "#e74c3c"})
        j2 = await h2.wait_for("joined"); code2 = j2["code"]
        q2 = Client("fix2-p2"); await q2.connect()
        await q2.send({"type": "join_room", "code": code2, "nickname": "Fix2P2", "colour": "#3498db"})
        await q2.wait_for("joined")
        await h2.send({"type": "update_settings", "game": "math",
                       "game_settings": {"math": {"difficulty": "Easy", "questions": 2,
                                                  "time_per_question": 6, "vote_timeout": 3}}})
        await h2.wait_lobby()
        await h2.send({"type": "start_game"})
        await h2.wait_for("game_started")
        msg = await h2.wait_for("question", timeout=15)
        ans = solve_math(msg["text"])
        await h2.send({"type": "answer", "answer": str(ans), "index": msg["index"]})
        try:
            await h2.wait_for("question_result", timeout=2)
            check("Fix2: round does NOT advance after only one player answers", False,
                  "question_result arrived after a single answer")
        except AssertionError:
            check("Fix2: round does NOT advance after only one player answers", True)
        await q2.send({"type": "answer", "answer": str(ans + 1), "index": msg["index"]})
        res = await h2.wait_for("question_result", timeout=10)
        check("Fix2: round advances once the second player answers",
              res["index"] == 1)
        vb = await h2.wait_for("vote_break", timeout=10)
        await h2.send({"type": "vote_next", "index": 1})
        # p2 does not vote; host alone is half of 2, so it should advance.
        msg2 = await h2.wait_for("question", timeout=10)
        check("Fix3: one of two humans voting NEXT advances the game",
              msg2["index"] == 2)
        # Q2 is the last question: answer both, then results (no vote).
        ans2 = solve_math(msg2["text"])
        await h2.send({"type": "answer", "answer": str(ans2), "index": 2})
        await q2.send({"type": "answer", "answer": str(ans2 + 1), "index": 2})
        results = await h2.wait_for("results", timeout=10)
        check("Fix2/3 game produces results", len(results["rankings"]) == 2)
        await h2.close(); await q2.close()

        # --- Fix 3: vote timeout advances with no votes ---
        h3 = Client("fix3-host"); await h3.connect()
        await h3.send({"type": "create_room", "nickname": "Fix3Host", "colour": "#e74c3c"})
        j3 = await h3.wait_for("joined"); code3 = j3["code"]
        r3 = Client("fix3-p2"); await r3.connect()
        await r3.send({"type": "join_room", "code": code3, "nickname": "Fix3P2", "colour": "#3498db"})
        await r3.wait_for("joined")
        await h3.send({"type": "update_settings", "game": "math",
                       "game_settings": {"math": {"difficulty": "Easy", "questions": 2,
                                                  "time_per_question": 5, "vote_timeout": 3}}})
        await h3.wait_lobby()
        await h3.send({"type": "start_game"})
        await h3.wait_for("game_started")
        m3 = await h3.wait_for("question", timeout=15)
        a3 = solve_math(m3["text"])
        await h3.send({"type": "answer", "answer": str(a3), "index": m3["index"]})
        await r3.send({"type": "answer", "answer": str(a3 + 1), "index": m3["index"]})
        await h3.wait_for("question_result", timeout=10)
        vb3 = await h3.wait_for("vote_break", timeout=10)
        check("Fix3: vote break shows a 60s-or-configured timeout",
              vb3["timeout"] == 3, str(vb3))
        # Nobody votes: the game must advance at the timeout.
        t0 = time.monotonic()
        m3b = await h3.wait_for("question", timeout=10)
        elapsed = time.monotonic() - t0
        check("Fix3: no votes -> advances at vote timeout",
              m3b["index"] == 2 and elapsed >= 2.5, f"elapsed={elapsed:.1f}s")
        a3b = solve_math(m3b["text"])
        await h3.send({"type": "answer", "answer": str(a3b), "index": 2})
        await r3.send({"type": "answer", "answer": str(a3b + 1), "index": 2})
        results = await h3.wait_for("results", timeout=10)
        check("Fix3 timeout game produces results", len(results["rankings"]) == 2)
        await h3.close(); await r3.close()

        # --- Fix 1: disconnect mid-question must NOT end the round ---
        h4 = Client("fix1-host"); await h4.connect()
        await h4.send({"type": "create_room", "nickname": "Fix1Host", "colour": "#e74c3c"})
        j4 = await h4.wait_for("joined"); code4 = j4["code"]
        r4 = Client("fix1-p2"); await r4.connect()
        await r4.send({"type": "join_room", "code": code4, "nickname": "Fix1P2", "colour": "#3498db"})
        await r4.wait_for("joined")
        lobby4 = await h4.wait_lobby_count(2)
        r4_id = [pl["id"] for pl in lobby4["room"]["players"] if pl["nickname"] == "Fix1P2"][0]
        await h4.send({"type": "update_settings", "game": "math",
                       "game_settings": {"math": {"difficulty": "Easy", "questions": 1,
                                                  "time_per_question": 6, "vote_timeout": 3}}})
        await h4.wait_lobby()
        await h4.send({"type": "start_game"})
        await h4.wait_for("game_started")
        m4 = await h4.wait_for("question", timeout=15)
        a4 = solve_math(m4["text"])
        await r4.close()  # disconnect mid-question
        await asyncio.sleep(0.5)
        await h4.send({"type": "answer", "answer": str(a4), "index": m4["index"]})
        try:
            await h4.wait_for("question_result", timeout=2)
            check("Fix1: round does NOT end early when a player disconnects mid-question", False,
                  "question_result arrived while a player was disconnected")
        except AssertionError:
            check("Fix1: round does NOT end early when a player disconnects mid-question", True)
        r4b = Client("fix1-p2-rejoin"); await r4b.connect()
        await r4b.send({"type": "join_room", "code": code4, "nickname": "Fix1P2",
                        "colour": "#3498db", "player_id": r4_id})
        rej = await r4b.wait_for("joined", timeout=5)
        check("Fix1: mid-game rejoin reclaims same player id",
              rej["player_id"] == r4_id, f"got {rej['player_id']}")
        # Resync should re-send the current question to the rejoined player.
        q_resync = await r4b.wait_for("question", timeout=5)
        check("Fix1: rejoined player is re-sent the current question",
              q_resync["index"] == m4["index"] and q_resync["text"] == m4["text"])
        await r4b.send({"type": "answer", "answer": str(a4 + 1), "index": m4["index"]})
        res4 = await h4.wait_for("question_result", timeout=10)
        check("Fix1: round ends once the reconnected player answers",
              res4["index"] == 1)
        results = await h4.wait_for("results", timeout=10)
        check("Fix1 disconnect game produces results", len(results["rankings"]) == 2)
        await h4.close(); await r4b.close()

        # --- v2.3 item 11: mid-game reclaim by player_id keeps
        # the same seat AND the same score (no duplicate) ---
        # A human disconnects mid-game, rejoins with the same
        # player_id, and must come back as the SAME player entry
        # (same id, nickname, score) — not a second entry — and
        # the game must still finish for them.
        h9 = Client("reclaim-host"); await h9.connect()
        await h9.send({"type": "create_room", "nickname": "ReclaimHost", "colour": "#e74c3c"})
        j9 = await h9.wait_for("joined"); code9 = j9["code"]
        r9 = Client("reclaim-p2"); await r9.connect()
        await r9.send({"type": "join_room", "code": code9, "nickname": "ReclaimP2", "colour": "#3498db"})
        j9b = await r9.wait_for("joined"); r9_id = j9b["player_id"]
        await h9.send({"type": "update_settings", "game": "math",
                       "game_settings": {"math": {"difficulty": "Easy", "questions": 2,
                                                  "time_per_question": 10, "vote_timeout": 3}}})
        await h9.wait_lobby()
        await h9.send({"type": "start_game"})
        await h9.wait_for("game_started")
        m9 = await h9.wait_for("question", timeout=15)
        a9 = solve_math(m9["text"])
        await h9.send({"type": "answer", "answer": str(a9), "index": m9["index"]})
        await r9.send({"type": "answer", "answer": str(a9), "index": m9["index"]})
        res9 = await h9.wait_for("question_result", timeout=10)
        s9_before = next(s["score"] for s in res9["scores"]
                         if s["player_id"] == r9_id)
        check("v2.3 item 11: the reclaiming player scored in Q1 "
              "(so score preservation is observable)",
              s9_before > 0, f"score={s9_before}")
        await h9.wait_for("vote_break", timeout=10)
        await h9.send({"type": "vote_next", "index": 1})
        m9b = await h9.wait_for("question", timeout=10)
        check("v2.3 item 11: Q2 started before the disconnect",
              m9b["index"] == 2)
        await r9.close()  # disconnect mid-question
        await asyncio.sleep(0.5)
        r9c = Client("reclaim-p2-rejoin"); await r9c.connect()
        await r9c.send({"type": "join_room", "code": code9,
                        "nickname": "ReclaimP2", "colour": "#3498db",
                        "player_id": r9_id})
        rej9 = await r9c.wait_for("joined", timeout=5)
        check("v2.3 item 11: mid-game rejoin reclaims the same "
              "player id",
              rej9["player_id"] == r9_id, f"got {rej9['player_id']}")
        q9_resync = await r9c.wait_for("question", timeout=5)
        check("v2.3 item 11: rejoined player is re-sent the "
              "current question",
              q9_resync["index"] == m9b["index"]
              and q9_resync["text"] == m9b["text"])
        async with httpx.AsyncClient(trust_env=False) as http:
            rr9 = await http.get(f"{BASE}/api/rooms/{code9}")
        _pl9 = rr9.json()["room"]["players"]
        _me9 = [p for p in _pl9 if p["id"] == r9_id]
        check("v2.3 item 11: no duplicate entry, same nickname "
              "and same score after the reclaim",
              len(_pl9) == 2 and len(_me9) == 1
              and _me9[0]["nickname"] == "ReclaimP2"
              and _me9[0]["score"] == s9_before,
              str([(p["nickname"], p["score"]) for p in _pl9]))
        a9b = solve_math(m9b["text"])
        await r9c.send({"type": "answer", "answer": str(a9b),
                        "index": m9b["index"]})
        await h9.send({"type": "answer", "answer": str(a9b),
                       "index": m9b["index"]})
        res9b = await h9.wait_for("question_result", timeout=10)
        check("v2.3 item 11: the round ends once the reclaimed "
              "player answers",
              res9b["index"] == 2)
        results = await h9.wait_for("results", timeout=10)
        check("v2.3 item 11: the reclaimed game produces results "
              "with both players",
              len(results["rankings"]) == 2,
              str([(r["nickname"], r["score"])
                   for r in results["rankings"]]))
        await h9.close(); await r9c.close()

        # ================= v1.2 tests =================
        # --- Fix 5: colour uniqueness on every path ---
        # Join with a colour the host already uses is rejected.
        copy = Client("colour-copy"); await copy.connect()
        await copy.send({"type": "join_room", "code": code,
                         "nickname": "CopyCat", "colour": "#e74c3c"})
        err = await copy.wait_for("error", timeout=5)
        check("v1.2 join with a taken colour is rejected",
              "already taken" in err["message"], err["message"])
        await copy.close()
        # update_profile with a taken colour is rejected.
        await p2b.send({"type": "update_profile", "colour": "#e74c3c"})
        err = await p2b.wait_for("error", timeout=5)
        check("v1.2 profile edit with a taken colour is rejected",
              "already taken" in err["message"], err["message"])
        # update_bot with a taken colour is rejected, bot keeps its colour.
        # (Force a fresh lobby broadcast first — the host's queue was
        # drained by earlier checks and no new one has been sent.)
        await host.send({"type": "update_settings", "room_name": "Test Room"})
        lobby = await host.wait_lobby_count(4)
        bots = [p for p in lobby["room"]["players"] if p["is_bot"]]
        bot0_colour = bots[0]["colour"]
        await host.send({"type": "update_bot", "bot_id": bots[0]["id"],
                         "colour": "#e74c3c"})
        err = await host.wait_for("error", timeout=5)
        check("v1.2 bot edit with a taken colour is rejected",
              "already taken" in err["message"], err["message"])
        # Add BOT auto-picks a colour nobody uses.
        used_before = {p["colour"] for p in lobby["room"]["players"]}
        await host.send({"type": "add_bot"})
        lobby = await host.wait_lobby_count(5)
        new_bots = [p for p in lobby["room"]["players"]
                    if p["is_bot"] and p["colour"] not in used_before]
        check("v1.2 Add BOT auto-picks an unused colour",
              len(new_bots) == 1, str([p["colour"] for p in lobby["room"]["players"]]))

        # --- Fix 1: stale nickname is freed after the grace period ---
        temp = Client("temp"); await temp.connect()
        await temp.send({"type": "join_room", "code": code,
                         "nickname": "Temp", "colour": "#95a5a6"})
        await temp.wait_for("joined")
        await host.wait_lobby_count(6)
        await temp.close()
        # Grace is 3s in this test run; after it, Temp must be gone
        # and the nickname free again (the v1.1 bug: it stayed taken
        # for minutes when the disconnect happened after a game).
        await asyncio.sleep(4.5)
        lobby = await host.wait_lobby_count(5)
        check("v1.2 disconnected player is removed after the grace",
              "Temp" not in [p["nickname"] for p in lobby["room"]["players"]],
              str([p["nickname"] for p in lobby["room"]["players"]]))
        temp2 = Client("temp2"); await temp2.connect()
        await temp2.send({"type": "join_room", "code": code,
                          "nickname": "Temp", "colour": "#95a5a6"})
        rej2 = await temp2.wait_for("joined", timeout=5)
        check("v1.2 nickname is free again after the grace",
              rej2["player_id"] is not None)
        await host.wait_lobby_count(6)

        # --- Fix 1: host kick ---
        kickme = Client("kickme"); await kickme.connect()
        await kickme.send({"type": "join_room", "code": code,
                           "nickname": "KickMe", "colour": "#34495e"})
        km_joined = await kickme.wait_for("joined")
        # The joined message already carries the new player's id —
        # no need to wait on a lobby broadcast for it.
        await host.send({"type": "kick_player",
                         "player_id": km_joined["player_id"]})
        kicked = await kickme.wait_for("kicked", timeout=5)
        check("v1.2 kicked player is told they were kicked",
              "kicked" in kicked["message"], kicked["message"])
        # Verify with a fresh HTTP read (lobby broadcasts in the
        # queue may be stale from before the kick).
        await asyncio.sleep(0.5)
        async with httpx.AsyncClient(trust_env=False) as http:
            r = await http.get(f"{BASE}/api/rooms/{code}")
        names = [p["nickname"] for p in r.json()["room"]["players"]]
        check("v1.2 kicked player leaves the lobby",
              "KickMe" not in names and len(names) == 6, str(names))
        await temp2.close(); await kickme.close()

        # --- Fix 1: room survives the host disconnecting (new host promoted) ---
        h5 = Client("fix1b-host"); await h5.connect()
        await h5.send({"type": "create_room", "nickname": "Fix1bHost", "colour": "#e74c3c"})
        j5 = await h5.wait_for("joined"); code5 = j5["code"]
        r5 = Client("fix1b-p2"); await r5.connect()
        await r5.send({"type": "join_room", "code": code5, "nickname": "Fix1bP2", "colour": "#3498db"})
        await r5.wait_for("joined")
        await h5.close()  # host drops out of the lobby
        promoted = False
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            lb = await r5.wait_for("lobby", timeout=deadline - time.monotonic())
            me5 = [p for p in lb["room"]["players"] if p["nickname"] == "Fix1bP2"]
            if me5 and me5[0]["is_host"]:
                promoted = True
                break
        check("v1.2 room survives host disconnect, another human is promoted",
              promoted)
        async with httpx.AsyncClient(trust_env=False) as http:
            r = await http.get(f"{BASE}/api/rooms/{code5}")
        check("v1.2 room still exists after host disconnect",
              r.json().get("exists") is True)
        await r5.close()

        # --- Fix 1: a room with no humans left is held, then deleted ---
        h6 = Client("fix1c-host"); await h6.connect()
        await h6.send({"type": "create_room", "nickname": "Fix1cHost", "colour": "#e74c3c"})
        j6 = await h6.wait_for("joined"); code6 = j6["code"]
        await h6.close()
        await asyncio.sleep(1)
        async with httpx.AsyncClient(trust_env=False) as http:
            r = await http.get(f"{BASE}/api/rooms/{code6}")
        check("v1.2 empty room is still there during the grace",
              r.json().get("exists") is True)
        await asyncio.sleep(9)  # 3s player grace + 6s room grace
        async with httpx.AsyncClient(trust_env=False) as http:
            r = await http.get(f"{BASE}/api/rooms/{code6}")
        check("v1.2 empty room is deleted after the grace",
              r.json().get("exists") is False)

        # --- Fix 2 + Fix 3: bots out of the vote, host skip ---
        h7 = Client("fix23-host"); await h7.connect()
        await h7.send({"type": "create_room", "nickname": "Fix23Host", "colour": "#e74c3c"})
        j7 = await h7.wait_for("joined"); code7 = j7["code"]
        r7 = Client("fix23-p2"); await r7.connect()
        await r7.send({"type": "join_room", "code": code7, "nickname": "Fix23P2", "colour": "#3498db"})
        await r7.wait_for("joined")
        await h7.send({"type": "add_bot"})
        await h7.wait_lobby_count(3)
        await h7.send({"type": "update_settings", "game": "math",
                       "game_settings": {"math": {"difficulty": "Easy", "questions": 3,
                                                  "time_per_question": 5, "vote_timeout": 8}}})
        await h7.wait_lobby()
        await h7.send({"type": "start_game"})
        await h7.wait_for("game_started")
        m7 = await h7.wait_for("question", timeout=15)
        a7 = solve_math(m7["text"])
        await h7.send({"type": "answer", "answer": str(a7), "index": m7["index"]})
        await r7.send({"type": "answer", "answer": str(a7 + 1), "index": m7["index"]})
        await h7.wait_for("question_result", timeout=10)
        vb7 = await h7.wait_for("vote_break", timeout=10)
        check("v1.2 vote threshold counts HUMANS only (2 humans + 1 bot -> 1)",
              vb7["votes_needed"] == 1 and vb7["votes"] == 0, str(vb7))
        # The bot must not auto-vote: no vote_update for 4.5s.
        try:
            await h7.wait_for("vote_update", timeout=4.5)
            check("v1.2 bot does not vote in the NEXT vote", False,
                  "a vote_update arrived with no human voting")
        except AssertionError:
            check("v1.2 bot does not vote in the NEXT vote", True)
        # Host alone voting (1 of 2 humans) advances.
        await h7.send({"type": "vote_next", "index": 1})
        m7b = await h7.wait_for("question", timeout=10)
        check("v1.2 one human vote advances (bot excluded)",
              m7b["index"] == 2)
        # Q2: host skip advances with ZERO votes.
        a7b = solve_math(m7b["text"])
        await h7.send({"type": "answer", "answer": str(a7b), "index": 2})
        await r7.send({"type": "answer", "answer": str(a7b + 1), "index": 2})
        await h7.wait_for("question_result", timeout=10)
        await h7.wait_for("vote_break", timeout=10)
        t0 = time.monotonic()
        await h7.send({"type": "host_skip"})
        m7c = await h7.wait_for("question", timeout=5)
        check("v1.2 host skip advances immediately with no votes",
              m7c["index"] == 3 and time.monotonic() - t0 < 3,
              f"index={m7c['index']} elapsed={time.monotonic()-t0:.1f}s")
        a7c = solve_math(m7c["text"])
        await h7.send({"type": "answer", "answer": str(a7c), "index": 3})
        await r7.send({"type": "answer", "answer": str(a7c + 1), "index": 3})
        results = await h7.wait_for("results", timeout=10)
        check("v1.2 vote/skip game produces results",
              len(results["rankings"]) == 3)
        await h7.close(); await r7.close()

        # --- Fix 5: Teams mode (unit + integration) ---
        # Team average: members scoring 100 and 300 -> team 200;
        # an Alone player is their own team with their own score.
        room_t = Room(code="TEAM01", mode="teams", team_count=2)
        pa = Player(id="ta", nickname="A", colour="#e74c3c", score=100)
        pb = Player(id="tb", nickname="B", colour="#3498db", score=300)
        pc = Player(id="tc", nickname="C", colour="#2ecc71", score=50)
        room_t.players = [pa, pb, pc]
        room_t.teams = {"ta": 0, "tb": 0, "tc": None}
        st = team_standings(room_t)
        team0 = [s for s in st if s["team"] == 0][0]
        alone = [s for s in st if s["team"] is None][0]
        check("v1.2 team score is the average of its members",
              team0["average"] == 200.0, f"average={team0['average']}")
        check("v1.2 Alone player is their own team with their own score",
              alone["average"] == 50.0 and alone["members"][0]["nickname"] == "C")
        check("v1.2 FFA mode has no team standings",
              team_standings(Room(code="FFA01", mode="ffa")) == [])

        h8 = Client("teams-host"); await h8.connect()
        await h8.send({"type": "create_room", "nickname": "TeamsHost", "colour": "#e74c3c"})
        j8 = await h8.wait_for("joined"); code8 = j8["code"]
        r8 = Client("teams-p2"); await r8.connect()
        await r8.send({"type": "join_room", "code": code8, "nickname": "TeamsP2", "colour": "#3498db"})
        await r8.wait_for("joined")
        await h8.send({"type": "add_bot"})
        await h8.wait_lobby_count(3)
        await h8.send({"type": "add_bot"})
        await h8.wait_lobby_count(4)
        # Team count cannot exceed the player count.
        await h8.send({"type": "update_settings", "mode": "teams", "team_count": 4})
        await h8.wait_lobby()
        # Start with no assignments -> every team empty -> rejected.
        await h8.send({"type": "start_game"})
        err = await h8.wait_for("error", timeout=5)
        check("v1.2 Teams start is rejected when teams are empty",
              "every team" in err["message"], err["message"])
        # Randomize: everyone assigned, every team has >= 1 member.
        await h8.send({"type": "randomize_teams"})
        lobby = await h8.wait_lobby_count(4)
        assigned = {p["team"] for p in lobby["room"]["players"]}
        check("v1.2 randomize assigns every player to a team",
              None not in assigned and assigned == {0, 1, 2, 3},
              str([(p["nickname"], p["team"]) for p in lobby["room"]["players"]]))
        # Manual assignment + Alone: put one bot Alone. (Reuse the
        # lobby from the randomize check — no new broadcast has been
        # sent since, so waiting for another one would time out.)
        bot8 = [p for p in lobby["room"]["players"] if p["is_bot"]][0]
        await h8.send({"type": "assign_team", "player_id": bot8["id"], "team": None})
        await h8.wait_lobby()
        # Play a short Teams game: results must carry team standings
        # AND individual rankings.
        await h8.send({"type": "update_settings", "mode": "teams", "team_count": 2,
                       "game": "math",
                       "game_settings": {"math": {"difficulty": "Easy", "questions": 2,
                                                  "time_per_question": 5, "vote_timeout": 3}}})
        await h8.wait_lobby()
        await h8.send({"type": "randomize_teams"})
        await h8.wait_lobby()
        await h8.send({"type": "start_game"})
        started = await h8.wait_for("game_started", timeout=5)
        check("v1.2 Teams game starts after randomize",
              started["game"] == "math" and started.get("party_mode") == "teams")
        for qi in (1, 2):
            mq = await h8.wait_for("question", timeout=15)
            aq = solve_math(mq["text"])
            await h8.send({"type": "answer", "answer": str(aq), "index": mq["index"]})
            await r8.send({"type": "answer", "answer": str(aq + 1), "index": mq["index"]})
            await h8.wait_for("question_result", timeout=10)
            if qi == 1:
                await h8.wait_for("vote_break", timeout=10)
                await h8.send({"type": "vote_next", "index": 1})
        results = await h8.wait_for("results", timeout=10)
        check("v1.2 Teams results include team standings and individuals",
              results.get("mode") == "teams" and len(results.get("teams", [])) == 2
              and len(results["rankings"]) == 4,
              str(results.get("teams")))
        team_avgs = {t["team"]: t["average"] for t in results["teams"]}
        members = {}
        for t in results["teams"]:
            members[t["team"]] = [m["nickname"] for m in t["members"]]
        ok_avg = all(
            abs(team_avgs[t] - round(sum(
                next(r["score"] for r in results["rankings"]
                     if r["nickname"] == n) for n in members[t]) / len(members[t]), 1)) < 0.01
            for t in team_avgs)
        check("v1.2 Teams results averages match member scores", ok_avg,
              str(results["teams"]))
        await h8.close(); await r8.close()

        # --- Fix 4 (v2.2): the standalone compact customization boxes
        # are gone — the lobby now edits via tap-to-expand player cards
        # (v2.2 client redesign). The editing itself is covered live
        # above (profile edit, bot edit); here pin the new UI.
        html = (ROOT / "static" / "index.html").read_text()
        check("v2.2 lobby player cards are tap-to-edit (bots + self)",
              "tap a card to edit it" in html
              and "update_bot" in html and "update_profile" in html)
        check("v2.2 lobby card edit covers nickname, colour, difficulty",
              "nickname:nn, colour:chosenColour, difficulty" in html)
        check("v2.2 lobby card edit can remove a bot",
              "remove_bot" in html)

        # --- v2.3 client pins (static: the wire behaviour is
        # covered live above; these pin the browser-side fixes) ---
        # Item 1: the end_choice handler must un-hide #endChoice —
        # the v2.2 handler filled the buttons in but never removed
        # `hidden`, so players were stuck on the standings screen.
        _ec_at = html.index('if(msg.type==="end_choice")')
        _ec_end = html.index('if(msg.type==="results")', _ec_at)
        _ec_block = html[_ec_at:_ec_end]
        check("v2.3 item 1: the end_choice handler un-hides "
              "#endChoice",
              '$("endChoice").classList.remove("hidden")' in _ec_block,
              f"handler spans {_ec_end - _ec_at} chars")
        # Item 4: the results handler renders the Secret outcome
        # banner (word, impostors, who won, whether the viewer won).
        check("v2.3 item 4: the results handler renders the Secret "
              "outcome banner",
              "secretResultsBanner" in html
              and "Citizens win" in html and "Impostors win" in html
              and "You win!" in html
              and "The other side wins this one." in html)
        # Item 9: the President header is President-appropriate
        # (not the quiz line) and the in-play hint no longer says
        # "pick 1 card of the same rank, 2 or higher."
        check("v2.3 item 9: the President header describes "
              "President, not quiz scoring",
              "President: shed all your cards first to become "
              "President" in html)
        check("v2.3 item 9: the contradictory President hint "
              "('same rank, 2 or higher') is gone",
              "2 or higher" not in html)
        # Item 11: the session is persisted so a reload reclaims
        # the same seat, and a lobby broadcast moves even a
        # results-screen client back to the lobby.
        check("v2.3 item 11: the client persists the session "
              "(sessionStorage) and auto-joins on load",
              "sessionStorage" in html and "pgh_session" in html)

        # ================= v2 tests =================
        # --- Unit: Secret Word tally + word bank ---
        check("v2 secret tally: catching an impostor wins for the word team",
              secret_tally({"a": "x", "b": "x", "c": "y"}, {"x"}) == ("x", True))
        check("v2 secret tally: voting out an innocent wins for the impostors",
              secret_tally({"a": "y", "b": "y"}, {"x"}) == ("y", False))
        # v2.2: a tie eliminates nobody and the game continues to the
        # next clue round — the helper reports (None, False): no
        # elimination, and the word team did not win this vote.
        check("v2.2 secret tally: a tie eliminates nobody",
              secret_tally({"a": "x", "b": "y"}, {"x"}) == (None, False))
        check("v2 secret tally: no votes, impostors win",
              secret_tally({}, {"x"}) == (None, False))
        check("v2 secret word bank has >= 60 words with 3+ clues each",
              len(SECRET_WORDS) >= 60
              and all(len(v) >= 3 for v in SECRET_WORDS.values()),
              f"{len(SECRET_WORDS)} words")

        # --- v2.2: secret_clues module (data worker's contract) ---
        check("v2.2 secret clues: 72 words, 30-40 distinct clues each, "
              "never the word itself",
              len(SECRET_CLUES) == 72
              and all(30 <= len(v) <= 40 for v in SECRET_CLUES.values())
              and all(len(set(v)) == len(v) for v in SECRET_CLUES.values())
              and all(w not in v for w, v in SECRET_CLUES.items()),
              f"{len(SECRET_CLUES)} words, "
              f"min {min(len(v) for v in SECRET_CLUES.values())} / "
              f"max {max(len(v) for v in SECRET_CLUES.values())} clues")
        check("v2.2 secret categories: 72 words across the 5 categories",
              len(SECRET_CATEGORY) == 72
              and set(SECRET_CATEGORY.values())
              == {"Animals", "Food", "Places", "Objects", "Jobs"},
              str(sorted(set(SECRET_CATEGORY.values()))))
        check("v2.2 vague pool: 80 unique generic words, none a bank word",
              len(VAGUE_CLUES_80) == 80
              and len(set(VAGUE_CLUES_80)) == 80
              and not any(c in SECRET_CLUES for c in VAGUE_CLUES_80),
              f"{len(VAGUE_CLUES_80)} vague clues")
        check("v2.2 secret clues cover the same 72 words as the bank",
              set(SECRET_CLUES) == set(SECRET_WORDS)
              and set(SECRET_CATEGORY) == set(SECRET_WORDS))

        # --- Unit: President legality / strategy / scoring / deck ---
        _hand = ["3♣", "3♦", "5♥", "K♠", "2♦"]
        check("v2 president: a pair of 3s is legal when leading",
              president_legal_play(_hand, ["3♣", "3♦"], None))
        check("v2 president: a mixed-rank set is illegal",
              not president_legal_play(_hand, ["3♣", "5♥"], None))
        check("v2 president: a lower pair cannot beat a higher pair",
              not president_legal_play(_hand, ["3♣", "3♦"], {"size": 2, "rank": 5}))
        check("v2 president: a 2 beats a King as a single",
              president_legal_play(_hand, ["2♦"], {"size": 1, "rank": 13}))
        check("v2 president: the wrong size cannot beat the trick",
              not president_legal_play(_hand, ["3♣"], {"size": 2, "rank": 3}))
        check("v2 president: a card not in hand is illegal",
              not president_legal_play(_hand, ["4♣"], None))
        # --- v2.1: specific illegal-play reasons (not a generic error) ---
        _r = president_illegal_reason(_hand, [], None)
        check("v2.1 president: empty selection names the problem",
              _r is not None and "haven't selected" in _r, str(_r))
        _r = president_illegal_reason(_hand, ["3♣"], {"size": 2, "rank": 3})
        check("v2.1 president: wrong size names the trick size",
              _r is not None and "2 card" in _r and "pair" in _r, str(_r))
        _r = president_illegal_reason(_hand, ["3♣", "3♦"],
                                      {"size": 2, "rank": 8})
        check("v2.1 president: lower rank names the higher-rank rule",
              _r is not None and "higher rank" in _r, str(_r))
        _r = president_illegal_reason(_hand, ["3♣", "5♥"], None)
        check("v2.1 president: mixed ranks names the same-rank rule",
              _r is not None and "same rank" in _r, str(_r))
        _r = president_illegal_reason(_hand, ["4♣"], None)
        check("v2.1 president: card not in hand names the problem",
              _r is not None and "isn't in your hand" in _r, str(_r))
        check("v2.1 president: a legal pair gets no reason (None)",
              president_illegal_reason(_hand, ["3♣", "3♦"], None) is None
              and president_illegal_reason(
                  _hand, ["2♦"], {"size": 1, "rank": 13}) is None)
        check("v2 president: lowest legal set when leading is a single 3",
              president_lowest_legal(_hand, None) == ["3♣"])
        check("v2 president: no pair can beat a pair of 4s here",
              president_lowest_legal(_hand, {"size": 2, "rank": 4}) == [])
        check("v2 president: 52-card deck, all unique",
              len(set(build_president_deck())) == 52)
        check("v2 president: scoring scale is 1000..0",
              president_scores_for(4) == [1000, 667, 333, 0]
              and president_scores_for(5) == [1000, 750, 500, 250, 0])

        # --- v2.3 item 2: President one pass per stack (unit) ---
        # Each unfinished player acts at most once per stack; the
        # stack ends when it comes back around and the stack winner
        # (highest play) leads the next stack. The old multi-pass
        # code let the leader act again after everyone else passed.
        def _pres_stack_room(hands):
            r = Room(code="PRES-1PASS")
            r.players = [Player(id=i, nickname=i, colour="#fff")
                         for i in ["a", "b", "c", "d"]]
            r.round_state = {"hands": {k: list(v)
                                       for k, v in hands.items()},
                             "trick": None, "turn": "a",
                             "last_played_by": None, "passed": set(),
                             "finished": [], "order": ["a", "b", "c", "d"],
                             "acted": set()}
            return r

        def _pres_player(r, pid):
            return next(p for p in r.players if p.id == pid)

        _rs = _pres_stack_room({"a": ["3♣", "7♥"], "b": ["5♥", "9♦"],
                                "c": ["6♣"], "d": ["8♠"]})
        _president_apply_play(_rs, _pres_player(_rs, "a"), ["3♣"])
        check("v2.3 president one-pass: after the lead, B is next",
              _next_president_turn(_rs, "a") == "b")
        _president_apply_play(_rs, _pres_player(_rs, "b"), ["5♥"])
        check("v2.3 president one-pass: after B beats the lead, C "
              "is next",
              _next_president_turn(_rs, "b") == "c")
        _president_apply_pass(_rs, _pres_player(_rs, "c"))
        check("v2.3 president one-pass: after C passes, D is next",
              _next_president_turn(_rs, "c") == "d")
        _president_apply_pass(_rs, _pres_player(_rs, "d"))
        check("v2.3 president one-pass: once everyone has acted, "
              "the stack ends (trick cleared, acted cleared) and "
              "the stack winner B leads the next stack — A does "
              "NOT get another turn in this stack",
              _rs.round_state["trick"] is None
              and _rs.round_state["acted"] == set()
              and _next_president_turn(_rs, "d") == "b",
              f"trick={_rs.round_state['trick']} "
              f"acted={_rs.round_state['acted']} "
              f"next={_next_president_turn(_rs, 'd')}")
        # Kept rule: if the stack winner finished by playing their
        # last cards, the next UNFINISHED player after them leads.
        _rs = _pres_stack_room({"a": ["3♣"], "b": ["5♥"],
                                "c": ["6♣", "K♠"], "d": ["8♠", "Q♦"]})
        _president_apply_play(_rs, _pres_player(_rs, "a"), ["3♣"])
        check("v2.3 president one-pass: A finishes by leading, B "
              "is next",
              _next_president_turn(_rs, "a") == "b"
              and _rs.round_state["finished"] == ["a"])
        _president_apply_play(_rs, _pres_player(_rs, "b"), ["5♥"])
        _president_apply_pass(_rs, _pres_player(_rs, "c"))
        _president_apply_pass(_rs, _pres_player(_rs, "d"))
        check("v2.3 president one-pass: the stack winner B finished, "
              "so the next unfinished player after B (C) leads",
              _rs.round_state["trick"] is None
              and _next_president_turn(_rs, "d") == "c",
              f"trick={_rs.round_state['trick']} "
              f"next={_next_president_turn(_rs, 'd')}")

        # --- v2.2 §6: President roles from the finish order ---
        def _pres_room(ids, finish):
            r = Room(code="PRES01", game="president")
            r.players = [Player(id=i, nickname=i, colour="#fff")
                         for i in ids]
            r.president_finish_order = list(finish)
            return r

        _pr = _pres_room(["a", "b", "c", "d"], ["a", "b", "c", "d"])
        check("v2.2 president roles for 4 players: P/C/C/Peasant",
              president_roles_for(_pr) == {"a": "President", "b": "Citizen",
                                           "c": "Citizen", "d": "Peasant"},
              str(president_roles_for(_pr)))
        _pr = _pres_room(["a", "b", "c", "d", "e"],
                         ["a", "b", "c", "d", "e"])
        check("v2.2 president roles for 5 players: P/VP/C/VPs/Peasant",
              president_roles_for(_pr) == {"a": "President",
                                           "b": "Vice President",
                                           "c": "Citizen",
                                           "d": "Vice Peasant",
                                           "e": "Peasant"},
              str(president_roles_for(_pr)))
        _pr = _pres_room(["a", "b", "c", "d", "e", "f"],
                         ["a", "b", "c", "d", "e", "f"])
        check("v2.2 president roles for 6 players: P/VP/C/C/VPs/Peasant",
              president_roles_for(_pr) == {"a": "President",
                                           "b": "Vice President",
                                           "c": "Citizen", "d": "Citizen",
                                           "e": "Vice Peasant",
                                           "f": "Peasant"},
              str(president_roles_for(_pr)))
        _pr = _pres_room(["a", "b", "c", "d", "e", "f", "g", "h"],
                         ["a", "b", "c", "d", "e", "f", "g", "h"])
        check("v2.2 president roles for 8 players: P/VP/Cx4/VPs/Peasant",
              president_roles_for(_pr) == {"a": "President",
                                           "b": "Vice President",
                                           "c": "Citizen", "d": "Citizen",
                                           "e": "Citizen", "f": "Citizen",
                                           "g": "Vice Peasant",
                                           "h": "Peasant"},
              str(president_roles_for(_pr)))
        # A newcomer is appended at the bottom (as if they finished
        # last), which naturally promotes the old Peasant.
        _pr = _pres_room(["a", "b", "c", "d", "e"], ["a", "b", "c", "d"])
        check("v2.2 president: newcomer appended at bottom of the order",
              president_ordered_ids(_pr) == ["a", "b", "c", "d", "e"],
              str(president_ordered_ids(_pr)))
        check("v2.2 president: newcomer arrival promotes Peasant to "
              "Vice Peasant and the newcomer is Peasant",
              president_roles_for(_pr)["d"] == "Vice Peasant"
              and president_roles_for(_pr)["e"] == "Peasant",
              str(president_roles_for(_pr)))
        # A player who left is filtered out of the stored order.
        _pr = _pres_room(["a", "b", "d"], ["a", "b", "c", "d"])
        check("v2.2 president: stored order filters to current players",
              president_ordered_ids(_pr) == ["a", "b", "d"])
        # Match 1 (no stored finish order): everyone is a Citizen.
        _pr = _pres_room(["a", "b", "c", "d"], [])
        check("v2.2 president match 1: everyone is a Citizen",
              president_roles_for(_pr) == {i: "Citizen"
                                           for i in "abcd"},
              str(president_roles_for(_pr)))
        # Roles are only published when President is the selected game.
        _pr.game = "reaction"
        check("v2.2 president roles are empty when President is not "
              "the selected game",
              president_roles_for(_pr) == {})
        check("v2.2 president titles for a 4-player finish",
              president_titles_for_finish(4)
              == ["President", "Citizen", "Citizen", "Peasant"])
        check("v2.2 president titles for a 5-player finish",
              president_titles_for_finish(5)
              == ["President", "Vice President", "Citizen",
                  "Vice Peasant", "Peasant"])

        # --- Unit: UNO deck / matching / bot strategy ---
        _deck = build_uno_deck()
        check("v2 UNO: 108-card deck", len(_deck) == 108, f"{len(_deck)}")
        check("v2 UNO: 4 wilds and 4 draw-4s",
              _deck.count("Wwild") == 4 and _deck.count("Wdraw4") == 4)
        check("v2 UNO: matching by colour", uno_match("R5", "G5", "R"))
        check("v2 UNO: matching by number", uno_match("B5", "R5", "G"))
        check("v2 UNO: wild always matches", uno_match("Wwild", "R5", "G"))
        check("v2 UNO: non-matching card rejected",
              not uno_match("B7", "R5", "G"))
        # v2.2: uno_bot_choice now returns a LIST of cards to play
        # (one card normally, two for a multi-play pair), or None.
        check("v2.2 UNO: bot prefers a number card over a wild",
              uno_bot_choice(["R5", "Wwild"], "R9", "R") == ["R5"])
        check("v2 UNO: bot with no match draws",
              uno_bot_choice(["B7"], "R5", "G") is None)
        check("v2 UNO: bot wild colour is its most common colour",
              uno_best_colour(["R5", "R7", "B3"]) == "R")
        check("v2 UNO: value parsing",
              uno_value("Gdraw2") == "draw2" and uno_value("Wwild") == "wild")
        # v2.2 §5: stacking — Hard always stacks a same-type card.
        check("v2.2 UNO: bot stacks a +2 on a pending +2 (Hard)",
              uno_bot_choice(["Gdraw2", "R5"], "R9", "R",
                             pending_type="draw2", difficulty="Hard")
              == ["Gdraw2"])
        check("v2.2 UNO: bot cannot stack a +4 on a pending +2",
              uno_bot_choice(["Wdraw4", "R5"], "R9", "R",
                             pending_type="draw2", difficulty="Hard") is None)
        check("v2.2 UNO: bot with no stackable card draws the pile",
              uno_bot_choice(["R5", "B3"], "R9", "R",
                             pending_type="draw2", difficulty="Hard") is None)
        # v2.2 §5: multi-play — a same-value pair, both cards matching
        # the top by value (B9 + G9 on R9; seeded: the Hard pair
        # probability is 0.7 and random.seed(42) draws 0.639…).
        _state = random.getstate()
        random.seed(42)
        _pair = uno_bot_choice(["B9", "G9", "R5"], "R9", "R",
                               multi_play=True, difficulty="Hard")
        random.setstate(_state)
        check("v2.2 UNO: bot multi-play returns a same-value pair",
              _pair == ["B9", "G9"], str(_pair))

        # --- v2.2 §5: UNO house-rule validation (pure, deterministic) ---
        check("v2.2 UNO validate: multi-play off rejects two cards",
              uno_validate_play(["R5", "B5"], ["R5", "B5"], "R9", "R",
                                multi_play=False) is not None)
        check("v2.2 UNO validate: multi-play on accepts a same-value pair",
              uno_validate_play(["R5", "B5"], ["R5", "B5"], "R9", "R",
                                multi_play=True) is None)
        check("v2.2 UNO validate: multi-play rejects mixed values",
              uno_validate_play(["R5", "B6"], ["R5", "B6"], "R9", "R",
                                multi_play=True) is not None)
        check("v2.2 UNO validate: multi-play rejects a first card that "
              "does not match the top card",
              uno_validate_play(["B5", "G5"], ["B5", "G5"], "R9", "R",
                                multi_play=True) is not None)
        check("v2.2 UNO validate: stacking accepts a +2 on a pending +2",
              uno_validate_play(["Gdraw2"], ["Gdraw2"], "R9", "R",
                                pending_draw=2, pending_type="draw2") is None)
        check("v2.2 UNO validate: stacking rejects a +4 on a pending +2",
              uno_validate_play(["Wdraw4"], ["Wdraw4"], "R9", "R",
                                pending_draw=2, pending_type="draw2") is not None)
        check("v2.2 UNO validate: stacking rejects a non-draw card on "
              "a pending draw",
              uno_validate_play(["R5"], ["R5"], "R9", "R",
                                pending_draw=2, pending_type="draw2") is not None)
        check("v2.2 UNO validate: a card not in hand is rejected",
              uno_validate_play(["R5"], ["B5"], "R9", "R") is not None)
        check("v2.2 UNO validate: an empty play is rejected",
              uno_validate_play(["R5"], [], "R9", "R") is not None)

        # --- v2.2 §5: multi-play effects (all effects trigger) ---
        def _uno_room(hand, top, colour, **kw):
            r = Room(code="UNO01")
            p1 = Player(id="u1", nickname="U1", colour="#fff")
            p2 = Player(id="u2", nickname="U2", colour="#fff")
            r.players = [p1, p2]
            st = {"hands": {"u1": list(hand), "u2": ["R3"]},
                  "draw": list(kw.get("draw", [])),
                  "discard": [top], "current_colour": colour,
                  "direction": 1, "order": ["u1", "u2"],
                  "pending_draw": kw.get("pending_draw", 0),
                  "pending_type": kw.get("pending_type"),
                  "skip_count": 0}
            r.round_state = st
            return r, p1

        _ru, _pu = _uno_room(["Rdraw2", "Gdraw2", "B5"], "R9", "R")
        _uno_apply_play_multi(_ru, _pu, ["Rdraw2", "Gdraw2"])
        check("v2.2 UNO multi-play: two +2s stack to a pending +4",
              _ru.round_state["pending_draw"] == 4
              and _ru.round_state["pending_type"] == "draw2"
              and _ru.round_state["hands"]["u1"] == ["B5"]
              and _ru.round_state["current_colour"] == "G",
              str(_ru.round_state))
        _ru, _pu = _uno_room(["Rskip", "Bskip", "G5"], "R9", "R")
        _uno_apply_play_multi(_ru, _pu, ["Rskip", "Bskip"])
        check("v2.2 UNO multi-play: two skips skip two players",
              _ru.round_state["skip_count"] == 2
              and _ru.round_state["current_colour"] == "B",
              str(_ru.round_state))
        _ru, _pu = _uno_room(["Rreverse", "Breverse", "G5"], "R9", "R")
        _uno_apply_play_multi(_ru, _pu, ["Rreverse", "Breverse"])
        check("v2.2 UNO multi-play: two reverses flip direction twice "
              "(back to normal)",
              _ru.round_state["direction"] == 1,
              str(_ru.round_state["direction"]))
        _ru, _pu = _uno_room(["Wwild", "R5"], "G9", "G")
        _uno_apply_play_multi(_ru, _pu, ["Wwild"], "B")
        check("v2.2 UNO multi-play: a Wild sets the colour once",
              _ru.round_state["current_colour"] == "B"
              and _ru.round_state["discard"][-1] == "Wwild",
              str(_ru.round_state))

        # --- v2.3 item 6: UNO multi-play — ANY card may lead ---
        # The v2.2 bug: the server only checked cards[0] against the
        # top card, so two SKIPs were rejected when the matching one
        # was not first in the sent order. Now the set is legal if
        # at least one card matches, and the apply step reorders so
        # a top-matching card is played first.
        check("v2.3 UNO validate: multi-play accepts two SKIPs when "
              "the matching one is NOT first in the sent order",
              uno_validate_play(["Bskip", "Rskip"], ["Bskip", "Rskip"],
                                "R9", "R", multi_play=True) is None)
        check("v2.3 UNO validate: multi-play accepts two SKIPs when "
              "the matching one IS first (regression)",
              uno_validate_play(["Rskip", "Bskip"], ["Rskip", "Bskip"],
                                "R9", "R", multi_play=True) is None)
        check("v2.3 UNO validate: multi-play still rejects a set "
              "where NO card matches the top card",
              uno_validate_play(["Bskip", "Gskip"], ["Bskip", "Gskip"],
                                "R9", "R", multi_play=True) is not None)
        check("v2.3 UNO validate: a single non-matching card is "
              "still rejected",
              uno_validate_play(["Bskip"], ["Bskip"], "R9", "R",
                                multi_play=True) is not None)
        _ru, _pu = _uno_room(["Bskip", "Rskip", "G5"], "R9", "R")
        _uno_apply_play_multi(_ru, _pu, ["Bskip", "Rskip"])
        check("v2.3 UNO multi-play: the apply step reorders so the "
              "top-matching SKIP leads (discard Rskip then Bskip, "
              "colour ends B, two skips banked)",
              _ru.round_state["discard"][-2:] == ["Rskip", "Bskip"]
              and _ru.round_state["current_colour"] == "B"
              and _ru.round_state["skip_count"] == 2
              and _ru.round_state["hands"]["u1"] == ["G5"],
              str(_ru.round_state))

        # --- v2.2 §5: draw rules (draw-until-playable / must-play) ---
        # OFF + playable drawn -> drawn card is auto-played (hand size
        # unchanged, top card becomes the drawn card).
        _rd, _pd = _uno_room(["R5", "B3"], "R9", "R", draw=["R7"])
        _uno_handle_draw(_rd, _pd)
        check("v2.2 UNO draw OFF: a playable drawn card must be played",
              len(_rd.round_state["hands"]["u1"]) == 2
              and _rd.round_state["discard"][-1] == "R7"
              and "R7" not in _rd.round_state["hands"]["u1"],
              str(_rd.round_state))
        # OFF + unplayable drawn -> kept, turn ends with one more card.
        _rd, _pd = _uno_room(["R5", "B3"], "R9", "R", draw=["B8"])
        _uno_handle_draw(_rd, _pd)
        check("v2.2 UNO draw OFF: an unplayable drawn card is kept",
              len(_rd.round_state["hands"]["u1"]) == 3
              and _rd.round_state["discard"][-1] == "R9",
              str(_rd.round_state))
        # ON: draws until a playable card appears, then auto-plays it
        # (draw pile pops from the end: B8 first, then R3).
        _rd, _pd = _uno_room(["R5", "B3"], "R9", "R", draw=["R3", "B8"])
        _rd.game_settings["uno"]["draw_until_playable"] = True
        _uno_handle_draw(_rd, _pd)
        check("v2.2 UNO draw ON: draws until playable, then auto-plays",
              _rd.round_state["hands"]["u1"] == ["R5", "B3", "B8"]
              and _rd.round_state["discard"][-1] == "R3",
              str(_rd.round_state))
        # Stacking take: a pending +4 draws the full total, then clears.
        _rd, _pd = _uno_room(["R5", "B3"], "R9", "R",
                             draw=["G1", "G2", "G3", "G4"],
                             pending_draw=4, pending_type="draw4")
        _uno_handle_draw(_rd, _pd)
        check("v2.2 UNO stacking: taking a pending +4 draws all 4 and "
              "clears the pending total",
              len(_rd.round_state["hands"]["u1"]) == 6
              and _rd.round_state["pending_draw"] == 0
              and _rd.round_state["pending_type"] is None,
              str(_rd.round_state))

        # --- Unit: Memory bot recall (seeded, statistical with wide margins) ---
        # v2.1 retune: per-digit error grows with length, so bots
        # survive the early rounds and fail gradually. At 4 digits
        # Easy ~83%, Medium ~94%, Hard ~98% correct.
        import random as _random
        _state = _random.getstate()  # restore after: the seed must not
        _random.seed(1234)           # leak into later game draws
        _easy = sum(memory_bot_correct("Easy", 4) for _ in range(200))
        _med = sum(memory_bot_correct("Medium", 4) for _ in range(200))
        _hard = sum(memory_bot_correct("Hard", 4) for _ in range(200))
        _random.setstate(_state)
        check("v2.1 memory: Hard bots recall better than Easy bots at 4 digits",
              _hard > _med > _easy and 140 < _easy < 185 and _hard > 180,
              f"easy={_easy}/200 medium={_med}/200 hard={_hard}/200")

        # --- v2.1: Memory bot survival simulation (seeded) ---
        # 200 simulated bots per difficulty play rounds of 4..12
        # digits; the mean last digit survived should land in the
        # target band for each difficulty (Easy 4-5, Medium 6-7,
        # Hard 8+), and Hard must clearly outlast Easy.
        def _sim_survival(diff):
            _s = _random.getstate()
            _random.seed(1234)
            total = 0
            for _ in range(200):
                digits, survived = 4, 0
                while digits <= 12:
                    if memory_bot_correct(diff, digits):
                        survived += 1
                        digits += 1
                    else:
                        break
                total += 3 + survived  # last digit survived (3 = none)
            _random.setstate(_s)
            return total / 200
        _surv = {d: _sim_survival(d) for d in ("Easy", "Medium", "Hard")}
        check("v2.1 memory: bot survival bands (Easy 4-5, Medium 6-7, Hard 8+)",
              4.0 <= _surv["Easy"] <= 5.5
              and 5.5 <= _surv["Medium"] <= 7.5
              and 7.5 <= _surv["Hard"] <= 10.0
              and _surv["Hard"] > _surv["Medium"] > _surv["Easy"],
              str({k: round(v, 2) for k, v in _surv.items()}))

        # --- Unit: Elimination pool filter ---
        check("v2 elimination: pool is the four quick games at 4+ players",
              [g for g in ELIM_POOL if GAMES[g]["min_players"] <= 4]
              == ["reaction", "math", "trivia", "memory"])
        check("v2 elimination: pool excludes Secret Word, President, UNO",
              not any(g in ELIM_POOL for g in ("secret", "president", "uno")))
        check("v2 elimination: Memory drops out of the pool below 2 players",
              "memory" not in [g for g in ELIM_POOL
                               if GAMES[g]["min_players"] <= 1])

        # --- v2.2 §4: turn order (unit) ---
        _rto = Room(code="TO01")
        _rto.players = [Player(id=i, nickname=i, colour="#fff")
                        for i in ["a", "b", "c", "d"]]
        _rto.turn_order_mode = "manual"
        _rto.manual_order = ["c", "a"]
        check("v2.2 manual turn order: host list first, the rest "
              "appended in join order",
              compute_turn_order(_rto) == ["c", "a", "b", "d"],
              str(compute_turn_order(_rto)))
        _rto.manual_order = ["c", "a", "zzz", "b", "d"]
        check("v2.2 manual turn order: unknown ids are dropped",
              compute_turn_order(_rto) == ["c", "a", "b", "d"])
        _rto.turn_order_mode = "random"
        _orders = {tuple(compute_turn_order(_rto)) for _ in range(30)}
        check("v2.2 random turn order: shuffles (several distinct orders "
              "in 30 draws) and is always a permutation of the players",
              len(_orders) >= 3
              and all(sorted(o) == ["a", "b", "c", "d"] for o in _orders),
              f"{len(_orders)} distinct orders")

        # --- v2.2 §9: team scoring styles (unit) ---
        def _team_room(scores, teams, style):
            r = Room(code="TS01", mode="teams", team_count=2)
            r.team_scoring = style
            r.players = [Player(id=i, nickname=i, colour="#fff",
                                score=s) for i, s in scores.items()]
            r.teams = dict(teams)
            return r

        _rts = _team_room({"a": 100, "b": 300, "c": 50},
                          {"a": 0, "b": 0, "c": 1}, "sum")
        _sts = {t["team"]: t["score"] for t in team_standings(_rts)}
        check("v2.2 team scoring sum: team totals are member sums",
              _sts == {0: 400.0, 1: 50.0}, str(_sts))
        _rts = _team_room({"a": 100, "b": 300, "c": 50},
                          {"a": 0, "b": 0, "c": 1}, "best")
        _sts = {t["team"]: t["score"] for t in team_standings(_rts)}
        check("v2.2 team scoring best: team totals are the best member",
              _sts == {0: 300.0, 1: 50.0}, str(_sts))
        _rts = _team_room({"a": 100, "b": 300, "c": 50},
                          {"a": 0, "b": 0, "c": 1}, "average")
        _sts = {t["team"]: t["average"] for t in team_standings(_rts)}
        check("v2.2 team scoring average: team totals are member means",
              _sts == {0: 200.0, 1: 50.0}, str(_sts))
        # An eliminated member (score 0) still counts in the average's
        # denominator — the team is not flattered by losing a member.
        _rts = _team_room({"a": 100, "b": 0, "c": 50},
                          {"a": 0, "b": 0, "c": 1}, "average")
        _sts = {t["team"]: t["average"] for t in team_standings(_rts)}
        check("v2.2 team scoring: an eliminated (0-score) member counts "
              "in the average denominator",
              _sts[0] == 50.0, str(_sts))
        _rts = _team_room({"a": 100, "b": 0, "c": 50},
                          {"a": 0, "b": 0, "c": 1}, "sum")
        _sts = {t["team"]: t["score"] for t in team_standings(_rts)}
        check("v2.2 team scoring: an eliminated member contributes 0 "
              "to the sum",
              _sts[0] == 100.0, str(_sts))
        _rts = _team_room({"a": 0, "b": 0, "c": 50},
                          {"a": 0, "b": 0, "c": 1}, "best")
        _sts = {t["team"]: t["score"] for t in team_standings(_rts)}
        check("v2.2 team scoring: a team with no surviving members "
              "totals 0 under best",
              _sts[0] == 0.0, str(_sts))

        # --- v2.2 §8: Secret Word multi-round game (5 humans) ---
        # 5 humans, 1 impostor, MANUAL turn order (scrambled). The
        # vote plan forces a round-1 tie (nobody out), a round-2
        # citizen elimination (the game continues, the victim
        # spectates), and a round-3 impostor catch. One game covers
        # the whole loop: multi-round, tie, spectating, hidden points.
        s_names = ["SecretHost", "SecretP2", "SecretP3", "SecretP4",
                   "SecretP5"]
        s_colours = ["#e74c3c", "#3498db", "#2ecc71", "#f1c40f", "#9b59b6"]
        s_clients = []
        s_ids = []
        s_code = None
        for i, (nm, col) in enumerate(zip(s_names, s_colours)):
            c = Client(f"secret-{i}")
            await c.connect()
            if i == 0:
                await c.send({"type": "create_room", "nickname": nm,
                              "colour": col})
            else:
                await c.send({"type": "join_room", "code": s_code,
                              "nickname": nm, "colour": col})
            j = await c.wait_for("joined")
            if i == 0:
                s_code = j["code"]
            s_ids.append(j["player_id"])
            s_clients.append(c)
        hs = s_clients[0]
        # Manual turn order, scrambled (not join order).
        s_manual = [s_ids[1], s_ids[4], s_ids[0], s_ids[3], s_ids[2]]
        await hs.send({"type": "update_settings", "game": "secret",
                       "turn_order_mode": "manual",
                       "turn_order": s_manual})
        await hs.wait_lobby_where(
            lambda r: r.get("turn_order_mode") == "manual"
            and r.get("turn_order") == s_manual)
        await hs.send({"type": "start_game"})
        started = await hs.wait_for("game_started", timeout=5)
        check("v2.2 Secret Word starts with the manual turn order",
              started["game"] == "secret"
              and started.get("turn_order") == s_manual, str(started))

        def s_vote_for(round_no, my_id, alive, clues):
            if round_no == 1:
                # 2-2-1 split for a tie: B gets 2, C gets 2, D gets 1;
                # nobody votes for themselves.
                _t1 = {s_ids[0]: s_ids[1], s_ids[1]: s_ids[2],
                       s_ids[2]: s_ids[1], s_ids[3]: s_ids[2],
                       s_ids[4]: s_ids[3]}
                return _t1[my_id]
            if round_no == 2:
                # Vote for a KNOWN citizen: someone whose round-1 clue
                # was a real (non-vague) clue. Skip self.
                r1 = clues.get(1, {})
                for pid in s_ids:
                    if pid in alive and pid != my_id and r1.get(pid) \
                            and r1[pid] not in VAGUE_CLUES_80:
                        return pid
                return next(pid for pid in alive if pid != my_id)
            # Round 3+: vote for the vague-clue giver (the impostor).
            rcur = clues.get(round_no, {}) or clues.get(1, {})
            for pid in s_ids:
                if pid in alive and pid != my_id \
                        and rcur.get(pid) in VAGUE_CLUES_80:
                    return pid
            return next(pid for pid in alive if pid != my_id)

        s_stores = [{} for _ in s_clients]
        await asyncio.wait_for(asyncio.gather(
            *(secret_driver(c, pid, st, s_vote_for)
              for c, pid, st in zip(s_clients, s_ids, s_stores))),
            timeout=240)
        sres = s_stores[0]["result"]
        _words = {}
        for st in s_stores:
            _words.update(st["words"])
        check("v2.2 Secret: exactly one impostor among the 5 humans",
              sum(1 for m in _words.values() if m["is_impostor"]) == 1,
              str([(m["role"], m["word"]) for m in _words.values()]))
        check("v2.2 Secret: category is sent to BOTH roles, word only "
              "to citizens",
              all(m["category"] for m in _words.values())
              and all((m["word"] in SECRET_WORDS)
                      if not m["is_impostor"] else (m["word"] is None)
                      for m in _words.values())
              and all(m["category"] == SECRET_CATEGORY[sres["word"]]
                      for m in _words.values()),
              str(_words))
        _clue_r1 = [pid for (rnd, pid) in s_stores[0]["clue_order"]
                    if rnd == 1]
        check("v2.2 Secret: clue order follows the manual turn order",
              _clue_r1 == s_manual, str(_clue_r1))
        _rr = s_stores[0]["round_results"]
        check("v2.2 Secret: round 1 is a tie — nobody out, game continues",
              len(_rr) >= 2 and _rr[0]["tie"] is True
              and _rr[0]["eliminated"] is None, str(_rr))
        check("v2.2 Secret: the game ran exactly 3 clue rounds",
              len(_rr) == 3 and [r["round"] for r in _rr] == [1, 2, 3],
              str([(r["round"], r["eliminated"], r["tie"]) for r in _rr]))
        check("v2.2 Secret: round breaks hide the scores too",
              all(vb.get("scores") == []
                  for vb in s_stores[0]["vote_breaks"]),
              str(s_stores[0]["vote_breaks"]))
        _elim2 = _rr[1]["eliminated"]
        _victim_store = s_stores[s_ids.index(_elim2)]
        check("v2.2 Secret: the voted-out player spectates (no clue turn "
              "or vote after elimination)",
              _victim_store["violations"] == [],
              str(_victim_store["violations"]))
        check("v2.2 Secret: round 3 had no clue turn for the eliminated "
              "player",
              all(pid != _elim2
                  for (rnd, pid) in s_stores[0]["clue_order"] if rnd == 3),
              str(s_stores[0]["clue_order"]))
        check("v2.2 Secret: round 2 eliminated a citizen, round 3 caught "
              "the impostor",
              _rr[1]["eliminated"] not in sres["impostors"]
              and _rr[2]["eliminated"] in sres["impostors"],
              str([(r["round"], r["eliminated"]) for r in _rr]))
        check("v2.2 Secret: citizens win by catching the impostor",
              sres["word_team_won"] is True
              and sres["eliminated"] in sres["impostors"]
              and sres.get("word") in SECRET_WORDS
              and len(sres.get("winners", [])) == 4, str(sres))
        results = await hs.wait_for("results", timeout=10)
        check("v2.2 Secret: results hide the points until end_choice",
              results.get("scores_hidden") is True
              and all("score" not in r for r in results["rankings"])
              and len(results["rankings"]) == 5,
              str(results["rankings"]))
        check("v2.2 Secret: results still name the word and impostors",
              results.get("word") in SECRET_WORDS
              and len(results.get("impostors", [])) == 1, str(results))
        # Expected per-round scores, recomputed from the round results:
        # +200 per round survived; a citizen who voted for a wrongly
        # eliminated citizen gets +100 instead of 200 that round.
        _imp = set(sres["impostors"])
        _exp = {pid: 0 for pid in s_ids}
        for _r in _rr:
            _elim = _r["eliminated"]
            _wrong = _elim is not None and _elim not in _imp
            for _pid in _r["alive"]:
                if _wrong and _r["votes"].get(_pid) == _elim \
                        and _pid not in _imp:
                    _exp[_pid] += 100
                else:
                    _exp[_pid] += 200
        end, _ = await drive_end_flow(hs, s_clients, choice="lobby",
                                      count=5)
        _got = {r["player_id"]: r["score"] for r in end["rankings"]}
        check("v2.2 Secret: end_choice reveals the per-round points "
              "(+200/round survived, +100 for a wrong citizen vote)",
              _got == _exp, f"got={_got} expected={_exp}")
        check("v2.2 Secret: the round-2 victim scored only for the "
              "round they survived",
              _got[_elim2] == 200, str(_got))
        for c in s_clients:
            await c.close()

        # --- v2.3 item 13 (REPRO): Secret Word scoring ---
        # 4 players, 1 impostor, round-1 vote eliminates the
        # impostor -> each alive citizen scores 200, the impostor
        # scores 0. The live playtest showed 400/350/350/0, which
        # does NOT match the +200/round rule. Retries fresh rooms
        # (max 3) if a round-1 catch does not materialise (a
        # citizen bot's clue can occasionally read as vague and
        # split the vote); a scoring mismatch is a FAIL of the
        # scoring code, not of this test.
        s13_done = False
        for _attempt in (1, 2, 3):
            if s13_done:
                break
            h13 = Client("s13-host")
            await h13.connect()
            await h13.send({"type": "create_room", "nickname": "S13Host",
                            "colour": "#e74c3c"})
            j13 = await h13.wait_for("joined")
            code13 = j13["code"]
            h13_id = j13["player_id"]
            r13 = Client("s13-p2")
            await r13.connect()
            await r13.send({"type": "join_room", "code": code13,
                            "nickname": "S13P2", "colour": "#3498db"})
            j13b = await r13.wait_for("joined")
            r13_id = j13b["player_id"]
            await h13.send({"type": "add_bot"})
            await h13.wait_lobby_count(3)
            await h13.send({"type": "add_bot"})
            lobby13 = await h13.wait_lobby_count(4)
            ids13 = [p["id"] for p in lobby13["room"]["players"]]
            await h13.send({"type": "update_settings", "game": "secret"})
            await h13.wait_lobby()
            await h13.send({"type": "start_game"})
            started13 = await h13.wait_for("game_started", timeout=5)
            check("v2.3 item 13: 4-player Secret Word starts",
                  started13["game"] == "secret", str(started13))

            def s13_vote_for(round_no, my_id, alive, clues):
                if round_no != 1:
                    return next(pid for pid in alive if pid != my_id)
                r1 = clues.get(1, {})
                # The impostor's clue is "thing" (human) or a vague
                # word (bot); citizens give real clues.
                for pid in alive:
                    if pid != my_id and r1.get(pid) == "thing":
                        return pid
                for pid in alive:
                    if pid != my_id and r1.get(pid) in VAGUE_CLUES_80:
                        return pid
                return next(pid for pid in alive if pid != my_id)

            s13_stores = [{}, {}]
            await asyncio.wait_for(asyncio.gather(
                secret_driver(h13, h13_id, s13_stores[0], s13_vote_for),
                secret_driver(r13, r13_id, s13_stores[1], s13_vote_for)),
                timeout=240)
            s13_res = s13_stores[0]["result"]
            s13_rr = s13_stores[0]["round_results"]
            if not s13_rr or s13_rr[0]["eliminated"] \
                    not in s13_res["impostors"]:
                # No round-1 catch this attempt — fresh room, retry.
                await h13.close()
                await r13.close()
                continue
            s13_done = True
            check("v2.3 item 13: the impostor is caught in round 1 "
                  "(one clue round, citizens win at once)",
                  len(s13_rr) == 1 and s13_res["word_team_won"] is True,
                  str([(r["round"], r["eliminated"]) for r in s13_rr]))
            # v2.3 item 3: every Secret state message carries
            # turn_order (the alive list, in clue order).
            _tos = [to for (_t, to) in s13_stores[0]["turn_orders"]]
            check("v2.3 item 3: secret_word / secret_clue_turn / "
                  "secret_clue_shown all carry turn_order (alive in "
                  "clue order)",
                  len(_tos) >= 3 and all(to is not None for to in _tos)
                  and all(sorted(to) == sorted(ids13) for to in _tos)
                  and all(to == _tos[0] for to in _tos),
                  str(s13_stores[0]["turn_orders"]))
            # v2.3 item 4: the results screen carries the outcome
            # (word, impostors, who won) in results_extra.
            s13_results = await h13.wait_for("results", timeout=10)
            check("v2.3 item 4: Secret results carry the outcome "
                  "banner fields (word, impostors, word_team_won, "
                  "winners) and hide the points until end_choice",
                  s13_results.get("word") == s13_res["word"]
                  and s13_results.get("word_team_won") is True
                  and s13_results.get("impostors") == s13_res["impostors"]
                  and set(s13_results.get("winners", []))
                  == {pid for pid in ids13
                      if pid not in s13_res["impostors"]}
                  and s13_results.get("scores_hidden") is True,
                  str(s13_results))
            # v2.3 item 10: with 2 humans the end-of-game vote
            # needs 1 vote (not 4, not a stale denominator).
            end13, _ = await drive_end_flow(
                h13, [h13, r13], choice="lobby", count=4,
                expect_votes_needed=1)
            _got13 = {r["player_id"]: r["score"]
                      for r in end13["rankings"]}
            _imp13 = set(s13_res["impostors"])
            check("v2.3 item 13: round-1 impostor catch scores "
                  "200 per alive citizen and 0 for the impostor",
                  _got13 == {pid: (0 if pid in _imp13 else 200)
                             for pid in ids13},
                  f"got={_got13} impostors={_imp13}")
            await h13.close()
            await r13.close()
        check("v2.3 item 13: a round-1 impostor catch materialised "
              "within 3 attempts",
              s13_done)

        # --- v2.2 §8: 2 impostors with <5 players ends immediately ---
        ha = Client("secret2-host")
        await ha.connect()
        await ha.send({"type": "create_room", "nickname": "AbortHost",
                       "colour": "#e74c3c"})
        ja = await ha.wait_for("joined")
        codea = ja["code"]
        ra = Client("secret2-p2")
        await ra.connect()
        await ra.send({"type": "join_room", "code": codea,
                       "nickname": "AbortP2", "colour": "#3498db"})
        await ra.wait_for("joined")
        await ha.send({"type": "add_bot"})
        await ha.wait_lobby_count(3)
        await ha.send({"type": "add_bot"})
        await ha.wait_lobby_count(4)
        await ha.send({"type": "update_settings", "game": "secret",
                       "game_settings": {"secret": {"impostors": 2}}})
        await ha.wait_lobby()
        await ha.send({"type": "start_game"})
        started = await ha.wait_for("game_started", timeout=5)
        check("v2.2 Secret (2 impostors) starts", started["game"] == "secret")
        ended = await ha.wait_for("secret_ended", timeout=10)
        check("v2.2 Secret: 2 impostors with 4 players ends immediately",
              ended.get("reason") == "too_few_for_two_impostors",
              str(ended))
        sres_a = await ha.wait_for("secret_result", timeout=10)
        check("v2.2 Secret: aborted game names the word, no winners",
              sres_a.get("word") in SECRET_WORDS
              and sres_a.get("word_team_won") is False
              and sres_a.get("winners") == [], str(sres_a))
        results = await ha.wait_for("results", timeout=10)
        check("v2.2 Secret: aborted results are flagged and hide points",
              results.get("aborted") is True
              and results.get("scores_hidden") is True
              and len(results["rankings"]) == 4, str(results))
        await drive_end_flow(ha, [ha, ra], choice="lobby", count=4)
        await ha.close()
        await ra.close()

        # --- v2.2 §6: President full game (2 humans + 2 bots) ---
        # Match 1 is all Citizens with the MANUAL turn order; the
        # replay then starts match 2 with roles from the match-1
        # finish order, the card exchange, and the President leading.
        hp = Client("pres-host"); await hp.connect()
        await hp.send({"type": "create_room", "nickname": "PresHost", "colour": "#e74c3c"})
        jp = await hp.wait_for("joined"); codep = jp["code"]; hp_id = jp["player_id"]
        rp = Client("pres-p2"); await rp.connect()
        await rp.send({"type": "join_room", "code": codep, "nickname": "PresP2", "colour": "#3498db"})
        jr = await rp.wait_for("joined"); rp_id = jr["player_id"]
        await hp.send({"type": "add_bot"}); await hp.wait_lobby_count(3)
        await hp.send({"type": "add_bot"})
        lobby = await hp.wait_lobby_count(4)
        _pbots = [p["id"] for p in lobby["room"]["players"] if p["is_bot"]]
        # Manual turn order, scrambled (not join order).
        p_manual = [rp_id, _pbots[0], hp_id, _pbots[1]]
        await hp.send({"type": "update_settings", "game": "president",
                       "game_settings": {"president": {"turn_seconds": 5}},
                       "turn_order_mode": "manual",
                       "turn_order": p_manual})
        await hp.wait_lobby_where(
            lambda r: r.get("turn_order_mode") == "manual"
            and r.get("turn_order") == p_manual)
        await hp.send({"type": "start_game"})
        started = await hp.wait_for("game_started", timeout=5)
        check("v2.2 President starts with the manual turn order",
              started["game"] == "president"
              and started.get("turn_order") == p_manual, str(started))

        async def pres_driver(client, my_id, store=None):
            # On my turn, lead with ALL cards of the lowest rank (sheds
            # fast), or play the lowest legal set; otherwise pass.
            while True:
                msg = await wait_any(client, ("president_state", "results"), timeout=60)
                if msg["type"] == "results":
                    return msg
                if store is not None and store.get("first_state") is None:
                    store["first_state"] = msg
                if msg["turn_id"] != my_id:
                    continue
                hand = msg["hand"]
                trick = msg["trick"]
                if not hand: continue  # finished: the v2.3 final president_state is informational, not a turn
                if trick is None:
                    by_rank = {}
                    for c in hand:
                        by_rank.setdefault(pres_rank(c), []).append(c)
                    play = by_rank[min(by_rank)]
                else:
                    play = president_lowest_legal(
                        hand, {"size": trick["size"], "rank": trick["rank"]})
                if play:
                    await client.send({"type": "president_play", "cards": play})
                else:
                    await client.send({"type": "president_pass"})

        p_store = {}
        pres_done = await asyncio.wait_for(asyncio.gather(
            pres_driver(hp, hp_id, p_store), pres_driver(rp, rp_id)), timeout=300)
        pres_res = pres_done[0]
        check("v2 President produces results with a finish order of 4",
              pres_res["game"] == "president" and len(pres_res["rankings"]) == 4,
              str([(r["nickname"], r["score"]) for r in pres_res["rankings"]]))
        check("v2 President: winner scores 1000, last scores 0",
              pres_res["rankings"][0]["score"] == 1000
              and pres_res["rankings"][-1]["score"] == 0,
              str([(r["nickname"], r["score"]) for r in pres_res["rankings"]]))
        check("v2.2 President match 1: everyone is a Citizen",
              p_store["first_state"]["roles"]
              == {pid: "Citizen" for pid in p_manual},
              str(p_store["first_state"]["roles"]))
        check("v2.2 President match 1: rotation is the manual order and "
              "the first player in it leads",
              p_store["first_state"]["turn_order"] == p_manual
              and p_store["first_state"]["turn_id"] == p_manual[0],
              str(p_store["first_state"]))
        _finish = pres_res.get("finish_order", [])
        check("v2 President: results carry the finish order",
              len(_finish) == 4 and len(set(_finish)) == 4, str(_finish))

        # --- v2.2 §6: match 2 (replay) — roles, exchange, President leads ---
        end, started2 = await drive_end_flow(hp, [hp, rp], choice="again")
        check("v2.2 President replay starts the same game again",
              started2["game"] == "president", str(started2))
        check("v2.2 President replay keeps the manual turn order",
              started2.get("turn_order") == p_manual,
              str(started2.get("turn_order")))

        async def pres_match2_driver(client, my_id, store):
            # Match 2: handle the card exchange (pick the lowest cards
            # when I am the giver), record the roles / rotation / first
            # turn, then keep playing legally.
            while True:
                msg = await wait_any(client, ("president_state", "results",
                                              "president_notice"), timeout=90)
                if msg["type"] == "results":
                    store["results"] = msg
                    return msg
                if msg["type"] == "president_notice":
                    continue
                if store.get("roles") is None:
                    store["roles"] = msg.get("roles")
                    store["turn_order"] = msg.get("turn_order")
                if msg.get("exchange"):
                    store["exchange_seen"] = True
                    if msg["exchange"]["giver_id"] == my_id:
                        n = msg["exchange"]["count"]
                        low = sorted(msg["hand"], key=pres_rank)[:n]
                        await client.send({"type": "president_exchange",
                                           "cards": low})
                    continue
                if store.get("first_turn") is None:
                    store["first_turn"] = msg["turn_id"]
                if msg["turn_id"] != my_id:
                    continue
                hand, trick = msg["hand"], msg["trick"]
                if not hand: continue  # finished: the v2.3 final president_state is informational, not a turn
                if trick is None:
                    by_rank = {}
                    for c in hand:
                        by_rank.setdefault(pres_rank(c), []).append(c)
                    play = by_rank[min(by_rank)]
                else:
                    play = president_lowest_legal(
                        hand, {"size": trick["size"], "rank": trick["rank"]})
                if play:
                    await client.send({"type": "president_play", "cards": play})
                else:
                    await client.send({"type": "president_pass"})

        p2_store = {}
        p2_tasks = [asyncio.create_task(pres_match2_driver(hp, hp_id, p2_store)),
                    asyncio.create_task(pres_match2_driver(rp, rp_id, {}))]
        _deadline = time.monotonic() + 60
        while time.monotonic() < _deadline and not (
                p2_store.get("roles") and p2_store.get("exchange_seen")
                and p2_store.get("first_turn")):
            await asyncio.sleep(0.2)
        _expected_roles = dict(zip(
            _finish, president_titles_for_finish(len(_finish))))
        check("v2.2 President match 2: roles come from the match-1 "
              "finish order",
              p2_store.get("roles") == _expected_roles,
              f"got={p2_store.get('roles')} expected={_expected_roles}")
        check("v2.2 President match 2: rotation is the finish order, "
              "strictly (President first … Peasant last)",
              p2_store.get("turn_order") == _finish,
              str(p2_store.get("turn_order")))
        check("v2.2 President match 2: the card exchange ran",
              p2_store.get("exchange_seen") is True)
        check("v2.2 President match 2: the President leads",
              p2_store.get("first_turn") == _finish[0],
              f"first_turn={p2_store.get('first_turn')} finish={_finish}")
        for _t in p2_tasks:
            _t.cancel()
        await asyncio.gather(*p2_tasks, return_exceptions=True)
        await hp.close(); await rp.close()

        # --- v2.1: President illegal plays + timeout auto-play notice ---
        # Fresh 4-player game, short 4s turns. The host probes the
        # error paths on its own turns, then idles so a timeout fires.
        hq = Client("pres2-host"); await hq.connect()
        await hq.send({"type": "create_room", "nickname": "Pres2Host", "colour": "#e74c3c"})
        jq = await hq.wait_for("joined"); codeq = jq["code"]; hq_id = jq["player_id"]
        rq = Client("pres2-p2"); await rq.connect()
        await rq.send({"type": "join_room", "code": codeq, "nickname": "Pres2P2", "colour": "#3498db"})
        jr = await rq.wait_for("joined"); rq_id = jr["player_id"]
        await hq.send({"type": "add_bot"}); await hq.wait_lobby_count(3)
        await hq.send({"type": "add_bot"})
        _lq = await hq.wait_lobby_count(4)
        _qbots = [p["id"] for p in _lq["room"]["players"] if p["is_bot"]]
        # v2.2: manual order with rq leading first (turn_order[0]
        # leads) so rq's pair lead reaches hq on the very next turn.
        _qmanual = [rq_id, hq_id] + _qbots
        await hq.send({"type": "update_settings", "game": "president",
                       "game_settings": {"president": {"turn_seconds": 4}},
                       "turn_order_mode": "manual",
                       "turn_order": _qmanual})
        await hq.wait_lobby_where(
            lambda r: r.get("turn_order_mode") == "manual"
            and r.get("turn_order") == _qmanual)
        await hq.send({"type": "start_game"})
        started = await hq.wait_for("game_started", timeout=5)
        check("v2.1 President (probe game) starts", started["game"] == "president")
        check("v2.2 President probe game uses the manual turn order",
              started.get("turn_order") == _qmanual, str(started))
        probe = {"empty": False, "size": False, "notice": ""}

        async def pres_auto_driver(client, my_id):
            # Play legally forever (used for the second human). When
            # leading, lead the lowest PAIR if one is held — that
            # puts a multi-card trick in front of the host, which is
            # what the size probe needs.
            while True:
                msg = await wait_any(client, ("president_state", "results",
                                              "president_notice"), timeout=90)
                if msg["type"] == "results":
                    return msg
                if msg["type"] == "president_notice":
                    continue
                if msg["turn_id"] != my_id:
                    continue
                hand, trick = msg["hand"], msg["trick"]
                if not hand: continue  # finished: the v2.3 final president_state is informational, not a turn
                if trick is None:
                    by_rank = {}
                    for c in hand:
                        by_rank.setdefault(pres_rank(c), []).append(c)
                    pairs = [g for g in by_rank.values() if len(g) >= 2]
                    play = (min(pairs, key=lambda g: pres_rank(g[0]))
                            if pairs else by_rank[min(by_rank)])
                else:
                    play = president_lowest_legal(
                        hand, {"size": trick["size"], "rank": trick["rank"]})
                if play:
                    await client.send({"type": "president_play", "cards": play})
                else:
                    await client.send({"type": "president_pass"})

        async def pres_probe_driver(client, my_id):
            idle = False  # once set, stop acting so a timeout fires
            while True:
                msg = await wait_any(client, ("president_state", "results",
                                              "president_notice", "error"),
                                     timeout=90)
                if msg["type"] == "results":
                    return msg
                if msg["type"] == "president_notice":
                    probe["notice"] = msg.get("message", "")
                    continue
                if msg["type"] == "error":
                    continue  # errors are asserted inline below
                if msg["turn_id"] != my_id:
                    continue
                if idle:
                    continue  # let the turn time out
                hand, trick = msg["hand"], msg["trick"]
                if not hand: continue  # finished: the v2.3 final president_state is informational, not a turn
                if not probe["empty"]:
                    # Probe 1: Play with NOTHING selected.
                    await client.send({"type": "president_play", "cards": []})
                    err = await wait_any(client, ("error",), timeout=5)
                    check("v2.1 President: empty play gets a specific error",
                          "haven't selected" in err.get("message", ""),
                          str(err))
                    probe["empty"] = True
                    # Turn is still ours and nothing was played: play
                    # a legal set now and the hand must shrink by
                    # exactly those cards.
                    if trick is None:
                        by_rank = {}
                        for c in hand:
                            by_rank.setdefault(pres_rank(c), []).append(c)
                        play = by_rank[min(by_rank)]
                    else:
                        play = president_lowest_legal(
                            hand, {"size": trick["size"],
                                   "rank": trick["rank"]})
                    if play:
                        await client.send({"type": "president_play",
                                           "cards": play})
                        nxt = await wait_any(client, ("president_state",),
                                             timeout=10)
                        check("v2.1 President: after an empty play the turn "
                              "is still ours and the legal play lands",
                              sorted(nxt["hand"]) == sorted(
                                  set(hand) - set(play)),
                              f"hand={hand} play={play} next={nxt['hand']}")
                    else:
                        await client.send({"type": "president_pass"})
                    continue
                if not probe["size"] and trick is not None:
                    # Probe 2: a wrong-SIZE play against the trick.
                    # The size error fires in either direction (one
                    # card vs a pair, or a pair vs a single), so take
                    # whichever wrong-size set the hand offers: a
                    # whole rank group whose size differs from the
                    # trick's. Under the v2.3 one-pass rotation a
                    # multi-card trick is only on the table briefly,
                    # so waiting for one specific direction made this
                    # probe chance-dependent (it flaked); any
                    # wrong-size set proves the same server rule.
                    by_rank = {}
                    for c in hand:
                        by_rank.setdefault(pres_rank(c), []).append(c)
                    wrong = next((g for g in by_rank.values()
                                  if len(g) != trick["size"]), None)
                    if wrong:
                        await client.send({"type": "president_play",
                                           "cards": wrong})
                        err = await wait_any(client, ("error",), timeout=5)
                        check("v2.1 President: wrong-size play gets a "
                              "specific size error",
                              ("play %d card" % trick["size"])
                              in err.get("message", "")
                              and ("not %d" % len(wrong))
                              in err.get("message", ""),
                              str(err))
                        probe["size"] = True
                        # Now play legally (or pass) and stop probing.
                        play = president_lowest_legal(
                            hand, {"size": trick["size"],
                                   "rank": trick["rank"]})
                        if play:
                            await client.send({"type": "president_play",
                                               "cards": play})
                        else:
                            await client.send({"type": "president_pass"})
                        # Both probes done: idle from now on so the
                        # next turn times out and the notice fires.
                        idle = True
                        continue
                    await client.send({"type": "president_pass"})
                    continue
                if trick is None:
                    by_rank = {}
                    for c in hand:
                        by_rank.setdefault(pres_rank(c), []).append(c)
                    play = by_rank[min(by_rank)]
                else:
                    play = president_lowest_legal(
                        hand, {"size": trick["size"], "rank": trick["rank"]})
                if play:
                    await client.send({"type": "president_play", "cards": play})
                else:
                    await client.send({"type": "president_pass"})

        pq1 = asyncio.create_task(pres_probe_driver(hq, hq_id))
        pq2 = asyncio.create_task(pres_auto_driver(rq, rq_id))
        pres2_res = await asyncio.wait_for(asyncio.gather(pq1, pq2),
                                           timeout=300)
        check("v2.1 President: empty-selection probe fired",
              probe["empty"])
        check("v2.1 President: wrong-size probe fired",
              probe["size"])
        check("v2.1 President: timeout auto-play broadcasts a notice",
              "ran out of time" in probe["notice"], probe["notice"])
        check("v2.1 President: probe game still produces results",
              pres2_res[0]["game"] == "president"
              and len(pres2_res[0]["rankings"]) == 4)
        await hq.close(); await rq.close()

        # --- v2.3 items 8 + 10: President final state, titles,
        # end-vote denominator, and the end flow back to lobby ---
        # The last finisher (Peasant) never appeared in-game in
        # v2.2: no final president_state was sent after they were
        # appended, and the results screen showed no titles. Now
        # the server sends a final president_state before the end
        # flow, and results_extra.titles names every finisher.
        hf8 = Client("pres8-host"); await hf8.connect()
        await hf8.send({"type": "create_room", "nickname": "Pres8Host", "colour": "#e74c3c"})
        jf8 = await hf8.wait_for("joined"); code8 = jf8["code"]; hf8_id = jf8["player_id"]
        rf8 = Client("pres8-p2"); await rf8.connect()
        await rf8.send({"type": "join_room", "code": code8, "nickname": "Pres8P2", "colour": "#3498db"})
        jr8 = await rf8.wait_for("joined"); rf8_id = jr8["player_id"]
        await hf8.send({"type": "add_bot"}); await hf8.wait_lobby_count(3)
        await hf8.send({"type": "add_bot"})
        lobby8 = await hf8.wait_lobby_count(4)
        _b8 = [p["id"] for p in lobby8["room"]["players"] if p["is_bot"]]
        p8_manual = [rf8_id, _b8[0], hf8_id, _b8[1]]
        await hf8.send({"type": "update_settings", "game": "president",
                        "game_settings": {"president": {"turn_seconds": 5}},
                        "turn_order_mode": "manual",
                        "turn_order": p8_manual})
        await hf8.wait_lobby_where(
            lambda r: r.get("turn_order_mode") == "manual"
            and r.get("turn_order") == p8_manual)
        await hf8.send({"type": "start_game"})
        started8 = await hf8.wait_for("game_started", timeout=5)
        check("v2.3 item 8: President final-state game starts",
              started8["game"] == "president", str(started8))

        async def pres_final_driver(client, my_id, store=None):
            while True:
                msg = await wait_any(client, ("president_state", "results",
                                              "president_notice"), timeout=90)
                if msg["type"] == "results":
                    return msg
                if msg["type"] == "president_notice":
                    continue
                if store is not None:
                    store.setdefault("states", []).append(msg)
                if msg.get("exchange"):
                    if msg["exchange"]["giver_id"] == my_id:
                        n = msg["exchange"]["count"]
                        low = sorted(msg["hand"], key=pres_rank)[:n]
                        await client.send({"type": "president_exchange",
                                           "cards": low})
                    continue
                if msg["turn_id"] != my_id:
                    continue
                hand, trick = msg["hand"], msg["trick"]
                if not hand: continue  # finished: the v2.3 final president_state is informational, not a turn
                if trick is None:
                    by_rank = {}
                    for c in hand:
                        by_rank.setdefault(pres_rank(c), []).append(c)
                    play = by_rank[min(by_rank)]
                else:
                    play = president_lowest_legal(
                        hand, {"size": trick["size"], "rank": trick["rank"]})
                if play:
                    await client.send({"type": "president_play", "cards": play})
                else:
                    await client.send({"type": "president_pass"})

        p8_store = {}
        pres8_done = await asyncio.wait_for(asyncio.gather(
            pres_final_driver(hf8, hf8_id, p8_store),
            pres_final_driver(rf8, rf8_id)), timeout=300)
        pres8_res = pres8_done[0]
        _finish8 = pres8_res.get("finish_order", [])
        _last_state = p8_store["states"][-1] if p8_store.get("states") else {}
        check("v2.3 item 8: a final president_state is sent after "
              "the last finisher is appended (all 4 finished, "
              "Peasant title visible in-game)",
              len(_last_state.get("finished", [])) == 4
              and _last_state.get("titles", {}).get(_finish8[-1] if _finish8 else None)
              == "Peasant",
              str({"finished": _last_state.get("finished"),
                   "titles": _last_state.get("titles")}))
        check("v2.3 item 8: results carry titles for the whole "
              "finish order (President first, Peasant last)",
              len(_finish8) == 4
              and pres8_res.get("titles", {}).get(_finish8[0]) == "President"
              and pres8_res.get("titles", {}).get(_finish8[-1]) == "Peasant"
              and [pres8_res.get("titles", {}).get(pid) for pid in _finish8]
              == president_titles_for_finish(4),
              str(pres8_res.get("titles")))
        # v2.3 item 10 (President half) + item 12 (lobby return):
        # the end-of-game vote needs 1 vote with 2 humans, and
        # replay_vote "lobby" returns to the lobby.
        await drive_end_flow(hf8, [hf8, rf8], choice="lobby", count=4,
                             expect_votes_needed=1)
        await hf8.close(); await rf8.close()

        # --- v2.2 §5: UNO full game (2 humans + 1 bot) ---
        # v2.2: drawn cards are auto-played when playable (no more
        # "drawn" offer to keep), uno_bot_choice returns a LIST, and
        # scoring is purely the finishing-order scale by cards left.
        hu = Client("uno-host"); await hu.connect()
        await hu.send({"type": "create_room", "nickname": "UnoHost", "colour": "#e74c3c"})
        ju = await hu.wait_for("joined"); codeu = ju["code"]; hu_id = ju["player_id"]
        ru = Client("uno-p2"); await ru.connect()
        await ru.send({"type": "join_room", "code": codeu, "nickname": "UnoP2", "colour": "#3498db"})
        jr = await ru.wait_for("joined"); ru_id = jr["player_id"]
        await hu.send({"type": "add_bot"})
        lobby = await hu.wait_lobby_count(3)
        _ubot = [p["id"] for p in lobby["room"]["players"] if p["is_bot"]][0]
        # Manual turn order, scrambled (not join order).
        u_manual = [ru_id, _ubot, hu_id]
        await hu.send({"type": "update_settings", "game": "uno",
                       "game_settings": {"uno": {"turn_seconds": 5}},
                       "turn_order_mode": "manual",
                       "turn_order": u_manual})
        await hu.wait_lobby_where(
            lambda r: r.get("turn_order_mode") == "manual"
            and r.get("turn_order") == u_manual)
        await hu.send({"type": "start_game"})
        started = await hu.wait_for("game_started", timeout=5)
        check("v2.2 UNO starts with the manual turn order",
              started["game"] == "uno"
              and started.get("turn_order") == u_manual, str(started))

        async def uno_driver(client, my_id, store=None):
            # v2.2: on my turn, play the bot's pick (a list — one card
            # normally, a same-value pair with multi-play on) or draw.
            # A drawn playable card is auto-played by the server, so
            # there is no drawn-card offer to answer anymore.
            while True:
                msg = await wait_any(client, ("uno_state", "results"), timeout=60)
                if msg["type"] == "results":
                    return msg
                if store is not None and store.get("first_turn") is None:
                    store["first_turn"] = msg["turn_id"]
                    # v2.3 items 3 + 7: keep the whole first state —
                    # turn_order and the highlighting inputs are
                    # asserted after the game.
                    store["first_state"] = msg
                    # The opening discard can be a Draw 2: real UNO
                    # makes the first player draw 2 and lose the turn,
                    # so the first state then belongs to player #2.
                    store["first_top"] = msg.get("top")
                if msg["turn_id"] != my_id:
                    continue
                choice = uno_bot_choice(
                    msg["hand"], msg["top"], msg["current_colour"],
                    pending_type=msg.get("pending_type"),
                    multi_play=msg.get("multi_play", False))
                if choice:
                    colour = None
                    if any(c.startswith("W") for c in choice):
                        colour = uno_best_colour(
                            [c for c in msg["hand"] if c not in choice]) or "R"
                    await client.send({"type": "uno_play", "cards": choice,
                                       "colour": colour})
                else:
                    await client.send({"type": "uno_draw"})

        u_store = {}
        uno_done = await asyncio.wait_for(asyncio.gather(
            uno_driver(hu, hu_id, u_store), uno_driver(ru, ru_id)), timeout=300)
        uno_res = uno_done[0]
        _first_top = u_store.get("first_top")
        _draw2_start = (_first_top is not None
                        and uno_value(_first_top) == "draw2")
        check("v2.2 UNO: the first player in the manual order starts "
              "(or #2 if the opening card is a Draw 2, which skips #1)",
              u_store.get("first_turn") == u_manual[0]
              or (_draw2_start
                  and u_store.get("first_turn") == u_manual[1]),
              f"first_turn={u_store.get('first_turn')} top={_first_top}")
        # --- v2.3 item 3: uno_state carries turn_order ---
        _ufs = u_store.get("first_state", {})
        check("v2.3 item 3: uno_state carries turn_order (the "
              "rotation, here the manual order)",
              _ufs.get("turn_order") == u_manual,
              str(_ufs.get("turn_order")))
        # --- v2.3 item 7: uno_state carries everything the client
        # needs to highlight playable cards (top card, current
        # colour, pending draw/type, multi-play flag, own hand).
        # The rendering itself is client-side.
        check("v2.3 item 7: uno_state carries the highlighting "
              "inputs (top, current_colour, pending_draw/type, "
              "multi_play, hand)",
              all(k in _ufs for k in ("top", "current_colour",
                                      "pending_draw", "pending_type",
                                      "multi_play", "hand"))
              and isinstance(_ufs.get("hand"), list)
              and len(_ufs["hand"]) == 7,
              str({k: _ufs.get(k) for k in
                   ("top", "current_colour", "pending_draw",
                    "pending_type", "multi_play")}))
        _uno_scale = president_scores_for(3)
        check("v2.2 UNO produces results on the finishing-order scale "
              "(winner 1000, 2nd 500, 3rd 0 — no winner bonus)",
              uno_res["game"] == "uno"
              and uno_res["rankings"][0].get("cards_left") == 0
              and [r["score"] for r in uno_res["rankings"]] == _uno_scale
              and [r.get("cards_left") for r in uno_res["rankings"]]
              == sorted(r.get("cards_left") for r in uno_res["rankings"]),
              str([(r["nickname"], r["score"], r.get("cards_left"))
                   for r in uno_res["rankings"]]))
        await drive_end_flow(hu, [hu, ru], choice="lobby", count=3)
        await hu.close(); await ru.close()

        # --- v2.2 §5: UNO with all house rules ON (2 humans + 1 bot) ---
        # Draw-until-playable + stacking + multi-play must flow through
        # settings into a complete, correctly scored game.
        hw = Client("unohouse-host"); await hw.connect()
        await hw.send({"type": "create_room", "nickname": "UnoHouseHost", "colour": "#e74c3c"})
        jw = await hw.wait_for("joined"); codew = jw["code"]; hw_id = jw["player_id"]
        rw = Client("unohouse-p2"); await rw.connect()
        await rw.send({"type": "join_room", "code": codew, "nickname": "UnoHouseP2", "colour": "#3498db"})
        jr = await rw.wait_for("joined"); rw_id = jr["player_id"]
        await hw.send({"type": "add_bot"}); await hw.wait_lobby_count(3)
        await hw.send({"type": "update_settings", "game": "uno",
                       "game_settings": {"uno": {"turn_seconds": 5,
                                                 "draw_until_playable": True,
                                                 "stacking": True,
                                                 "multi_play": True}}})
        await hw.wait_lobby()
        await hw.send({"type": "start_game"})
        started = await hw.wait_for("game_started", timeout=5)
        check("v2.2 UNO house-rules game starts",
              started["game"] == "uno"
              and started["settings"].get("draw_until_playable") is True
              and started["settings"].get("stacking") is True
              and started["settings"].get("multi_play") is True,
              str(started.get("settings")))
        house_done = await asyncio.wait_for(asyncio.gather(
            uno_driver(hw, hw_id), uno_driver(rw, rw_id)), timeout=300)
        house_res = house_done[0]
        check("v2.2 UNO house-rules game finishes on the same scale",
              house_res["game"] == "uno"
              and [r["score"] for r in house_res["rankings"]] == _uno_scale,
              str([(r["nickname"], r["score"], r.get("cards_left"))
                   for r in house_res["rankings"]]))
        await drive_end_flow(hw, [hw, rw], choice="lobby", count=3)
        await hw.close(); await rw.close()

        # --- v2: Memory game (3 humans; host answers right, others wrong) ---
        hm = Client("mem-host"); await hm.connect()
        await hm.send({"type": "create_room", "nickname": "MemHost", "colour": "#e74c3c"})
        jm = await hm.wait_for("joined"); codem = jm["code"]
        m2 = Client("mem-p2"); await m2.connect()
        await m2.send({"type": "join_room", "code": codem, "nickname": "MemP2", "colour": "#3498db"})
        await m2.wait_for("joined")
        m3 = Client("mem-p3"); await m3.connect()
        await m3.send({"type": "join_room", "code": codem, "nickname": "MemP3", "colour": "#2ecc71"})
        await m3.wait_for("joined")
        await hm.send({"type": "update_settings", "game": "memory",
                       "game_settings": {"memory": {"memorize_seconds": 2, "recall_seconds": 5}}})
        await hm.wait_lobby()
        await hm.send({"type": "start_game"})
        started = await hm.wait_for("game_started", timeout=5)
        check("v2 Memory starts", started["game"] == "memory")

        async def mem_driver(client, correct, rounds_seen=None):
            number = None
            while True:
                msg = await wait_any(client, ("memory_show", "memory_recall",
                                              "memory_round_result",
                                              "vote_break", "results"),
                                     timeout=30)
                if msg["type"] == "memory_show":
                    number = msg["number"]
                    check(f"v2 Memory: round {msg['round']} number has {msg['digits']} digits",
                          len(str(msg["number"])) == msg["digits"], str(msg))
                elif msg["type"] == "memory_recall":
                    await client.send({"type": "memory_answer",
                                       "answer": number if correct else "0000"})
                elif msg["type"] == "memory_round_result":
                    if rounds_seen is not None:
                        rounds_seen.append(msg)
                elif msg["type"] == "vote_break":
                    # v2.2 §3: a NEXT vote follows each non-final round.
                    await client.send({"type": "vote_next",
                                       "index": msg["index"]})
                elif msg["type"] == "results":
                    return msg

        mem_done = await asyncio.wait_for(asyncio.gather(
            mem_driver(hm, True), mem_driver(m2, False), mem_driver(m3, False)),
            timeout=120)
        mem_res = mem_done[0]
        check("v2 Memory: host wins by answering right, others score 0",
              mem_res["game"] == "memory"
              and mem_res["rankings"][0]["nickname"] == "MemHost"
              and mem_res["rankings"][0]["score"] == 1000
              and all(r["score"] == 0 for r in mem_res["rankings"][1:]),
              str([(r["nickname"], r["score"]) for r in mem_res["rankings"]]))
        await hm.close(); await m2.close(); await m3.close()

        # --- v2: Memory tie-redo (2 humans; both wrong, then host right) ---
        ht = Client("tie-host"); await ht.connect()
        await ht.send({"type": "create_room", "nickname": "TieHost", "colour": "#e74c3c"})
        jt = await ht.wait_for("joined"); codet = jt["code"]
        rt = Client("tie-p2"); await rt.connect()
        await rt.send({"type": "join_room", "code": codet, "nickname": "TieP2", "colour": "#3498db"})
        await rt.wait_for("joined")
        await ht.send({"type": "update_settings", "game": "memory",
                       "game_settings": {"memory": {"memorize_seconds": 2, "recall_seconds": 5}}})
        await ht.wait_lobby()
        await ht.send({"type": "start_game"})
        await ht.wait_for("game_started", timeout=5)

        async def mem_tie_driver(client, plan, rounds_seen=None):
            # plan: one boolean per memory_show seen (wrong, then right).
            shows = 0
            number = None
            while True:
                msg = await wait_any(client, ("memory_show", "memory_recall",
                                              "memory_round_result",
                                              "vote_break", "results"),
                                     timeout=30)
                if msg["type"] == "memory_show":
                    number = msg["number"]
                    shows += 1
                elif msg["type"] == "memory_recall":
                    ok = plan[min(shows - 1, len(plan) - 1)]
                    await client.send({"type": "memory_answer",
                                       "answer": number if ok else "0000"})
                elif msg["type"] == "memory_round_result":
                    if rounds_seen is not None:
                        rounds_seen.append(msg)
                elif msg["type"] == "vote_break":
                    # v2.2 §3: the tie redo is gated by a NEXT vote —
                    # without this the test would wait out the 60s
                    # default timeout after the tied round.
                    await client.send({"type": "vote_next",
                                       "index": msg["index"]})
                elif msg["type"] == "results":
                    return msg

        tie_rounds = []
        tie_done = await asyncio.wait_for(asyncio.gather(
            mem_tie_driver(ht, [False, True], tie_rounds),
            mem_tie_driver(rt, [False, False])), timeout=120)
        tie_res = tie_done[0]
        check("v2 Memory tie: first round eliminates everyone (a tie)",
              len(tie_rounds) >= 2 and len(tie_rounds[0]["survivors"]) == 0,
              str(tie_rounds))
        check("v2 Memory tie: redo is the same digit length (4)",
              tie_rounds[1]["digits"] == tie_rounds[0]["digits"] == 4,
              str([(r["round"], r["digits"]) for r in tie_rounds]))
        check("v2 Memory tie: host wins after the redo",
              tie_res["rankings"][0]["nickname"] == "TieHost"
              and tie_res["rankings"][0]["score"] == 1000,
              str([(r["nickname"], r["score"]) for r in tie_res["rankings"]]))
        await ht.close(); await rt.close()

        # --- v2.3 item 5: Memory round_start precedes memory_show ---
        # v2.2 regression: the server sent each human their
        # memory_show BEFORE broadcasting memory_round_start, and
        # the client's round_start handler clears the canvas — so
        # the just-drawn number was erased and nothing was shown
        # to memorize. The client-visible order must be
        # round_start first, then every memory_show for that round.
        hm5 = Client("mem5-host"); await hm5.connect()
        await hm5.send({"type": "create_room", "nickname": "Mem5Host", "colour": "#e74c3c"})
        jm5 = await hm5.wait_for("joined"); code5 = jm5["code"]
        m5 = Client("mem5-p2"); await m5.connect()
        await m5.send({"type": "join_room", "code": code5, "nickname": "Mem5P2", "colour": "#3498db"})
        await m5.wait_for("joined")
        await hm5.send({"type": "update_settings", "game": "memory",
                        "game_settings": {"memory": {"memorize_seconds": 2, "recall_seconds": 5}}})
        await hm5.wait_lobby()
        await hm5.send({"type": "start_game"})
        await hm5.wait_for("game_started", timeout=5)

        async def mem_order_driver(client, correct, order_log):
            number = None
            while True:
                msg = await wait_any(client, ("memory_show", "memory_round_start",
                                              "memory_recall", "memory_round_result",
                                              "vote_break", "results"), timeout=30)
                if msg["type"] == "memory_round_start":
                    order_log.append(("round_start", msg["round"]))
                elif msg["type"] == "memory_show":
                    order_log.append(("show", msg["round"]))
                    number = msg["number"]
                elif msg["type"] == "memory_recall":
                    await client.send({"type": "memory_answer",
                                       "answer": number if correct else "0000"})
                elif msg["type"] == "vote_break":
                    await client.send({"type": "vote_next",
                                       "index": msg["index"]})
                elif msg["type"] == "results":
                    return msg

        order5 = []
        mem5_done = await asyncio.wait_for(asyncio.gather(
            mem_order_driver(hm5, True, order5),
            mem_order_driver(m5, False, [])), timeout=120)
        check("v2.3 item 5: memory_round_start is broadcast BEFORE "
              "the memory_show for the same round (numbers stay "
              "visible for the full memorize time)",
              ("round_start", 1) in order5 and ("show", 1) in order5
              and order5.index(("round_start", 1))
              < order5.index(("show", 1)),
              str(order5))
        check("v2.3 item 5: the order-fixed Memory game still "
              "finishes (host wins)",
              mem5_done[0]["game"] == "memory"
              and mem5_done[0]["rankings"][0]["nickname"] == "Mem5Host",
              str([(r["nickname"], r["score"])
                   for r in mem5_done[0]["rankings"]]))
        await hm5.close(); await m5.close()

        # --- v2: Elimination series end-to-end (2 humans + 2 bots) ---
        he = Client("elim-host"); await he.connect()
        await he.send({"type": "create_room", "nickname": "ElimHost", "colour": "#e74c3c"})
        je = await he.wait_for("joined"); codee = je["code"]; he_id = je["player_id"]
        re_ = Client("elim-p2"); await re_.connect()
        await re_.send({"type": "join_room", "code": codee, "nickname": "ElimP2", "colour": "#3498db"})
        jr = await re_.wait_for("joined"); re_id = jr["player_id"]
        await he.send({"type": "add_bot"}); await he.wait_lobby_count(3)
        await he.send({"type": "add_bot"}); await he.wait_lobby_count(4)
        # v2.1: short 3s vote-break timeout so the pacing breaks
        # (and the no-vote timeout path below) stay fast in the test.
        await he.send({"type": "update_settings", "game": "elimination",
                       "game_settings": {"elimination": {"vote_timeout": 3}}})
        await he.wait_lobby()
        elim_state = {"results": [], "eliminated": [], "challenges": [],
                      "lobby": False, "breaks": [], "events": []}

        async def elim_driver(client, my_id):
            number = None
            my_end_votes = 0
            while True:
                msg = await wait_any(client, (
                    "question", "reaction_wait", "reaction_go",
                    "memory_show", "memory_recall",
                    "elimination_challenge", "elimination_eliminated",
                    "vote_break",
                    "results", "elimination_end_vote", "lobby"), timeout=120)
                t = msg["type"]
                elim_state["events"].append(
                    (t, msg.get("game") or msg.get("game_name")))
                if t == "vote_break":
                    # v2.1 pacing break between challenges. Vote NEXT
                    # in the FIRST series only; in the second series
                    # (after the first end vote) don't vote, so the
                    # break must advance at the 3s timeout instead.
                    if msg.get("elim_break"):
                        elim_state["breaks"].append(
                            (msg["index"], msg.get("eliminated"),
                             msg.get("remaining"), msg["votes_needed"],
                             msg["timeout"]))
                        if my_end_votes == 0:
                            await client.send({"type": "vote_next",
                                               "index": msg["index"]})
                    elif my_end_votes == 0:
                        await client.send({"type": "vote_next",
                                           "index": msg["index"]})
                elif t == "question":
                    if msg.get("options"):
                        await client.send({"type": "answer", "answer": 0,
                                           "index": msg["index"]})
                    else:
                        ans = solve_math(msg["text"])
                        await client.send({"type": "answer", "answer": str(ans),
                                           "index": msg["index"]})
                elif t == "reaction_go":
                    await client.send({"type": "reaction_click"})
                elif t == "memory_show":
                    number = msg["number"]
                elif t == "memory_recall":
                    await client.send({"type": "memory_answer",
                                       "answer": number or "0000"})
                elif t == "elimination_challenge":
                    elim_state["challenges"].append(
                        (msg["game"], msg.get("finale", False)))
                elif t == "elimination_eliminated":
                    elim_state["eliminated"].append(msg["nickname"])
                elif t == "results":
                    # Count each series ONCE (host only): both drivers
                    # see the same broadcast, so appending from both
                    # would make one series look like two.
                    if client is he:
                        elim_state["results"].append(msg)
                elif t == "elimination_end_vote":
                    # First end vote -> Play Again (new series);
                    # second end vote -> Return to Lobby, forced by the
                    # HOST's host_replay override (v2.2) instead of a
                    # community vote.
                    my_end_votes += 1
                    if my_end_votes == 1:
                        await client.send({"type": "elimination_vote",
                                           "choice": "again"})
                    elif client is he:
                        elim_state["host_replay_used"] = True
                        await client.send({"type": "host_replay",
                                           "choice": "lobby"})
                elif t == "lobby":
                    if msg["room"]["phase"] == "lobby" and elim_state["results"]:
                        elim_state["lobby"] = True
                        return

        ed1 = asyncio.create_task(elim_driver(he, he_id))
        ed2 = asyncio.create_task(elim_driver(re_, re_id))
        await he.send({"type": "start_game"})
        deadline = time.monotonic() + 420
        while time.monotonic() < deadline and not elim_state["lobby"]:
            await asyncio.sleep(1)
        check("v2 Elimination: two series ran (Play Again, then Lobby)",
              len(elim_state["results"]) == 2,
              f"results={len(elim_state['results'])} challenges={elim_state['challenges']}")
        check("v2.3 item 15: challenges came from the quick-game pool "
              "with NO finale (3 challenges per series x 2 series, "
              "seen by both drivers)",
              all(g in ("reaction", "math", "trivia", "memory")
                  for g, _ in elim_state["challenges"])
              and not any(f for _, f in elim_state["challenges"])
              and len(elim_state["challenges"]) == 12,
              str(elim_state["challenges"]))
        check("v2.3 item 15: one player eliminated per challenge until "
              "1 remains (3 per series x 2 series, seen by both drivers)",
              len(elim_state["eliminated"]) == 12,
              str(elim_state["eliminated"]))
        first = elim_state["results"][0]
        check("v2 Elimination: finishing order lists every player once",
              first["game"] == "elimination"
              and len(first.get("finishing_order", [])) == 4
              and len({r["player_id"] for r in first["finishing_order"]}) == 4
              and first["finishing_order"][0]["player_id"] == first["victor"],
              str(first.get("finishing_order")))
        check("v2.3 item 15: only the last remaining player scores "
              "(victor 1000, everyone else 0 — no finale places)",
              first["finishing_order"][0]["score"] == 1000
              and all(r["score"] == 0
                      for r in first["finishing_order"][1:]),
              str([(r["nickname"], r["score"]) for r in first["finishing_order"]]))
        check("v2 Elimination: series ends back in the lobby",
              elim_state["lobby"])
        check("v2.2 Elimination: the second end vote was forced by the "
              "host's host_replay override (not a community vote)",
              elim_state.get("host_replay_used") is True)
        # --- v2.1: pacing breaks + intros ---
        # At least one break per challenge in each series (regular
        # challenges + finale), each naming who was eliminated and
        # how many remain, with the 3s test timeout.
        check("v2.1 Elimination: a NEXT vote break follows every challenge",
              len(elim_state["breaks"]) >= len(elim_state["challenges"]),
              f"breaks={elim_state['breaks']} challenges={elim_state['challenges']}")
        check("v2.1 Elimination: breaks name the eliminated player and "
              "the players remaining, with the short test timeout",
              all(b[1] and isinstance(b[2], int) and b[2] >= 1
                  and b[4] == 3 for b in elim_state["breaks"]),
              str(elim_state["breaks"]))
        # Every challenge was announced (intro) by name BEFORE any
        # of its questions / reaction rounds started.
        _evs = elim_state["events"]
        _intro_ok = True
        for i, (t, g) in enumerate(_evs):
            if t == "elimination_challenge":
                if g not in GAMES:
                    _intro_ok = False
                # the next game-play event must come after this intro
                _rest = [x for x, _ in _evs[i + 1:]]
                if not any(x in ("question", "reaction_wait", "memory_show",
                                 "results", "elimination_end_vote")
                           for x in _rest):
                    _intro_ok = False
        check("v2.1 Elimination: every challenge intro names its game",
              _intro_ok and len(elim_state["challenges"]) >= 2,
              str(elim_state["challenges"]))
        await asyncio.wait_for(asyncio.gather(ed1, ed2), timeout=30)
        await he.close(); await re_.close()

        # ================= v2.2 tests: replay / end flow =================
        # --- host_replay "again" replays the same game, scores reset ---
        hr1 = Client("replay-host"); await hr1.connect()
        await hr1.send({"type": "create_room", "nickname": "ReplayHost", "colour": "#e74c3c"})
        j1 = await hr1.wait_for("joined"); code1 = j1["code"]
        r1 = Client("replay-p2"); await r1.connect()
        await r1.send({"type": "join_room", "code": code1, "nickname": "ReplayP2", "colour": "#3498db"})
        await r1.wait_for("joined")
        await hr1.send({"type": "update_settings", "game": "math",
                        "game_settings": {"math": {"difficulty": "Easy", "questions": 2,
                                                   "time_per_question": 5, "vote_timeout": 3}}})
        await hr1.wait_lobby()
        await hr1.send({"type": "start_game"})
        await hr1.wait_for("game_started")
        for qi in (1, 2):
            mq = await hr1.wait_for("question", timeout=15)
            aq = solve_math(mq["text"])
            await hr1.send({"type": "answer", "answer": str(aq), "index": mq["index"]})
            await r1.send({"type": "answer", "answer": str(aq + 1), "index": mq["index"]})
            res = await hr1.wait_for("question_result", timeout=10)
            if qi == 1:
                await hr1.wait_for("vote_break", timeout=10)
                await hr1.send({"type": "vote_next", "index": 1})
        g1_final = {r["nickname"]: r["score"] for r in
                    (await hr1.wait_for("results", timeout=10))["rankings"]}
        check("v2.2 replay setup: host scored in game 1",
              g1_final.get("ReplayHost", 0) > 0, str(g1_final))
        # End-of-game vote, then the HOST overrides with host_replay.
        vb = await hr1.wait_for("vote_break", timeout=10)
        check("v2.2 end-of-game vote break is flagged end_of_game",
              vb.get("end_of_game") is True, str(vb))
        # Drive the end-of-game vote with host_skip, then host_replay "again".
        await hr1.send({"type": "host_skip"})
        end = await hr1.wait_for("end_choice", timeout=10)
        check("v2.2 end_choice carries the final standings",
              len(end["rankings"]) == 2
              and all("score" in r for r in end["rankings"]),
              str(end["rankings"]))
        await hr1.send({"type": "host_replay", "choice": "again"})
        started = await hr1.wait_for("game_started", timeout=10)
        check("v2.2 host_replay 'again' replays the same game",
              started["game"] == "math", str(started))
        # Game 2, Q1: the host's score restarts from 0 (not the
        # game-1 total) — scores reset on a non-championship replay.
        mq = await hr1.wait_for("question", timeout=15)
        aq = solve_math(mq["text"])
        await hr1.send({"type": "answer", "answer": str(aq), "index": mq["index"]})
        await r1.send({"type": "answer", "answer": str(aq + 1), "index": mq["index"]})
        res = await hr1.wait_for("question_result", timeout=10)
        _host_pts = next(p["points"] for p in res["points"]
                         if p["player_id"] == j1["player_id"])
        _host_score = next(s["score"] for s in res["scores"]
                           if s["player_id"] == j1["player_id"])
        check("v2.2 replay resets scores (game 2 score == Q1 points only, "
              "not game 1 total + Q1 points)",
              _host_score == _host_pts,
              f"game2 score={_host_score} (Q1 points={_host_pts}) "
              f"game1 final={g1_final.get('ReplayHost')}")
        await hr1.wait_for("vote_break", timeout=10)
        await hr1.send({"type": "vote_next", "index": 1})
        mq = await hr1.wait_for("question", timeout=15)
        aq = solve_math(mq["text"])
        await hr1.send({"type": "answer", "answer": str(aq), "index": mq["index"]})
        await r1.send({"type": "answer", "answer": str(aq + 1), "index": mq["index"]})
        await hr1.wait_for("question_result", timeout=10)
        await hr1.wait_for("results", timeout=10)
        # host_replay "lobby" returns to the lobby.
        await drive_end_flow(hr1, [hr1, r1], choice="lobby", count=2,
                             method="skip")
        await hr1.close(); await r1.close()

        # --- host_replay "lobby" (host override, no community vote) ---
        hl = Client("hreplay-host"); await hl.connect()
        await hl.send({"type": "create_room", "nickname": "HLHost", "colour": "#e74c3c"})
        jl = await hl.wait_for("joined"); codel = jl["code"]
        rl = Client("hreplay-p2"); await rl.connect()
        await rl.send({"type": "join_room", "code": codel, "nickname": "HLP2", "colour": "#3498db"})
        await rl.wait_for("joined")
        await hl.send({"type": "update_settings", "game": "math",
                       "game_settings": {"math": {"difficulty": "Easy", "questions": 1,
                                                  "time_per_question": 5, "vote_timeout": 3}}})
        await hl.wait_lobby()
        await hl.send({"type": "start_game"})
        await hl.wait_for("game_started")
        mq = await hl.wait_for("question", timeout=15)
        aq = solve_math(mq["text"])
        await hl.send({"type": "answer", "answer": str(aq), "index": mq["index"]})
        await rl.send({"type": "answer", "answer": str(aq + 1), "index": mq["index"]})
        await hl.wait_for("question_result", timeout=10)
        await hl.wait_for("results", timeout=10)
        vb = await hl.wait_for("vote_break", timeout=10)
        check("v2.2 end-of-game vote break is flagged end_of_game (2)",
              vb.get("end_of_game") is True, str(vb))
        await hl.send({"type": "host_skip"})
        await hl.wait_for("end_choice", timeout=10)
        await hl.send({"type": "host_replay", "choice": "lobby"})
        lobby = await hl.wait_lobby_phase("lobby", 2, timeout=10)
        check("v2.2 host_replay 'lobby' returns to the lobby with no "
              "community vote",
              lobby["room"]["phase"] == "lobby")
        await hl.close(); await rl.close()

        # --- replay timeout defaults to the lobby (60s) ---
        ht = Client("timeout-host"); await ht.connect()
        await ht.send({"type": "create_room", "nickname": "TOHost", "colour": "#e74c3c"})
        jt = await ht.wait_for("joined"); codet = jt["code"]
        rt = Client("timeout-p2"); await rt.connect()
        await rt.send({"type": "join_room", "code": codet, "nickname": "TOP2", "colour": "#3498db"})
        await rt.wait_for("joined")
        await ht.send({"type": "update_settings", "game": "math",
                       "game_settings": {"math": {"difficulty": "Easy", "questions": 1,
                                                  "time_per_question": 5, "vote_timeout": 3}}})
        await ht.wait_lobby()
        await ht.send({"type": "start_game"})
        await ht.wait_for("game_started")
        mq = await ht.wait_for("question", timeout=15)
        aq = solve_math(mq["text"])
        await ht.send({"type": "answer", "answer": str(aq), "index": mq["index"]})
        await rt.send({"type": "answer", "answer": str(aq + 1), "index": mq["index"]})
        await ht.wait_for("question_result", timeout=10)
        await ht.wait_for("results", timeout=10)
        await ht.wait_for("vote_break", timeout=10)
        await ht.send({"type": "vote_next", "index": 1})
        await ht.wait_for("end_choice", timeout=10)
        # Nobody votes in the replay vote: the 60s timeout must
        # return the room to the lobby. Drain the stale lobby
        # broadcasts (join / settings) first, or the timed wait
        # below would return at once on an old one.
        await ht.drain("lobby")
        t0 = time.monotonic()
        lobby = await ht.wait_lobby_phase("lobby", 2, timeout=75)
        elapsed = time.monotonic() - t0
        check("v2.2 replay vote timeout returns to the lobby (~60s)",
              lobby["room"]["phase"] == "lobby" and elapsed >= 55,
              f"elapsed={elapsed:.1f}s")
        await ht.close(); await rt.close()

        # --- v2.2 §1: replay of a "random" game re-rolls a new game ---
        hr_clients = []
        hr_ids = []
        hr_code = None
        for i, (nm, col) in enumerate([("RandHost", "#e74c3c"),
                                       ("RandP2", "#3498db"),
                                       ("RandP3", "#2ecc71")]):
            c = Client(f"rand-{i}")
            await c.connect()
            if i == 0:
                await c.send({"type": "create_room", "nickname": nm,
                              "colour": col})
            else:
                await c.send({"type": "join_room", "code": hr_code,
                              "nickname": nm, "colour": col})
            j = await c.wait_for("joined")
            if i == 0:
                hr_code = j["code"]
            hr_ids.append(j["player_id"])
            hr_clients.append(c)
        hr = hr_clients[0]
        await hr.send({"type": "update_settings", "game": "random",
                       "game_settings": {
                           "reaction": {"rounds": 1},
                           "math": {"difficulty": "Easy", "questions": 1,
                                    "time_per_question": 5, "vote_timeout": 3},
                           "trivia": {"category": "Mixed", "difficulty": "Easy",
                                      "questions": 1, "time_per_question": 5,
                                      "vote_timeout": 3},
                           "memory": {"memorize_seconds": 2,
                                      "recall_seconds": 5},
                           "uno": {"turn_seconds": 3},
                           "president": {"turn_seconds": 3}}})
        await hr.wait_lobby_where(lambda r: r.get("game") == "random")
        # Start a RANDOM game; if the draw needs 4+ players (Secret /
        # President) with only 3 in the room, start_game errors — retry
        # until a playable game is drawn.
        first_game = None
        for _attempt in range(10):
            await hr.send({"type": "start_game"})
            msg = await wait_any(hr, ("game_started", "error"), timeout=5)
            if msg["type"] == "game_started":
                first_game = msg["game"]
                break
        check("v2.2 random game starts (a playable draw)",
              first_game is not None, f"first_game={first_game}")
        rand_state = {"games": [first_game], "want_again": True,
                      "replays": 0, "lobby": False,
                      "host_client": hr,
                      "secret_target": hr_ids[1]}
        rand_tasks = [asyncio.create_task(any_game_driver(
                          c, pid, rand_state, mem_correct=(i == 0)))
                      for i, (c, pid) in enumerate(zip(hr_clients, hr_ids))]
        # Wait until a different game has been drawn (up to 3 replays),
        # then let the current game finish and return to the lobby.
        _deadline = time.monotonic() + 420
        while time.monotonic() < _deadline and not rand_state["lobby"]:
            if len(rand_state["games"]) >= 2 \
                    and any(g != first_game
                            for g in rand_state["games"][1:]):
                rand_state["want_again"] = False
            await asyncio.sleep(0.5)
        await asyncio.wait_for(asyncio.gather(*rand_tasks), timeout=60)
        check("v2.2 replay of a random game re-rolls a new game",
              len(rand_state["games"]) >= 2
              and any(g != first_game for g in rand_state["games"][1:]),
              str(rand_state["games"]))
        check("v2.2 every replayed game is a valid random pick",
              all(g in ("reaction", "math", "trivia", "secret", "president",
                        "uno", "memory") for g in rand_state["games"]),
              str(rand_state["games"]))
        check("v2.2 random replay series ends back in the lobby",
              rand_state["lobby"] is True)
        for c in hr_clients:
            await c.close()

        # --- v2.3 item 12: end flow for EVERY game ---
        # Each game finishes -> end-of-game NEXT vote -> end_choice
        # -> replay_vote "again" (same game replays) -> finish again
        # -> replay_vote "lobby" (back to the lobby). Elimination
        # uses its own end vote instead; it is covered by the two
        # Elimination series tests above (Play Again, then Lobby).
        # Short settings keep each game fast (1 round / 1 question).
        for _ef_game, _ef_settings, _ef_bots in [
                ("reaction", {"reaction": {"rounds": 1}}, 1),
                ("math", {"math": {"difficulty": "Easy", "questions": 1,
                                   "time_per_question": 5,
                                   "vote_timeout": 3}}, 1),
                ("trivia", {"trivia": {"category": "Mixed",
                                       "difficulty": "Easy",
                                       "questions": 1,
                                       "time_per_question": 5,
                                       "vote_timeout": 3}}, 1),
                ("memory", {"memory": {"memorize_seconds": 2,
                                       "recall_seconds": 5}}, 1),
                ("uno", {"uno": {"turn_seconds": 5}}, 1),
                ("president", {"president": {"turn_seconds": 5}}, 2),
                ("secret", {"secret": {"impostors": 1}}, 2)]:
            await run_end_flow_game(_ef_game, _ef_settings, _ef_bots)

        # --- v2.2 §9: Reaction scoring edge cases ---
        # Solo game: a single player has one average, so best == worst
        # and everyone scores 1000.
        hsolo = Client("solo-host"); await hsolo.connect()
        await hsolo.send({"type": "create_room", "nickname": "SoloHost", "colour": "#e74c3c"})
        await hsolo.wait_for("joined")
        await hsolo.send({"type": "update_settings", "game": "reaction",
                          "game_settings": {"reaction": {"rounds": 2}}})
        await hsolo.wait_lobby()
        await hsolo.send({"type": "start_game"})
        await hsolo.wait_for("game_started")
        for rnd in range(1, 3):
            await hsolo.wait_for("reaction_wait", timeout=10)
            await hsolo.wait_for("reaction_go", timeout=10)
            await hsolo.send({"type": "reaction_click"})
            await hsolo.wait_for("reaction_round_result", timeout=10)
            if rnd == 1:
                await hsolo.wait_for("vote_break", timeout=10)
                await hsolo.send({"type": "vote_next", "index": 1})
        results = await hsolo.wait_for("results", timeout=10)
        check("v2.2 Reaction: a solo player (best == worst average) "
              "scores 1000",
              len(results["rankings"]) == 1
              and results["rankings"][0]["score"] == 1000,
              str(results["rankings"]))
        await drive_end_flow(hsolo, [hsolo], choice="lobby", count=1)
        await hsolo.close()
        # False starts only: a player with no valid time scores 0.
        hf = Client("fs-host"); await hf.connect()
        await hf.send({"type": "create_room", "nickname": "FSHost", "colour": "#e74c3c"})
        jf = await hf.wait_for("joined")
        rf = Client("fs-p2"); await rf.connect()
        await rf.send({"type": "join_room", "code": jf["code"], "nickname": "FSP2", "colour": "#3498db"})
        await rf.wait_for("joined")
        await hf.send({"type": "add_bot"}); await hf.wait_lobby_count(3)
        await hf.send({"type": "update_settings", "game": "reaction",
                       "game_settings": {"reaction": {"rounds": 2}}})
        await hf.wait_lobby()
        await hf.send({"type": "start_game"})
        await hf.wait_for("game_started")

        async def fs_driver(client, my_id, false_start):
            # False-start driver: click during the wait (before the
            # green light) every round, never click on go.
            while True:
                msg = await wait_any(client, ("reaction_wait", "reaction_go",
                                              "reaction_round_result",
                                              "vote_break", "results"),
                                     timeout=15)
                if msg["type"] == "reaction_wait":
                    if false_start:
                        await client.send({"type": "reaction_click"})
                elif msg["type"] == "reaction_go":
                    if not false_start:
                        await client.send({"type": "reaction_click"})
                elif msg["type"] == "vote_break":
                    await client.send({"type": "vote_next",
                                       "index": msg["index"]})
                elif msg["type"] == "results":
                    return msg

        fs_done = await asyncio.wait_for(asyncio.gather(
            fs_driver(hf, jf["player_id"], True),
            fs_driver(rf, None, False)), timeout=120)
        fs_res = fs_done[0]
        _fs_host = next(r for r in fs_res["rankings"]
                        if r["nickname"] == "FSHost")
        _fs_p2 = next(r for r in fs_res["rankings"]
                      if r["nickname"] == "FSP2")
        check("v2.2 Reaction: a player with no valid time (all false "
              "starts) scores 0 and has no average",
              _fs_host["score"] == 0 and _fs_host["avg_ms"] is None,
              str(fs_res["rankings"]))
        check("v2.2 Reaction: the clean player still scores 1000 (best)",
              _fs_p2["score"] == 1000, str(fs_res["rankings"]))
        await drive_end_flow(hf, [hf, rf], choice="lobby", count=3)
        await hf.close(); await rf.close()

        # --- v2.2 §9: team scoring styles live (sum, then best) ---
        hts = Client("tsum-host"); await hts.connect()
        await hts.send({"type": "create_room", "nickname": "TSHost", "colour": "#e74c3c"})
        jts = await hts.wait_for("joined"); codets = jts["code"]; hts_id = jts["player_id"]
        rts = Client("tsum-p2"); await rts.connect()
        await rts.send({"type": "join_room", "code": codets, "nickname": "TSP2", "colour": "#3498db"})
        jrts = await rts.wait_for("joined"); rts_id = jrts["player_id"]
        await hts.send({"type": "add_bot"})
        lobby = await hts.wait_lobby_count(3)
        _tbot = [p["id"] for p in lobby["room"]["players"]
                 if p["is_bot"]][0]
        await hts.send({"type": "update_settings", "mode": "teams", "team_count": 2,
                        "team_scoring": "sum", "game": "math",
                        "game_settings": {"math": {"difficulty": "Easy", "questions": 1,
                                                   "time_per_question": 5, "vote_timeout": 3}}})
        await hts.wait_lobby()
        # Deterministic teams: host + p2 on Team 1, the bot on Team 2.
        await hts.send({"type": "assign_team", "player_id": hts_id, "team": 0})
        await hts.wait_lobby()
        await hts.send({"type": "assign_team", "player_id": rts_id, "team": 0})
        await hts.wait_lobby()
        await hts.send({"type": "assign_team", "player_id": _tbot, "team": 1})
        await hts.wait_lobby_where(
            lambda r: r["teams"].get(_tbot) == 1)
        await hts.send({"type": "start_game"})
        started = await hts.wait_for("game_started", timeout=5)
        check("v2.2 team game starts in Teams mode with sum scoring",
              started.get("party_mode") == "teams"
              and started.get("team_scoring") == "sum", str(started))
        mq = await hts.wait_for("question", timeout=15)
        aq = solve_math(mq["text"])
        await hts.send({"type": "answer", "answer": str(aq), "index": mq["index"]})
        await rts.send({"type": "answer", "answer": str(aq + 1), "index": mq["index"]})
        await hts.wait_for("question_result", timeout=10)
        results = await hts.wait_for("results", timeout=10)
        _sums = {t["team"]: t["score"] for t in results.get("teams", [])}
        _members = {}
        for t in results.get("teams", []):
            _members[t["team"]] = [m["nickname"] for m in t["members"]]
        _ok_sum = all(
            abs(_sums[tm] - sum(
                next(r["score"] for r in results["rankings"]
                     if r["nickname"] == n) for n in _members[tm])) < 0.01
            for tm in _sums)
        check("v2.2 team scoring sum (live): team totals are member sums",
              _ok_sum and len(_sums) == 2, str(results.get("teams")))
        # Second game with "best": the team total is its best member.
        await drive_end_flow(hts, [hts, rts], choice="lobby", count=3)
        await hts.send({"type": "update_settings", "team_scoring": "best"})
        await hts.wait_lobby_where(
            lambda r: r.get("team_scoring") == "best")
        await hts.send({"type": "start_game"})
        await hts.wait_for("game_started", timeout=5)
        mq = await hts.wait_for("question", timeout=15)
        aq = solve_math(mq["text"])
        await hts.send({"type": "answer", "answer": str(aq), "index": mq["index"]})
        await rts.send({"type": "answer", "answer": str(aq + 1), "index": mq["index"]})
        await hts.wait_for("question_result", timeout=10)
        results = await hts.wait_for("results", timeout=10)
        _best = {t["team"]: t["score"] for t in results.get("teams", [])}
        _members = {}
        for t in results.get("teams", []):
            _members[t["team"]] = [m["nickname"] for m in t["members"]]
        _ok_best = all(
            abs(_best[tm] - max(
                next(r["score"] for r in results["rankings"]
                     if r["nickname"] == n) for n in _members[tm])) < 0.01
            for tm in _best)
        check("v2.2 team scoring best (live): team totals are the best "
              "member's score",
              _ok_best and len(_best) == 2, str(results.get("teams")))
        await drive_end_flow(hts, [hts, rts], choice="lobby", count=3)
        await hts.close(); await rts.close()

        # --- v2.3 item 16: Elimination ON auto-switches Teams OFF ---
        # v2.2 let Elimination honour Teams mode; v2.3 forces FFA
        # when Elimination starts (server sets room.mode = "ffa" in
        # start_game, so lobby broadcasts reflect it too). The room
        # is set up in Teams mode with assigned teams, then
        # Elimination starts: every game_started must say ffa, the
        # results must be FFA with no team standings, and the room
        # must be FFA when it returns to the lobby.
        het = Client("elimteams-host"); await het.connect()
        await het.send({"type": "create_room", "nickname": "ElimTHost", "colour": "#e74c3c"})
        jet = await het.wait_for("joined"); codeet = jet["code"]; het_id = jet["player_id"]
        ret = Client("elimteams-p2"); await ret.connect()
        await ret.send({"type": "join_room", "code": codeet, "nickname": "ElimTP2", "colour": "#3498db"})
        await ret.wait_for("joined")
        await het.send({"type": "add_bot"}); await het.wait_lobby_count(3)
        await het.send({"type": "add_bot"}); await het.wait_lobby_count(4)
        await het.send({"type": "update_settings", "mode": "teams", "team_count": 2,
                        "game": "elimination",
                        "game_settings": {"elimination": {"vote_timeout": 3}}})
        await het.wait_lobby()
        await het.send({"type": "randomize_teams"})
        await het.wait_lobby()
        await het.send({"type": "start_game"})
        et_state = {"results": None, "party_modes": set(), "lobby": False}

        async def elim_teams_driver(client, my_id):
            number = None
            while True:
                msg = await wait_any(client, (
                    "game_started", "question", "reaction_wait", "reaction_go",
                    "memory_show", "memory_recall",
                    "elimination_challenge", "elimination_eliminated",
                    "vote_break", "results", "elimination_end_vote",
                    "lobby"), timeout=120)
                t = msg["type"]
                if t == "game_started":
                    et_state["party_modes"].add(msg.get("party_mode"))
                elif t == "vote_break":
                    await client.send({"type": "vote_next",
                                       "index": msg["index"]})
                elif t == "question":
                    if msg.get("options"):
                        await client.send({"type": "answer", "answer": 0,
                                           "index": msg["index"]})
                    else:
                        ans = solve_math(msg["text"])
                        await client.send({"type": "answer", "answer": str(ans),
                                           "index": msg["index"]})
                elif t == "reaction_go":
                    await client.send({"type": "reaction_click"})
                elif t == "memory_show":
                    number = msg["number"]
                elif t == "memory_recall":
                    await client.send({"type": "memory_answer",
                                       "answer": number or "0000"})
                elif t == "results":
                    et_state["results"] = msg
                elif t == "elimination_end_vote":
                    await client.send({"type": "elimination_vote",
                                       "choice": "lobby"})
                elif t == "lobby":
                    if msg["room"]["phase"] == "lobby" and et_state["results"]:
                        et_state["lobby"] = True
                        et_state["lobby_room"] = msg["room"]
                        return

        et1 = asyncio.create_task(elim_teams_driver(het, het_id))
        et2 = asyncio.create_task(elim_teams_driver(ret, None))
        await asyncio.wait_for(asyncio.gather(et1, et2), timeout=420)
        _etr = et_state["results"]
        check("v2.3 item 16: starting Elimination while Teams is on "
              "forces FFA (every game_started says ffa)",
              et_state["party_modes"] == {"ffa"},
              str(et_state["party_modes"]))
        check("v2.3 item 16: Elimination results are FFA with no "
              "team standings",
              _etr is not None and _etr.get("mode") == "ffa"
              and _etr.get("teams") == [],
              str((_etr.get("mode"), _etr.get("teams"))
                  if _etr else None))
        check("v2.3 item 16: the room is FFA when the series ends "
              "back in the lobby",
              et_state["lobby"] is True
              and et_state.get("lobby_room", {}).get("mode") == "ffa",
              str(et_state.get("lobby_room", {}).get("mode")))
        check("v2.3 item 16: the FFA-forced series still runs to "
              "one winner (no finale)",
              _etr is not None
              and len(_etr.get("finishing_order", [])) == 4
              and _etr["finishing_order"][0]["player_id"] == _etr.get("victor")
              and _etr["finishing_order"][0]["score"] == 1000,
              str(_etr.get("finishing_order") if _etr else None))
        await het.close(); await ret.close()

        # --- v2.2 §10: Championship mode ---
        hc = Client("champ-host"); await hc.connect()
        await hc.send({"type": "create_room", "nickname": "ChampHost", "colour": "#e74c3c"})
        jc = await hc.wait_for("joined"); codec = jc["code"]; hc_id = jc["player_id"]
        rc = Client("champ-p2"); await rc.connect()
        await rc.send({"type": "join_room", "code": codec, "nickname": "ChampP2", "colour": "#3498db"})
        jrc = await rc.wait_for("joined"); rc_id = jrc["player_id"]
        await hc.send({"type": "add_bot"})
        lobby = await hc.wait_lobby_count(3)
        _cbot = [p["id"] for p in lobby["room"]["players"]
                 if p["is_bot"]][0]
        await hc.send({"type": "update_settings", "championship": True})
        lb = await hc.wait_lobby_where(lambda r: r["championship"] is True)
        check("v2.2 Championship: lobby shows the toggle on, no games yet",
              lb["room"]["champ_games"] == 0
              and lb["room"]["champ_history"] == [])

        async def champ_math(host, p2):
            """One 1-question Math game + end flow. Returns
            (end_choice, host_points_this_game) — the host's actual
            points (990-1000 by speed), used as the expected delta."""
            await host.send({"type": "update_settings", "game": "math",
                             "game_settings": {"math": {"difficulty": "Easy", "questions": 1,
                                                        "time_per_question": 5, "vote_timeout": 3}}})
            await host.wait_lobby_where(lambda r: r["phase"] == "lobby")
            await host.send({"type": "start_game"})
            await host.wait_for("game_started", timeout=5)
            mq = await host.wait_for("question", timeout=15)
            aq = solve_math(mq["text"])
            await host.send({"type": "answer", "answer": str(aq), "index": mq["index"]})
            await p2.send({"type": "answer", "answer": str(aq + 1), "index": mq["index"]})
            res = await host.wait_for("question_result", timeout=10)
            _pts = next((p["points"] for p in res["points"]
                         if p["player_id"] == hc_id), 0)
            await host.wait_for("results", timeout=10)
            end, lobby = await drive_end_flow(host, [host, p2],
                                              choice="lobby", count=3)
            return end, _pts, lobby

        def _row(hist, pid, team):
            return next((r for r in hist
                         if r["player_id"] == pid and r["team"] == team), None)

        # Games 1-2 (FFA): scores persist, counter increments, one row
        # per player keyed (player, None).
        end1, d1, lb = await champ_math(hc, rc)
        check("v2.2 Championship: game 1 recorded (counter 1)",
              end1["champ_games"] == 1, str(end1["champ_games"]))
        _h1 = _row(end1["champ_history"], hc_id, None)
        _p1 = _row(end1["champ_history"], rc_id, None)
        check("v2.2 Championship: game 1 rows hold the per-game deltas",
              _h1 is not None and _h1["per_game"] == [d1]
              and _h1["tally"] == d1
              and _p1 is not None and _p1["per_game"] == [0]
              and _p1["tally"] == 0,
              str(end1["champ_history"]))
        # (the lobby came back with champ_math's end flow — do NOT wait
        # for a second one; the broadcast was already consumed.)
        _scores = {p["nickname"]: p["score"]
                   for p in lb["room"]["players"]}
        check("v2.2 Championship: scores persist in the lobby after "
              "game 1 (not reset)",
              _scores.get("ChampHost") == d1
              and _scores.get("ChampP2") == 0, str(_scores))
        end2, d2, _ = await champ_math(hc, rc)
        check("v2.2 Championship: game 2 recorded (counter 2, deltas "
              "accumulate)",
              end2["champ_games"] == 2
              and _row(end2["champ_history"], hc_id, None)["per_game"]
              == [d1, d2]
              and _row(end2["champ_history"], hc_id, None)["tally"]
              == d1 + d2,
              str(end2["champ_history"]))
        # Switch to Teams (host + p2 on Team 1, bot on Team 2). The
        # switch alone creates NO new history rows.
        await hc.send({"type": "update_settings", "mode": "teams", "team_count": 2})
        await hc.wait_lobby()
        await hc.send({"type": "assign_team", "player_id": hc_id, "team": 0})
        await hc.wait_lobby()
        await hc.send({"type": "assign_team", "player_id": rc_id, "team": 0})
        await hc.wait_lobby()
        await hc.send({"type": "assign_team", "player_id": _cbot, "team": 1})
        lb = await hc.wait_lobby_where(lambda r: r["teams"].get(_cbot) == 1)
        check("v2.2 Championship: switching teams creates no rows until "
              "a game is played with that team",
              len(lb["room"]["champ_history"]) == 3
              and all(r["team"] is None for r in lb["room"]["champ_history"]),
              str(lb["room"]["champ_history"]))
        # Game 3 (teams): new rows for (host, 0), (p2, 0), (bot, 1),
        # padded with 0s for the two FFA games.
        end3, d3, _ = await champ_math(hc, rc)
        _h3 = _row(end3["champ_history"], hc_id, 0)
        _b3 = _row(end3["champ_history"], _cbot, 1)
        check("v2.2 Championship: game 3 (teams) creates padded rows",
              end3["champ_games"] == 3
              and _h3 is not None and _h3["per_game"] == [0, 0, d3]
              and _b3 is not None and len(_b3["per_game"]) == 3
              and _b3["per_game"][:2] == [0, 0],
              str(end3["champ_history"]))
        # Game 4: host switches to Team 2 (with the bot). A new row
        # (host, 1) is created, padded; the old (host, 0) row is kept.
        await hc.send({"type": "assign_team", "player_id": hc_id, "team": 1})
        await hc.wait_lobby_where(lambda r: r["teams"].get(hc_id) == 1)
        end4, d4, _ = await champ_math(hc, rc)
        check("v2.2 Championship: switching teams mid-championship "
              "creates a new padded row for the new team",
              end4["champ_games"] == 4
              and _row(end4["champ_history"], hc_id, 1) is not None
              and _row(end4["champ_history"], hc_id, 1)["per_game"]
              == [0, 0, 0, d4],
              str(end4["champ_history"]))
        # Game 5: host switches BACK to Team 1 — the old (host, 0) row
        # is REUSED (not a second row for the same team).
        await hc.send({"type": "assign_team", "player_id": hc_id, "team": 0})
        await hc.wait_lobby_where(lambda r: r["teams"].get(hc_id) == 0)
        end5, d5, _ = await champ_math(hc, rc)
        _h0_rows = [r for r in end5["champ_history"]
                    if r["player_id"] == hc_id and r["team"] == 0]
        check("v2.2 Championship: switching back reuses the old row",
              len(_h0_rows) == 1
              and _h0_rows[0]["per_game"] == [0, 0, d3, 0, d5]
              and _h0_rows[0]["tally"] == d3 + d5,
              str(end5["champ_history"]))
        check("v2.2 Championship: counter reached 5 games",
              end5["champ_games"] == 5, str(end5["champ_games"]))
        # Turning championship OFF ends it: history + counter cleared.
        await hc.send({"type": "update_settings", "championship": False})
        lb = await hc.wait_lobby_where(
            lambda r: r["championship"] is False)
        check("v2.2 Championship: turning it OFF clears the history "
              "and the counter",
              lb["room"]["champ_games"] == 0
              and lb["room"]["champ_history"] == [],
              str(lb["room"]))
        await hc.close(); await rc.close()

        # ================= v2.3 item 14: Group Elimination =================
        # --- internal: deterministic group pick / balance / rebalance ---
        def _mkgrp(pid, score):
            return Player(id=pid, nickname=f"P{pid}", colour="#ffffff",
                          score=score)

        _gp = [_mkgrp("a", 100), _mkgrp("b", 200), _mkgrp("c", 300),
               _mkgrp("d", 400)]
        check("v2.3 item 14 (internal): worst team is the lowest "
              "TOTAL points (300 vs 700)",
              _elim_pick_worst_team({0: [100, 200], 1: [300, 400]}) == 0)
        check("v2.3 item 14 (internal): tied totals fall back to the "
              "lowest single member score",
              _elim_pick_worst_team({0: [100, 300], 1: [200, 200]}) == 0)
        check("v2.3 item 14 (internal): an empty group is skipped "
              "when picking the worst team",
              _elim_pick_worst_team({0: [], 1: [50]}) == 1)
        _gmap = {"a": 0, "b": 0, "c": 1, "d": 1}
        check("v2.3 item 14 (internal): worst player is the lowest "
              "score inside the worst team",
              _elim_pick_worst_player(_gp, group=0, groups=_gmap).id == "a"
              and _elim_pick_worst_player(_gp, group=1, groups=_gmap).id == "c")
        check("v2.3 item 14 (internal): worst player overall with no "
              "group filter",
              _elim_pick_worst_player(_gp).id == "a")
        _uroom = Room(code="UNIT")
        _uroom.players = list(_gp) + [_mkgrp("e", 500), _mkgrp("f", 600)]
        _uroom.elim_groups = {"a": 0, "b": 0, "c": 0,
                              "d": 1, "e": 1, "f": 1}
        check("v2.3 item 14 (internal): a host-set 3v3 split is kept "
              "as-is at series start",
              _elim_balance_groups(_uroom, _uroom.players)
              == {"a": 0, "b": 0, "c": 0, "d": 1, "e": 1, "f": 1})
        _uroom.elim_groups = {"a": 0, "b": 0, "c": 1}
        _bal = _elim_balance_groups(_uroom, _uroom.players)
        check("v2.3 item 14 (internal): unassigned players fill the "
              "smaller group (3v3) and host-set groups are kept",
              _bal["a"] == 0 and _bal["b"] == 0 and _bal["c"] == 1
              and sorted(_bal.values()) == [0, 0, 0, 1, 1, 1],
              str(_bal))
        _uroom.elim_groups = {p.id: 0 for p in _uroom.players}
        _bal = _elim_balance_groups(_uroom, _uroom.players)
        check("v2.3 item 14 (internal): everyone in one group is "
              "split evenly instead (3v3)",
              sorted(_bal.values()) == [0, 0, 0, 1, 1, 1], str(_bal))
        _uroom.elim_groups = {"a": 2, "b": 0, "c": 1, "d": 0, "e": 1}
        _bal = _elim_balance_groups(_uroom, _uroom.players)
        check("v2.3 item 14 (internal): a stored Black (2) group is "
              "treated as unassigned — no 2 in the result",
              2 not in _bal.values()
              and sorted(_bal.values()) == [0, 0, 0, 1, 1, 1],
              str(_bal))
        _rroom = Room(code="UNITR")
        _rroom.players = list(_gp) + [_mkgrp("e", 500)]
        _rgroups = {"a": 0, "b": 0, "c": 0, "d": 0, "e": 1}
        _trs = _elim_rebalance_groups(_rroom, _rroom.players, _rgroups)
        check("v2.3 item 14 (internal): a 4v1 rebalance moves one "
              "player from the bigger group to the smaller (3v2)",
              len(_trs) == 1 and _trs[0]["from"] == 0
              and _trs[0]["to"] == 1 and _trs[0]["player_id"] in "abcd"
              and sorted(_rgroups.values()) == [0, 0, 0, 1, 1],
              str((_trs, _rgroups)))
        _rroom.players = list(_gp) + [_mkgrp("e", 500), _mkgrp("f", 600)]
        _rgroups = {"a": 0, "b": 0, "c": 0, "d": 0, "e": 0, "f": 1}
        _trs = _elim_rebalance_groups(_rroom, _rroom.players, _rgroups)
        check("v2.3 item 14 (internal): a 5v1 rebalance moves until "
              "the gap is <= 1 (two moves, 3v3)",
              len(_trs) == 2
              and all(t["from"] == 0 and t["to"] == 1 for t in _trs)
              and sorted(_rgroups.values()) == [0, 0, 0, 1, 1, 1],
              str((_trs, _rgroups)))
        _rgroups = {"a": 0, "b": 0, "c": 0, "d": 1, "e": 1, "f": 1}
        check("v2.3 item 14 (internal): already-balanced groups need "
              "no transfers",
              _elim_rebalance_groups(_rroom, _rroom.players, _rgroups) == []
              and sorted(_rgroups.values()) == [0, 0, 0, 1, 1, 1])
        # --- internal: Reaction interpolation (v2.2 §9) — the formula
        # Group mode relies on to pick the worst team / worst player
        # after a Reaction challenge (v2.3 item 14) ---
        def _mkreact(pid, times):
            p = Player(id=pid, nickname=f"P{pid}", colour="#ffffff")
            p.reaction_times = list(times)
            return p

        _rp = [_mkreact("a", [300, 300, 300]),
               _mkreact("b", [350, 350, 350]),
               _mkreact("c", [400, 400, 400]),
               _mkreact("d", [500, 500, 500])]
        check("v2.3 item 14 (internal): Reaction points interpolate "
              "by average time — 300/350/400/500 ms -> 1000/750/500/0",
              _reaction_points(_rp)
              == {"a": 1000, "b": 750, "c": 500, "d": 0},
              str(_reaction_points(_rp)))
        check("v2.3 item 14 (internal): all-equal Reaction averages "
              "give everyone 1000",
              _reaction_points([_mkreact("a", [300, 300]),
                                _mkreact("b", [300, 300])])
              == {"a": 1000, "b": 1000})
        check("v2.3 item 14 (internal): a player with no valid "
              "Reaction time scores 0 while the others interpolate",
              _reaction_points([_mkreact("a", [300, 300]),
                                _mkreact("b", []),
                                _mkreact("c", [500, 500])])
              == {"a": 1000, "b": 0, "c": 0})
        check("v2.3 item 14 (internal): no valid Reaction times at "
              "all gives no points",
              _reaction_points([_mkreact("a", []), _mkreact("b", [])])
              == {})
        # --- internal (async): a Reaction CHALLENGE assigns the
        # interpolated points to p.score (v2.3 item 14 fix) — before
        # the fix the challenge path returned the ranking without
        # scoring, so every Group total was 0 and the pick was a
        # random tie-break. All bots, so the per-round vote breaks
        # end immediately and no websocket is needed.
        _rroom2 = Room(code="UNITRC")
        _rroom2.phase = "playing"
        _rbots = [Player(id=f"r{i}", nickname=f"RB{i}",
                         colour="#ffffff", is_bot=True,
                         difficulty="Hard") for i in range(4)]
        _rroom2.players = list(_rbots)
        _rrank = await run_reaction_game(
            _rroom2, participants=list(_rbots), challenge=True)
        check("v2.3 item 14 (internal): a Reaction challenge returns "
              "a full ranking (best average first)",
              _rrank is not None and len(_rrank) == 4,
              str(_rrank))
        _ravgs = {p.id: (round(sum(p.reaction_times)
                               / len(p.reaction_times))
                         if p.reaction_times else None)
                  for p in _rbots}
        _rvalid = [a for a in _ravgs.values() if a is not None]
        check("v2.3 item 14 (internal): every bot recorded valid "
              "reaction times in the challenge (Hard never "
              "false-starts)",
              len(_rvalid) == 4, str(_ravgs))
        _rbest, _rworst = min(_rvalid), max(_rvalid)
        _rexp = {}
        for p in _rbots:
            _a = _ravgs[p.id]
            _rexp[p.id] = 1000 if _rbest == _rworst else round(
                1000 * (_rworst - _a) / (_rworst - _rbest))
        check("v2.3 item 14 (internal): a Reaction challenge assigns "
              "the interpolated points to p.score (not all 0)",
              all(p.score == _rexp[p.id] for p in _rbots)
              and sum(p.score for p in _rbots) > 0
              and max(p.score for p in _rbots) == 1000,
              str({p.nickname: (p.score, _ravgs[p.id])
                   for p in _rbots}))
        _rorder = [r["player_id"] for r in _rrank]
        _rscores = [next(p.score for p in _rbots if p.id == pid)
                    for pid in _rorder]
        check("v2.3 item 14 (internal): the challenge ranking order "
              "matches the assigned scores (best average first)",
              _rscores == sorted(_rscores, reverse=True),
              str(list(zip(_rorder, _rscores))))
        # Lobby payload: president_roles is present even when
        # President is NOT the selected game, as long as a finish
        # order is stored ({} when none is stored).
        _proom = Room(code="UNITP", game="math")
        _proom.players = [_mkgrp("a", 0), _mkgrp("b", 0),
                          _mkgrp("c", 0), _mkgrp("d", 0)]
        check("v2.3 item 14 (internal): lobby payload carries "
              "president_roles {} with no finish order stored",
              _proom.public()["president_roles"] == {})
        _proom.president_finish_order = ["b", "a", "d", "c"]
        check("v2.3 item 14 (internal): lobby payload carries the "
              "stored President roles even when Math is selected",
              _proom.public()["president_roles"]
              == {"b": "President", "a": "Citizen",
                  "d": "Citizen", "c": "Peasant"},
              str(_proom.public()["president_roles"]))
        check("v2.3 item 14 (internal): president_roles_for itself "
              "is still gated on President being selected",
              president_roles_for(_proom) == {})

        # --- lobby: Group availability + assignment via update_settings ---
        gh = Client("grp-host"); await gh.connect()
        await gh.send({"type": "create_room", "nickname": "GrpHost",
                       "colour": "#e74c3c"})
        gj = await gh.wait_for("joined"); gcode = gj["code"]
        gh_id = gj["player_id"]
        gp2 = Client("grp-p2"); await gp2.connect()
        await gp2.send({"type": "join_room", "code": gcode,
                        "nickname": "GrpP2", "colour": "#3498db"})
        gj2 = await gp2.wait_for("joined"); gp2_id = gj2["player_id"]
        globby = None
        for _count in (3, 4):
            await gh.send({"type": "add_bot"})
            globby = await gh.wait_lobby_count(_count)
        gplayers = list(globby["room"]["players"])
        gbots = [p for p in gplayers if p["is_bot"]]
        check("v2.3 item 14 (lobby): default Elimination mode is "
              "Normal when no mode is set",
              globby["room"].get("elimination_mode", "normal") == "normal")
        # Pick Elimination with no mode set: the lobby still says normal.
        await gh.send({"type": "update_settings", "game": "elimination"})
        globby = await gh.wait_lobby_where(
            lambda r: r.get("game") == "elimination")
        check("v2.3 item 14 (lobby): Elimination picked with no mode "
              "defaults to Normal, with the group fields present",
              globby["room"]["elimination_mode"] == "normal"
              and globby["room"]["elim_groups"] == {}
              and globby["room"]["elim_phase"] == "groups"
              and "president_roles" in globby["room"],
              str({k: globby["room"].get(k) for k in
                   ("elimination_mode", "elim_groups", "elim_phase",
                    "president_roles")}))
        # Non-host cannot change settings at all.
        await gp2.send({"type": "update_settings",
                        "elimination_mode": "group",
                        "elim_groups": {gh_id: 0, gp2_id: 1}})
        err = await gp2.wait_for("error")
        check("v2.3 item 14 (lobby): non-host update_settings is "
              "rejected",
              "Only the host" in err["message"], str(err))
        # A rejected change sends an error but NO lobby broadcast,
        # so check the untouched state over HTTP instead of waiting
        # for a broadcast that never comes.
        async with httpx.AsyncClient(trust_env=False) as http:
            _room = (await http.get(
                f"{BASE}/api/rooms/{gcode}")).json()["room"]
        check("v2.3 item 14 (lobby): the rejected non-host change "
              "left the mode and groups untouched",
              _room["elimination_mode"] == "normal"
              and _room["elim_groups"] == {}, str(_room))
        # Below 6 players Group mode is rejected and the mode stays Normal.
        await gh.send({"type": "update_settings",
                       "elimination_mode": "group"})
        err = await gh.wait_for("error")
        check("v2.3 item 14 (lobby): Group mode below 6 players is "
              "rejected with an error",
              "at least 6" in err["message"], str(err))
        async with httpx.AsyncClient(trust_env=False) as http:
            _room = (await http.get(
                f"{BASE}/api/rooms/{gcode}")).json()["room"]
        check("v2.3 item 14 (lobby): the rejected Group mode left "
              "the mode Normal",
              _room["elimination_mode"] == "normal", str(_room))
        # At 6 players Group mode opens up.
        for _count in (5, 6):
            await gh.send({"type": "add_bot"})
            globby = await gh.wait_lobby_count(_count)
        gplayers = list(globby["room"]["players"])
        gbots = [p for p in gplayers if p["is_bot"]]
        await gh.send({"type": "update_settings",
                       "elimination_mode": "group"})
        globby = await gh.wait_lobby_where(
            lambda r: r.get("elimination_mode") == "group")
        check("v2.3 item 14 (lobby): Group mode is accepted at 6 "
              "players, phase starts in groups",
              globby["room"]["elimination_mode"] == "group"
              and globby["room"]["elim_phase"] == "groups",
              str({k: globby["room"].get(k) for k in
                   ("elimination_mode", "elim_phase")}))
        # Full group map accepted (host-set Red/Blue split).
        gmap = {gh_id: 0, gp2_id: 0, gbots[0]["id"]: 0,
                gbots[1]["id"]: 1, gbots[2]["id"]: 1, gbots[3]["id"]: 1}
        await gh.send({"type": "update_settings", "elim_groups": gmap})
        globby = await gh.wait_lobby_where(
            lambda r: r.get("elim_groups") == gmap)
        check("v2.3 item 14 (lobby): a full host-set group map is "
              "accepted",
              globby["room"]["elim_groups"] == gmap,
              str(globby["room"]["elim_groups"]))
        # Values other than 0/1 and unknown player ids are ignored.
        gbad = {gh_id: 2, gp2_id: 0, gbots[0]["id"]: 7,
                gbots[1]["id"]: "1", gbots[2]["id"]: True,
                gbots[3]["id"]: 1, "ghost-id": 0}
        await gh.send({"type": "update_settings", "elim_groups": gbad})
        globby = await gh.wait_lobby_where(
            lambda r: r.get("elim_groups") == {gp2_id: 0,
                                               gbots[3]["id"]: 1})
        check("v2.3 item 14 (lobby): group values other than 0/1 "
              "(2, 7, '1', True) and unknown ids are ignored",
              globby["room"]["elim_groups"]
              == {gp2_id: 0, gbots[3]["id"]: 1},
              str(globby["room"]["elim_groups"]))
        await gh.close(); await gp2.close()

        # --- live: full 6-player Group series, host-set 3v3 split ---
        def _groups_a(players):
            host_id = next(p["id"] for p in players if p["is_host"])
            p2_id = next(p["id"] for p in players
                         if not p["is_host"] and not p["is_bot"])
            bots = [p["id"] for p in players if p["is_bot"]]
            return {host_id: 0, bots[0]: 0, bots[1]: 0,
                    p2_id: 1, bots[2]: 1, bots[3]: 1}

        gstate_a, gplayers_a = await run_group_series("GA", 4, _groups_a)
        gverified_a, gskipped_a = verify_group_series(
            "GA", gstate_a, gplayers_a, expect_full=True)
        # The series ends back in the lobby, still in Group mode,
        # now in the Black phase, with the merged groups stored
        # (five 2s + the first elimination's old 0/1) and no
        # President roles (none was played).
        _last = gstate_a.get("last_lobby") or {}
        _vals = sorted((_last.get("elim_groups") or {}).values())
        check("v2.3 item 14 (GA): the lobby after the series still "
              "carries the group fields (mode group, phase black, "
              "merged groups, empty president_roles)",
              _last.get("elimination_mode") == "group"
              and _last.get("elim_phase") == "black"
              and _vals in ([0, 2, 2, 2, 2, 2], [1, 2, 2, 2, 2, 2])
              and _last.get("president_roles") == {},
              str({k: _last.get(k) for k in
                   ("elimination_mode", "elim_phase", "elim_groups",
                    "president_roles")}))

        # --- live: 7-player 6-vs-1 series — the first break MUST
        # rebalance with transfers (any first elimination leaves a
        # 6v0 or 5v1 split, gap >= 4), then the merge at 5. Played
        # only until the Black phase starts.
        def _groups_b(players):
            last = players[-1]["id"]
            return {p["id"]: (1 if p["id"] == last else 0)
                    for p in players}

        gstate_b, gplayers_b = await run_group_series(
            "GB", 5, _groups_b, until_merge=True)
        gverified_b, gskipped_b = verify_group_series(
            "GB", gstate_b, gplayers_b, expect_full=False)
        check("v2.3 item 14 (GB): the 6-vs-1 first break rebalanced "
              "with transfers (gap back to <= 1)",
              len(gstate_b["breaks"]) >= 1
              and len(gstate_b["breaks"][0]["transfers"]) >= 2
              and gstate_b["breaks"][0]["merged_to_black"] is False,
              str(gstate_b["breaks"][0] if gstate_b["breaks"] else None))
        check("v2.3 item 14 (GB): the second break merged the final "
              "5 into Black and the third challenge is Black phase",
              len(gstate_b["breaks"]) >= 2
              and gstate_b["breaks"][1]["merged_to_black"] is True
              and len(gstate_b["challenges"]) >= 3
              and gstate_b["challenges"][2]["elim_phase"] == "black",
              f"breaks={gstate_b['breaks']} "
              f"challenges={gstate_b['challenges']}")
        # Any Reaction challenge across GA/GB must carry real group
        # totals (not 0/0) — the Reaction challenge scores now
        # (v2.3 item 14 fix). Vacuous when no Reaction challenge
        # happened to come up; the in-process check above pins the
        # wiring deterministically either way.
        _reaction_breaks = [
            b for st in (gstate_a, gstate_b) for b in st["breaks"]
            if st["challenges"][b["n"] - 1]["game"] == "reaction"]
        check("v2.3 item 14 (GA+GB): every recorded Reaction break "
              "carries real group totals (not 0/0)",
              all(sum(int(v) for v in b["group_totals"].values()) > 0
                  for b in _reaction_breaks),
              f"reaction breaks={len(_reaction_breaks)} "
              f"totals={[b['group_totals'] for b in _reaction_breaks]}")
        check("v2.3 item 14 (GA+GB): at least one challenge across "
              "both series had a uniquely determined victim that "
              "matched the server's pick",
              gverified_a + gverified_b >= 1,
              f"GA verified={gverified_a} skipped={gskipped_a}; "
              f"GB verified={gverified_b} skipped={gskipped_b}")

        await host.close(); await p2b.close(); await p2.close()
        print(f"\nALL CHECKS PASSED: {sum(PASS)}/{len(PASS)}")
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()


def test_hub():
    """pytest entry point: run the whole end-to-end suite."""
    asyncio.run(main())


if __name__ == "__main__":
    asyncio.run(main())
