"""Automated end-to-end test for Party Game Hub v1.

Starts the real server on port 8001, then over real WebSockets:
  - creates a room, joins a 2nd human, adds 2 bots via the host
  - verifies the lobby list and bot editing
  - starts and plays a full game of Reaction Test (3 rounds),
    Speed Math (5 questions) and Speed Trivia (5 questions)
  - checks scores / results are produced for each game

Run:  .venv/bin/python tests/test_hub.py
Exit code 0 = all checks passed.
"""
import asyncio
import json
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
from server import make_math_question, record_answer, Room, Player, team_standings  # noqa: E402
from server import (GAMES, ELIM_POOL, SECRET_WORDS, VAGUE_CLUES, secret_tally,  # noqa: E402
                    build_president_deck, president_legal_play,  # noqa: E402
                    president_illegal_reason,  # noqa: E402
                    president_lowest_legal, president_scores_for,  # noqa: E402
                    pres_rank,  # noqa: E402
                    build_uno_deck, uno_match, uno_bot_choice,  # noqa: E402
                    uno_best_colour, uno_value, memory_bot_correct)  # noqa: E402
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

    async def close(self):
        if self.ws:
            await self.ws.close()


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

        # --- back to lobby, Speed Math: 5 questions ---
        await host.send({"type": "play_again"})
        await host.wait_lobby_phase("lobby", 4)
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
        await host.send({"type": "play_again"})
        await host.wait_lobby_phase("lobby", 4)
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
        await host.send({"type": "play_again"})
        lobby = await host.wait_lobby_phase("lobby", 4)
        check("Play Again returns to lobby with same 4 players",
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
        await host.send({"type": "play_again"})
        lobby = await host.wait_lobby_phase("lobby", 4)
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

        # --- Fix 4: customization boxes are compact (~1/3 width) ---
        html = (ROOT / "static" / "index.html").read_text()
        check("v1.2 lobby profile box uses the compact class",
              'class="card compact"' in html)
        check("v1.2 compact CSS caps width near one third (~210px)",
              "max-width: 210px" in html and ".compact .swatch" in html)
        check("v1.2 bot edit panel uses the compact class",
              'id="botEdit" class="card compact hidden"' in html)

        # ================= v2 tests =================
        # --- Unit: Secret Word tally + word bank ---
        check("v2 secret tally: catching an impostor wins for the word team",
              secret_tally({"a": "x", "b": "x", "c": "y"}, {"x"}) == ("x", True))
        check("v2 secret tally: voting out an innocent wins for the impostors",
              secret_tally({"a": "y", "b": "y"}, {"x"}) == ("y", False))
        check("v2 secret tally: a tie eliminates nobody, impostors win",
              secret_tally({"a": "x", "b": "y"}, {"x"}) == (None, False))
        check("v2 secret tally: no votes, impostors win",
              secret_tally({}, {"x"}) == (None, False))
        check("v2 secret word bank has >= 60 words with 3+ clues each",
              len(SECRET_WORDS) >= 60
              and all(len(v) >= 3 for v in SECRET_WORDS.values()),
              f"{len(SECRET_WORDS)} words")

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
        check("v2 UNO: bot prefers a number card over a wild",
              uno_bot_choice(["R5", "Wwild"], "R9", "R") == "R5")
        check("v2 UNO: bot with no match draws",
              uno_bot_choice(["B7"], "R5", "G") is None)
        check("v2 UNO: bot wild colour is its most common colour",
              uno_best_colour(["R5", "R7", "B3"]) == "R")
        check("v2 UNO: value parsing",
              uno_value("Gdraw2") == "draw2" and uno_value("Wwild") == "wild")

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

        # --- v2: Secret Word full game (2 humans + 2 bots) ---
        hs = Client("secret-host"); await hs.connect()
        await hs.send({"type": "create_room", "nickname": "SecretHost", "colour": "#e74c3c"})
        js = await hs.wait_for("joined"); codes = js["code"]; hs_id = js["player_id"]
        rs = Client("secret-p2"); await rs.connect()
        await rs.send({"type": "join_room", "code": codes, "nickname": "SecretP2", "colour": "#3498db"})
        jr = await rs.wait_for("joined"); rs_id = jr["player_id"]
        await hs.send({"type": "add_bot"}); await hs.wait_lobby_count(3)
        await hs.send({"type": "add_bot"}); await hs.wait_lobby_count(4)
        await hs.send({"type": "update_settings", "game": "secret"})
        await hs.wait_lobby()
        await hs.send({"type": "start_game"})
        started = await hs.wait_for("game_started", timeout=5)
        check("v2 Secret Word starts", started["game"] == "secret")
        my_word = await hs.wait_for("secret_word", timeout=5)
        check("v2 Secret Word: host gets a private word or impostor notice",
              (my_word["is_impostor"] and my_word["word"] is None)
              or (not my_word["is_impostor"] and my_word["word"] in SECRET_WORDS),
              str(my_word))

        async def secret_driver(client, my_id, store):
            while True:
                msg = await wait_any(client, ("secret_clue_turn", "secret_vote_start"), timeout=45)
                if msg["type"] == "secret_vote_start":
                    target = next(p["player_id"] for p in msg["players"] if p["is_bot"])
                    await client.send({"type": "secret_vote", "player_id": target})
                    break
                if msg["player_id"] == my_id:
                    await client.send({"type": "secret_clue", "clue": "sweet"})
            store["result"] = await client.wait_for("secret_result", timeout=40)

        sstore = {}
        await asyncio.wait_for(asyncio.gather(
            secret_driver(hs, hs_id, sstore),
            secret_driver(rs, rs_id, sstore)), timeout=120)
        sres = sstore["result"]
        check("v2 Secret Word: result names the word and 1-2 impostors",
              sres.get("word") in SECRET_WORDS
              and len(sres.get("impostors", [])) in (1, 2)
              and len(sres.get("winners", [])) >= 1, str(sres))
        check("v2 Secret Word: winners are consistent with the vote",
              (sres["word_team_won"] and sres["eliminated"] in sres["impostors"])
              or (not sres["word_team_won"] and sres["eliminated"] not in sres["impostors"]),
              str(sres))
        results = await hs.wait_for("results", timeout=10)
        check("v2 Secret Word produces results for all 4 players",
              len(results["rankings"]) == 4 and results["game"] == "secret",
              str([(r["nickname"], r["score"]) for r in results["rankings"]]))
        await hs.close(); await rs.close()

        # --- v2: President full game (2 humans + 2 bots) ---
        hp = Client("pres-host"); await hp.connect()
        await hp.send({"type": "create_room", "nickname": "PresHost", "colour": "#e74c3c"})
        jp = await hp.wait_for("joined"); codep = jp["code"]; hp_id = jp["player_id"]
        rp = Client("pres-p2"); await rp.connect()
        await rp.send({"type": "join_room", "code": codep, "nickname": "PresP2", "colour": "#3498db"})
        jr = await rp.wait_for("joined"); rp_id = jr["player_id"]
        await hp.send({"type": "add_bot"}); await hp.wait_lobby_count(3)
        await hp.send({"type": "add_bot"}); await hp.wait_lobby_count(4)
        await hp.send({"type": "update_settings", "game": "president",
                       "game_settings": {"president": {"turn_seconds": 5}}})
        await hp.wait_lobby()
        await hp.send({"type": "start_game"})
        started = await hp.wait_for("game_started", timeout=5)
        check("v2 President starts", started["game"] == "president")

        async def pres_driver(client, my_id):
            # On my turn, lead with ALL cards of the lowest rank (sheds
            # fast), or play the lowest legal set; otherwise pass.
            while True:
                msg = await wait_any(client, ("president_state", "results"), timeout=60)
                if msg["type"] == "results":
                    return msg
                if msg["turn_id"] != my_id:
                    continue
                hand = msg["hand"]
                trick = msg["trick"]
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

        pres_done = await asyncio.wait_for(asyncio.gather(
            pres_driver(hp, hp_id), pres_driver(rp, rp_id)), timeout=300)
        pres_res = pres_done[0]
        check("v2 President produces results with a finish order of 4",
              pres_res["game"] == "president" and len(pres_res["rankings"]) == 4,
              str([(r["nickname"], r["score"]) for r in pres_res["rankings"]]))
        check("v2 President: winner scores 1000, last scores 0",
              pres_res["rankings"][0]["score"] == 1000
              and pres_res["rankings"][-1]["score"] == 0,
              str([(r["nickname"], r["score"]) for r in pres_res["rankings"]]))
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
        await hq.send({"type": "add_bot"}); await hq.wait_lobby_count(4)
        await hq.send({"type": "update_settings", "game": "president",
                       "game_settings": {"president": {"turn_seconds": 4}}})
        await hq.wait_lobby()
        await hq.send({"type": "start_game"})
        started = await hq.wait_for("game_started", timeout=5)
        check("v2.1 President (probe game) starts", started["game"] == "president")
        probe = {"empty": False, "size": False, "notice": ""}

        async def pres_auto_driver(client, my_id):
            # Play legally forever (used for the second human).
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
                if (not probe["size"] and trick is not None
                        and trick["size"] >= 2):
                    # Probe 2: one higher card against a multi-card
                    # trick (pair or bigger). If we have no higher
                    # single card, pass and wait for the next
                    # multi-card trick instead of spending the hand.
                    higher = [c for c in hand
                              if pres_rank(c) > trick["rank"]]
                    if higher:
                        await client.send({"type": "president_play",
                                           "cards": [higher[0]]})
                        err = await wait_any(client, ("error",), timeout=5)
                        check("v2.1 President: single vs multi-card trick "
                              "gets a specific size error",
                              ("play %d card" % trick["size"])
                              in err.get("message", "")
                              and "not 1" in err.get("message", ""),
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
        check("v2.1 President: wrong-size probe fired (single vs multi-card trick)",
              probe["size"])
        check("v2.1 President: timeout auto-play broadcasts a notice",
              "ran out of time" in probe["notice"], probe["notice"])
        check("v2.1 President: probe game still produces results",
              pres2_res[0]["game"] == "president"
              and len(pres2_res[0]["rankings"]) == 4)
        await hq.close(); await rq.close()

        # --- v2: UNO full game (2 humans + 1 bot) ---
        hu = Client("uno-host"); await hu.connect()
        await hu.send({"type": "create_room", "nickname": "UnoHost", "colour": "#e74c3c"})
        ju = await hu.wait_for("joined"); codeu = ju["code"]; hu_id = ju["player_id"]
        ru = Client("uno-p2"); await ru.connect()
        await ru.send({"type": "join_room", "code": codeu, "nickname": "UnoP2", "colour": "#3498db"})
        jr = await ru.wait_for("joined"); ru_id = jr["player_id"]
        await hu.send({"type": "add_bot"}); await hu.wait_lobby_count(3)
        await hu.send({"type": "update_settings", "game": "uno",
                       "game_settings": {"uno": {"turn_seconds": 5}}})
        await hu.wait_lobby()
        await hu.send({"type": "start_game"})
        started = await hu.wait_for("game_started", timeout=5)
        check("v2 UNO starts", started["game"] == "uno")

        async def uno_driver(client, my_id):
            while True:
                msg = await wait_any(client, ("uno_state", "results"), timeout=60)
                if msg["type"] == "results":
                    return msg
                if msg["turn_id"] != my_id:
                    continue
                if msg.get("drawn"):
                    drawn = msg["drawn"]
                    if uno_match(drawn, msg["top"], msg["current_colour"]):
                        await client.send({"type": "uno_play", "card": drawn,
                                           "colour": "R" if drawn.startswith("W") else None})
                    else:
                        await client.send({"type": "uno_end_turn"})
                    continue
                card = uno_bot_choice(msg["hand"], msg["top"], msg["current_colour"])
                if card:
                    await client.send({"type": "uno_play", "card": card,
                                       "colour": "R" if card.startswith("W") else None})
                else:
                    await client.send({"type": "uno_draw"})

        uno_done = await asyncio.wait_for(asyncio.gather(
            uno_driver(hu, hu_id), uno_driver(ru, ru_id)), timeout=300)
        uno_res = uno_done[0]
        check("v2 UNO produces results: winner emptied their hand (+1000)",
              uno_res["game"] == "uno" and uno_res["rankings"][0]["score"] == 1000
              and uno_res["rankings"][0].get("cards_left") == 0,
              str([(r["nickname"], r["score"], r.get("cards_left"))
                   for r in uno_res["rankings"]]))
        await hu.close(); await ru.close()

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
                                              "memory_round_result", "results"),
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
                                              "memory_round_result", "results"),
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
                    elim_state["results"].append(msg)
                elif t == "elimination_end_vote":
                    # First end vote -> Play Again (new series);
                    # second end vote -> Return to Lobby.
                    my_end_votes += 1
                    choice = "again" if my_end_votes == 1 else "lobby"
                    await client.send({"type": "elimination_vote",
                                       "choice": choice})
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
        check("v2 Elimination: challenges came from the quick-game pool",
              all(g in ("reaction", "math", "trivia", "memory")
                  for g, _ in elim_state["challenges"])
              and any(f for _, f in elim_state["challenges"]),
              str(elim_state["challenges"]))
        check("v2 Elimination: players were eliminated down to the finale",
              len(elim_state["eliminated"]) >= 2,
              str(elim_state["eliminated"]))
        first = elim_state["results"][0]
        check("v2 Elimination: finishing order lists every player once",
              first["game"] == "elimination"
              and len(first.get("finishing_order", [])) == 4
              and len({r["player_id"] for r in first["finishing_order"]}) == 4
              and first["finishing_order"][0]["player_id"] == first["victor"],
              str(first.get("finishing_order")))
        check("v2 Elimination: victor scores 1000, 2nd 500, 3rd 250",
              first["finishing_order"][0]["score"] == 1000
              and first["finishing_order"][1]["score"] == 500
              and first["finishing_order"][2]["score"] == 250,
              str([(r["nickname"], r["score"]) for r in first["finishing_order"]]))
        check("v2 Elimination: series ends back in the lobby",
              elim_state["lobby"])
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

        await host.close(); await p2b.close(); await p2.close()
        print(f"\nALL CHECKS PASSED: {sum(PASS)}/{len(PASS)}")
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()


if __name__ == "__main__":
    asyncio.run(main())
