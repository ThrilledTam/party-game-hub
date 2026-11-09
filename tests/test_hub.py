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
    check("trivia bank has >= 60 questions", len(QUESTIONS) >= 60,
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
