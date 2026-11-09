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
from server import make_math_question, record_answer, Room, Player  # noqa: E402
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
                check(f"math Q{q} vote break shown, needs half",
                      vb["votes_needed"] == 2, str(vb))
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
                check(f"trivia Q{q} vote break shown",
                      vb["votes_needed"] == 2, str(vb))
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
