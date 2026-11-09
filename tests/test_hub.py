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
    # e.g. "12 + 5 = ?" / "8 × 3 = ?" / "20 ÷ 4 = ?"
    m = re.match(r"(\d+)\s*([+\-−×÷])\s*(\d+)", text)
    a, op, b = int(m.group(1)), m.group(2), int(m.group(3))
    return {"+": a + b, "-": a - b, "−": a - b, "×": a * b, "÷": a // b}[op]


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
        results = await host.wait_for("results", timeout=10)
        check("Speed Trivia produces results",
              len(results["rankings"]) == 4,
              str([(r["nickname"], r["score"]) for r in results["rankings"]]))

        # --- back to lobby, same players still there ---
        await host.send({"type": "play_again"})
        lobby = await host.wait_lobby_phase("lobby", 4)
        check("Play Again returns to lobby with same 4 players",
              len(lobby["room"]["players"]) == 4 and lobby["room"]["phase"] == "lobby")

        await host.close(); await p2.close()
        print(f"\nALL CHECKS PASSED: {sum(PASS)}/{len(PASS)}")
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()


if __name__ == "__main__":
    asyncio.run(main())
