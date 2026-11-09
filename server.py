"""
Party Game Hub — v1
====================
A free, simple, online party-game hub for 1-8 players (humans + bots).

Stack: Python 3.12, FastAPI + WebSockets, single-page frontend in static/.
No accounts, no sign-in. All game logic lives server-side in this file so
every player in a room sees the same state at the same time.

V1 games: Reaction Test, Speed Math, Speed Trivia.
Later phases (add new functions in the GAME HANDLERS section, and a new
entry in GAMES): Secret Word / Impostor, President / Ditchdiggers, UNO.

Run locally:
    python -m venv .venv
    .venv/bin/pip install -r requirements.txt
    .venv/bin/uvicorn server:app --host 0.0.0.0 --port 8000
Then open http://localhost:8000

Protocol (JSON over one WebSocket per player, endpoint /ws):
  Client -> server:
    create_room {nickname, colour, room_name?}
    join_room   {code, nickname, colour}
    update_settings {room_name?, max_players?, game?, game_settings?}
    add_bot     {}
    update_bot  {bot_id, nickname?, colour?, difficulty?}
    remove_bot  {bot_id}
    start_game  {}
    reaction_click {}
    answer      {answer}            # math: number as string, trivia: option index
    play_again  {}                  # back to lobby, same players
  Server -> client (broadcast to the whole room unless noted):
    joined      {code, player_id, room}   # reply to this player only
    lobby       {room}                    # full room state
    error       {message}
    game_started {game, settings, mode}  # mode: "solo" | "competition"
    reaction_wait {round, total}
    reaction_go   {round, total}
    reaction_round_result {round, total, results:[{player_id,nickname,colour,is_bot,ms,false_start}]}
    question    {game, index, total, text, options, time_limit}
    question_result {index, total, correct_answer, points:[{player_id,points}], scores:[...]}
    results     {game, rankings:[{player_id,nickname,colour,is_bot,score,avg_ms,best_ms}]}
    game_cancelled {reason}

Scoring (Speed Math / Speed Trivia): first correct answer gets the most
points. Correct = 500..1000 points, scaled by speed:
    points = 1000 - int(500 * elapsed_seconds / time_limit)
Wrong answer or timeout = 0.

Reaction Test: a click before the green "CLICK NOW!" is a false start.
That round counts as a fail for that player (no time recorded, shown as
"Too soon!"). Fastest *average* of successful rounds wins.
"""

import asyncio
import json
import random
import secrets
import string
import time
import uuid
from dataclasses import dataclass, field

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from questions import QUESTIONS

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

PALETTE = [
    "#e74c3c",  # red
    "#e67e22",  # orange
    "#f1c40f",  # yellow
    "#2ecc71",  # green
    "#3498db",  # blue
    "#9b59b6",  # purple
    "#fd79a8",  # pink
    "#1abc9c",  # teal
    "#95a5a6",  # grey
    "#34495e",  # dark blue
]
BOT_NAMES = ["Bot Bob", "Bot Bella", "Bot Max", "Bot Mia", "Bot Leo",
             "Bot Zoe", "Bot Sam", "Bot Ivy"]
DIFFICULTIES = ["Easy", "Medium", "Hard"]

# Bot behaviour per difficulty.
# reaction: (min_ms, max_ms) simulated reaction time; false_start_chance.
# quiz:    (min_delay_s, max_delay_s, accuracy) for Math/Trivia answers.
BOT_REACTION = {
    "Easy":   {"ms": (380, 600), "false_start": 0.15},
    "Medium": {"ms": (250, 380), "false_start": 0.05},
    "Hard":   {"ms": (180, 280), "false_start": 0.0},
}
BOT_QUIZ = {
    "Easy":   {"delay": (3.0, 8.0), "accuracy": 0.55},
    "Medium": {"delay": (2.0, 5.0), "accuracy": 0.75},
    "Hard":   {"delay": (1.0, 3.0), "accuracy": 0.92},
}

GAMES = {
    "reaction": {"name": "Reaction Test", "min_players": 1},
    "math":     {"name": "Speed Math",    "min_players": 1},
    "trivia":   {"name": "Speed Trivia",  "min_players": 1},
}
GAME_CHOICES = list(GAMES) + ["random"]

DEFAULT_SETTINGS = {
    "reaction": {"rounds": 5},
    "math":     {"difficulty": "Medium", "questions": 10, "time_per_question": 15},
    "trivia":   {"category": "Mixed", "difficulty": "Medium",
                 "questions": 10, "time_per_question": 15},
}

# ---------------------------------------------------------------------------
# Data model (in-memory; rooms vanish when the server restarts — fine for v1)
# ---------------------------------------------------------------------------

@dataclass
class Player:
    id: str
    nickname: str
    colour: str
    is_bot: bool = False
    difficulty: str | None = None          # bots only
    is_host: bool = False
    ws: WebSocket | None = None            # humans only; None when disconnected
    connected: bool = True
    score: int = 0
    reaction_times: list = field(default_factory=list)  # ms, successful rounds only
    false_starts: int = 0

    def public(self):
        return {"id": self.id, "nickname": self.nickname, "colour": self.colour,
                "is_bot": self.is_bot, "difficulty": self.difficulty,
                "is_host": self.is_host, "connected": self.connected,
                "score": self.score}


@dataclass
class Room:
    code: str
    name: str = "Party Room"
    max_players: int = 8
    players: list = field(default_factory=list)   # Player, join order
    phase: str = "lobby"                          # lobby | playing | results
    game: str = "reaction"                        # selected game, or "random"
    game_settings: dict = field(default_factory=lambda: {
        k: dict(v) for k, v in DEFAULT_SETTINGS.items()})
    game_task: asyncio.Task | None = None
    # Per-game transient state (reset at each game start)
    current_game: str | None = None
    round_state: dict = field(default_factory=dict)

    def public(self):
        return {"code": self.code, "name": self.name,
                "max_players": self.max_players, "phase": self.phase,
                "game": self.game, "game_settings": self.game_settings,
                "current_game": self.current_game,
                "players": [p.public() for p in self.players],
                "min_players": GAMES.get(
                    self.current_game or self.game, {"min_players": 1}
                )["min_players"] if self.game != "random" else 1}

    def humans(self):
        return [p for p in self.players if not p.is_bot]

    def host(self):
        for p in self.players:
            if p.is_host:
                return p
        return None

    def get_player(self, pid):
        for p in self.players:
            if p.id == pid:
                return p
        return None


ROOMS: dict[str, Room] = {}

app = FastAPI(title="Party Game Hub v1")
app.mount("/static", StaticFiles(directory="static"), name="static")


@app.get("/")
async def home():
    return FileResponse("static/index.html")


@app.get("/api/health")
async def health():
    return {"ok": True, "rooms": len(ROOMS)}


@app.get("/api/rooms/{code}")
async def room_info(code: str):
    room = ROOMS.get(code.upper())
    if not room:
        return {"exists": False}
    return {"exists": True, "room": room.public()}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def new_code() -> str:
    alphabet = string.ascii_uppercase + "23456789"  # no 0/1/O/I confusion
    while True:
        code = "".join(secrets.choice(alphabet) for _ in range(6))
        if code not in ROOMS:
            return code


def free_colour(room: Room, preferred: str | None = None) -> str:
    """Pick a colour not already taken in the room, if possible."""
    used = {p.colour for p in room.players}
    if preferred and preferred not in used:
        return preferred
    for c in PALETTE:
        if c not in used:
            return c
    return preferred or random.choice(PALETTE)


async def send(player: Player, msg: dict):
    if player.ws is not None and not player.is_bot:
        try:
            await player.ws.send_text(json.dumps(msg))
        except Exception:
            pass


async def broadcast(room: Room, msg: dict):
    for p in room.players:
        await send(p, msg)


async def broadcast_lobby(room: Room):
    await broadcast(room, {"type": "lobby", "room": room.public()})


def rankings(room: Room, *, by_avg=False):
    """Rankings for the results screen."""
    rows = []
    for p in room.players:
        avg = None
        best = None
        if p.reaction_times:
            avg = round(sum(p.reaction_times) / len(p.reaction_times))
            best = min(p.reaction_times)
        rows.append({"player_id": p.id, "nickname": p.nickname,
                     "colour": p.colour, "is_bot": p.is_bot,
                     "score": p.score, "avg_ms": avg, "best_ms": best,
                     "false_starts": p.false_starts})
    if by_avg:
        # Lowest average first; players with no successful round go last.
        rows.sort(key=lambda r: (r["avg_ms"] is None, r["avg_ms"] or 0,
                                 -r["score"]))
    else:
        rows.sort(key=lambda r: -r["score"])
    return rows


def cancel_game_task(room: Room):
    if room.game_task and not room.game_task.done():
        room.game_task.cancel()
    room.game_task = None


# ---------------------------------------------------------------------------
# Question generation
# ---------------------------------------------------------------------------

def make_math_question(difficulty: str):
    """Return (text, correct_answer:int)."""
    if difficulty == "Easy":
        op = random.choice(["+", "-"])
        a, b = random.randint(0, 20), random.randint(0, 20)
        if op == "-" and b > a:
            a, b = b, a
    elif difficulty == "Medium":
        op = random.choice(["+", "-", "x"])
        if op == "x":
            a, b = random.randint(2, 12), random.randint(2, 12)
        else:
            a, b = random.randint(0, 50), random.randint(0, 50)
            if op == "-" and b > a:
                a, b = b, a
    else:  # Hard
        op = random.choice(["+", "-", "x", "÷"])
        if op == "x":
            a, b = random.randint(3, 15), random.randint(3, 15)
        elif op == "÷":
            b = random.randint(2, 12)
            ans = random.randint(2, 12)
            a = b * ans
            return f"{a} ÷ {b} = ?", ans
        else:
            a, b = random.randint(0, 100), random.randint(0, 100)
            if op == "-" and b > a:
                a, b = b, a
    if op == "+":
        return f"{a} + {b} = ?", a + b
    if op == "-":
        return f"{a} − {b} = ?", a - b
    return f"{a} × {b} = ?", a * b


def pick_trivia_questions(category: str, difficulty: str, count: int):
    """Sample questions, preferring the chosen difficulty, falling back to
    other difficulties in the same category so a 15-question game always
    has enough questions."""
    pool = QUESTIONS if category == "Mixed" else [
        q for q in QUESTIONS if q["category"] == category]
    preferred = [q for q in pool if q["difficulty"] == difficulty]
    rest = [q for q in pool if q["difficulty"] != difficulty]
    random.shuffle(preferred)
    random.shuffle(rest)
    chosen = (preferred + rest)[:count]
    random.shuffle(chosen)
    return chosen


# ---------------------------------------------------------------------------
# Game handlers — one coroutine per game. Add new games here in later phases.
# ---------------------------------------------------------------------------

async def run_reaction_game(room: Room):
    """Reaction Test. Server controls the wait/go timing for everyone."""
    settings = room.game_settings["reaction"]
    total = int(settings.get("rounds", 5))
    room.round_state = {"round": 0, "go_at": None, "clicks": {}}
    try:
        for rnd in range(1, total + 1):
            room.round_state = {"round": rnd, "go_at": None, "clicks": {}}
            await broadcast(room, {"type": "reaction_wait",
                                   "round": rnd, "total": total})
            # Decide each bot's plan for this round up front.
            bot_plans = {}
            for p in room.players:
                if p.is_bot:
                    cfg = BOT_REACTION[p.difficulty or "Medium"]
                    false = random.random() < cfg["false_start"]
                    bot_plans[p.id] = {
                        "false": false,
                        "ms": random.randint(*cfg["ms"]),
                    }
            delay = random.uniform(1.0, 4.0)
            # Schedule bot false starts (they click before the green light).
            async def bot_false_start(bot, when):
                await asyncio.sleep(when)
                if room.phase == "playing" and room.round_state.get("round") == rnd:
                    record_reaction(room, bot, None, false_start=True)
            for p in room.players:
                if p.is_bot and bot_plans[p.id]["false"]:
                    asyncio.create_task(bot_false_start(
                        p, delay * random.uniform(0.3, 0.9)))
            await asyncio.sleep(delay)
            if room.phase != "playing":
                return
            room.round_state["go_at"] = time.monotonic()
            await broadcast(room, {"type": "reaction_go",
                                   "round": rnd, "total": total})
            # Schedule bot (on-time) clicks.
            async def bot_click(bot, ms):
                await asyncio.sleep(ms / 1000.0)
                if room.phase == "playing" and room.round_state.get("round") == rnd:
                    record_reaction(room, bot, ms)
            for p in room.players:
                if p.is_bot and not bot_plans[p.id]["false"]:
                    asyncio.create_task(bot_click(p, bot_plans[p.id]["ms"]))
            # Wait until everyone has clicked or 5s pass.
            deadline = time.monotonic() + 5.0
            while time.monotonic() < deadline:
                clicked = {pid for pid in room.round_state["clicks"]}
                remaining = [p for p in room.players
                             if p.id not in clicked and p.connected]
                if not remaining:
                    break
                await asyncio.sleep(0.1)
            results = []
            for p in room.players:
                c = room.round_state["clicks"].get(p.id)
                results.append({"player_id": p.id, "nickname": p.nickname,
                                "colour": p.colour, "is_bot": p.is_bot,
                                "ms": c["ms"] if c else None,
                                "false_start": bool(c and c["false_start"])})
            await broadcast(room, {"type": "reaction_round_result",
                                   "round": rnd, "total": total,
                                   "results": results})
            await asyncio.sleep(1.5)
        room.phase = "results"
        await broadcast(room, {"type": "results", "game": "reaction",
                               "rankings": rankings(room, by_avg=True)})
        await broadcast_lobby(room)
    except asyncio.CancelledError:
        pass


def record_reaction(room: Room, player: Player, ms, false_start=False):
    """Record one player's click for the current reaction round."""
    clicks = room.round_state.setdefault("clicks", {})
    if player.id in clicks:
        return  # only the first click per round counts
    clicks[player.id] = {"ms": ms, "false_start": false_start}
    if false_start:
        player.false_starts += 1
    elif ms is not None:
        player.reaction_times.append(ms)


async def run_quiz_game(room: Room, game: str):
    """Shared loop for Speed Math and Speed Trivia."""
    settings = room.game_settings[game]
    total = int(settings.get("questions", 10))
    time_limit = int(settings.get("time_per_question", 15))
    # Build the question list.
    questions = []
    if game == "math":
        for _ in range(total):
            text, ans = make_math_question(settings.get("difficulty", "Medium"))
            questions.append({"text": text, "options": None, "answer": ans})
    else:
        for q in pick_trivia_questions(settings.get("category", "Mixed"),
                                       settings.get("difficulty", "Medium"),
                                       total):
            questions.append({"text": q["question"], "options": q["options"],
                              "answer": q["answer"]})
    try:
        for idx, q in enumerate(questions, start=1):
            room.round_state = {"index": idx, "answers": {},
                                "start": time.monotonic(),
                                "correct": q["answer"]}
            await broadcast(room, {"type": "question", "game": game,
                                   "index": idx, "total": total,
                                   "text": q["text"], "options": q["options"],
                                   "time_limit": time_limit})
            # Schedule bot answers.
            async def bot_answer(bot, correct, delay):
                await asyncio.sleep(delay)
                if room.phase != "playing":
                    return
                st = room.round_state
                if st.get("index") != idx or bot.id in st["answers"]:
                    return
                if game == "math":
                    if correct:
                        ans = q["answer"]
                    else:
                        ans = q["answer"] + random.choice([-3, -2, -1, 1, 2, 3, 10])
                    record_answer(room, bot, str(ans))
                else:
                    if correct:
                        ans = q["answer"]
                    else:
                        ans = random.choice([i for i in range(4)
                                             if i != q["answer"]])
                    record_answer(room, bot, ans)
            for p in room.players:
                if p.is_bot:
                    cfg = BOT_QUIZ[p.difficulty or "Medium"]
                    delay = random.uniform(*cfg["delay"])
                    delay = min(delay, time_limit - 0.5)
                    asyncio.create_task(bot_answer(
                        p, random.random() < cfg["accuracy"], max(0.5, delay)))
            # Wait for the timer, or until every connected player answered.
            deadline = time.monotonic() + time_limit
            while time.monotonic() < deadline:
                answered = set(room.round_state["answers"])
                remaining = [p for p in room.players
                             if p.id not in answered and p.connected]
                if not remaining:
                    break
                await asyncio.sleep(0.1)
            # Reveal the answer and the points awarded this question.
            correct = q["answer"]
            points = []
            for pid, a in room.round_state["answers"].items():
                if a["points"]:
                    points.append({"player_id": pid, "points": a["points"]})
            await broadcast(room, {"type": "question_result",
                                   "index": idx, "total": total,
                                   "correct_answer": correct,
                                   "points": points,
                                   "scores": [{"player_id": p.id,
                                               "nickname": p.nickname,
                                               "colour": p.colour,
                                               "is_bot": p.is_bot,
                                               "score": p.score}
                                              for p in room.players]})
            await asyncio.sleep(1.2)
        room.phase = "results"
        await broadcast(room, {"type": "results", "game": game,
                               "rankings": rankings(room)})
        await broadcast_lobby(room)
    except asyncio.CancelledError:
        pass


def record_answer(room: Room, player: Player, answer):
    """Score one quiz answer. Faster correct answers earn more points."""
    st = room.round_state
    if player.id in st.get("answers", {}):
        return  # first answer only
    correct = st["correct"]
    elapsed = time.monotonic() - st["start"]
    time_limit = 15
    # time_limit is in the broadcast question; recover from settings.
    game = room.current_game
    if game:
        time_limit = int(room.game_settings[game].get("time_per_question", 15))
    is_correct = False
    if game == "math":
        try:
            is_correct = int(str(answer).strip()) == int(correct)
        except (ValueError, TypeError):
            is_correct = False
    else:
        try:
            is_correct = int(answer) == int(correct)
        except (ValueError, TypeError):
            is_correct = False
    points = 0
    if is_correct:
        points = 1000 - int(500 * min(elapsed, time_limit) / time_limit)
        player.score += points
    st["answers"][player.id] = {"answer": answer, "points": points,
                                "elapsed_ms": round(elapsed * 1000)}


# ---------------------------------------------------------------------------
# Room actions (called from the WebSocket handler)
# ---------------------------------------------------------------------------

def min_players_for(room: Room, game: str) -> int:
    return GAMES[game]["min_players"]


async def start_game(room: Room, player: Player):
    if not player.is_host:
        await send(player, {"type": "error",
                            "message": "Only the host can start the game."})
        return
    if room.phase != "lobby":
        return
    game = room.game
    if game == "random":
        game = random.choice(list(GAMES))
    if len(room.players) < min_players_for(room, game):
        await send(player, {"type": "error", "message":
            f"{GAMES[game]['name']} needs at least "
            f"{min_players_for(room, game)} player(s). Add bots or wait for players."})
        return
    # Reset scores for a fresh game.
    for p in room.players:
        p.score = 0
        p.reaction_times = []
        p.false_starts = 0
    room.current_game = game
    room.phase = "playing"
    mode = "solo" if len(room.players) == 1 else "competition"
    await broadcast(room, {"type": "game_started", "game": game,
                           "settings": room.game_settings[game],
                           "mode": mode})
    await broadcast_lobby(room)
    if game == "reaction":
        room.game_task = asyncio.create_task(run_reaction_game(room))
    else:
        room.game_task = asyncio.create_task(run_quiz_game(room, game))


async def handle_message(room: Room, player: Player, msg: dict):
    mtype = msg.get("type")

    if mtype == "update_settings":
        if not player.is_host:
            await send(player, {"type": "error",
                                "message": "Only the host can change settings."})
            return
        if room.phase != "lobby":
            return
        if "room_name" in msg and isinstance(msg["room_name"], str):
            room.name = msg["room_name"][:40] or room.name
        if "max_players" in msg:
            try:
                mp = int(msg["max_players"])
                if 2 <= mp <= 8 and mp >= len(room.players):
                    room.max_players = mp
            except (ValueError, TypeError):
                pass
        if "game" in msg and msg["game"] in GAME_CHOICES:
            room.game = msg["game"]
        if isinstance(msg.get("game_settings"), dict):
            for game, patch in msg["game_settings"].items():
                if game in DEFAULT_SETTINGS and isinstance(patch, dict):
                    room.game_settings[game].update(patch)
        await broadcast_lobby(room)

    elif mtype == "add_bot":
        if not player.is_host:
            await send(player, {"type": "error",
                                "message": "Only the host can add bots."})
            return
        if room.phase != "lobby":
            await send(player, {"type": "error",
                                "message": "Bots can only be added in the lobby."})
            return
        if len(room.players) >= room.max_players:
            await send(player, {"type": "error",
                                "message": f"Room is full ({room.max_players} players max)."})
            return
        used_names = {p.nickname for p in room.players}
        name = next((n for n in BOT_NAMES if n not in used_names),
                    f"Bot {len(room.players) + 1}")
        bot = Player(id=uuid.uuid4().hex[:8], nickname=name,
                     colour=free_colour(room), is_bot=True,
                     difficulty="Medium")
        room.players.append(bot)
        await broadcast_lobby(room)

    elif mtype == "update_bot":
        if not player.is_host:
            await send(player, {"type": "error",
                                "message": "Only the host can edit bots."})
            return
        bot = room.get_player(msg.get("bot_id", ""))
        if not bot or not bot.is_bot:
            return
        if isinstance(msg.get("nickname"), str) and msg["nickname"].strip():
            nick = msg["nickname"].strip()[:20]
            if nick not in {p.nickname for p in room.players if p is not bot}:
                bot.nickname = nick
        if isinstance(msg.get("colour"), str):
            bot.colour = msg["colour"]
        if msg.get("difficulty") in DIFFICULTIES:
            bot.difficulty = msg["difficulty"]
        await broadcast_lobby(room)

    elif mtype == "remove_bot":
        if not player.is_host:
            return
        bot = room.get_player(msg.get("bot_id", ""))
        if bot and bot.is_bot and room.phase == "lobby":
            room.players.remove(bot)
            await broadcast_lobby(room)

    elif mtype == "start_game":
        await start_game(room, player)

    elif mtype == "reaction_click":
        if room.phase != "playing" or room.current_game != "reaction":
            return
        st = room.round_state
        if st.get("go_at") is None:
            record_reaction(room, player, None, false_start=True)
        else:
            ms = round((time.monotonic() - st["go_at"]) * 1000)
            record_reaction(room, player, ms)

    elif mtype == "answer":
        if room.phase != "playing" or room.current_game not in ("math", "trivia"):
            return
        record_answer(room, player, msg.get("answer"))

    elif mtype == "play_again":
        # Back to the lobby with the same players; drop disconnected humans.
        cancel_game_task(room)
        room.players = [p for p in room.players if p.connected or p.is_bot]
        room.phase = "lobby"
        room.current_game = None
        for p in room.players:
            p.score = 0
            p.reaction_times = []
            p.false_starts = 0
        await broadcast_lobby(room)


# ---------------------------------------------------------------------------
# WebSocket entry point
# ---------------------------------------------------------------------------

@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    await ws.accept()
    room = None
    player = None
    try:
        # First message must be create_room or join_room.
        first = json.loads(await ws.receive_text())
        nickname = str(first.get("nickname", "")).strip()[:20]
        colour = first.get("colour") or random.choice(PALETTE)
        if not nickname:
            await ws.send_text(json.dumps(
                {"type": "error", "message": "Nickname is required."}))
            await ws.close()
            return
        if first.get("type") == "create_room":
            code = new_code()
            room = Room(code=code,
                        name=str(first.get("room_name") or "Party Room")[:40])
            ROOMS[code] = room
            player = Player(id=uuid.uuid4().hex[:8], nickname=nickname,
                            colour=colour, is_host=True, ws=ws)
            room.players.append(player)
        elif first.get("type") == "join_room":
            code = str(first.get("code", "")).strip().upper()
            room = ROOMS.get(code)
            if not room:
                await ws.send_text(json.dumps(
                    {"type": "error", "message": f"No room with code {code}."}))
                await ws.close()
                return
            if len(room.players) >= room.max_players:
                await ws.send_text(json.dumps(
                    {"type": "error", "message": "That room is full."}))
                await ws.close()
                return
            if nickname in {p.nickname for p in room.players}:
                await ws.send_text(json.dumps(
                    {"type": "error",
                     "message": f"Nickname '{nickname}' is already taken in this room. Pick another."}))
                await ws.close()
                return
            player = Player(id=uuid.uuid4().hex[:8], nickname=nickname,
                            colour=free_colour(room, colour), ws=ws)
            room.players.append(player)
        else:
            await ws.send_text(json.dumps(
                {"type": "error", "message": "First message must be create_room or join_room."}))
            await ws.close()
            return

        await send(player, {"type": "joined", "code": room.code,
                            "player_id": player.id, "room": room.public()})
        await broadcast_lobby(room)

        while True:
            msg = json.loads(await ws.receive_text())
            await handle_message(room, player, msg)
    except WebSocketDisconnect:
        pass
    except Exception:
        pass
    finally:
        # Graceful disconnect handling.
        if room is not None and player is not None:
            cancel_needed = False
            if room.phase == "lobby":
                # Remove from the lobby; promote the next human to host.
                was_host = player.is_host
                room.players = [p for p in room.players if p is not player]
                if was_host:
                    for p in room.players:
                        if not p.is_bot:
                            p.is_host = True
                            break
                if not room.humans():
                    ROOMS.pop(room.code, None)
                    return
            else:
                # Mid-game: freeze their score, game continues.
                player.connected = False
                player.ws = None
                if player.is_host:
                    player.is_host = False
                    for p in room.players:
                        if not p.is_bot and p.connected:
                            p.is_host = True
                            break
                if not room.humans() or not any(
                        p.connected for p in room.humans()):
                    cancel_game_task(room)
                    ROOMS.pop(room.code, None)
                    return
            await broadcast_lobby(room)
