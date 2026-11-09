"""
Party Game Hub — v1.2
====================
A free, simple, online party-game hub for 1-8 players (humans + bots).

Stack: Python 3.12, FastAPI + WebSockets, single-page frontend in static/.
No accounts, no sign-in. All game logic lives server-side in this file so
every player in a room sees the same state at the same time.

V1 games: Reaction Test, Speed Math, Speed Trivia.
V2 games: Secret Word / Impostor, President / Ditchdiggers, UNO,
Memory, plus Elimination mode (a series of quick challenges).

Run locally:
    python -m venv .venv
    .venv/bin/pip install -r requirements.txt
    .venv/bin/uvicorn server:app --host 0.0.0.0 --port 8000
Then open http://localhost:8000

Protocol (JSON over one WebSocket per player, endpoint /ws):
  Client -> server:
    create_room {nickname, colour, room_name?}
    join_room   {code, nickname, colour}
    update_settings {room_name?, max_players?, game?, game_settings?,
                     mode?, team_count?}
    add_bot     {}
    update_bot  {bot_id, nickname?, colour?, difficulty?}
    remove_bot  {bot_id}
    kick_player {player_id}       # v1.2 host kick, lobby/results only
    randomize_teams {}            # v1.2 host, teams mode, lobby only
    assign_team {player_id, team} # v1.2 host; team 0..n-1 or null = Alone
    start_game  {}
    reaction_click {}
    answer      {answer, index?}  # math: number as string, trivia: option index;
                                  # index = question number; a stale/late answer
                                  # for an earlier question is ignored
    secret_clue {clue}            # v2 Secret Word: your one-word clue
    secret_vote {player_id}       # v2 Secret Word: who you think is the impostor
    president_play {cards}        # v2 President: play a set of cards
    president_pass {}             # v2 President: pass your turn
    uno_play    {card, colour?}   # v2 UNO: play a card (colour for Wilds)
    uno_draw    {}                # v2 UNO: draw a card
    uno_end_turn {}               # v2 UNO: keep a drawn card, end turn
    memory_answer {answer}        # v2 Memory: type the number back
    elimination_vote {choice}     # v2 Elimination end vote: "again" | "lobby"
    ping        {}                # keep-alive; server replies {"type": "pong"}
    play_again  {}                  # back to lobby, same players
    update_profile {nickname?, colour?}  # own nickname/colour, lobby/results only
    vote_next   {index?}          # vote to advance after a question (quiz games)
    host_skip   {}                # v1.2 host override during a vote break
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
    vote_break  {index, total, scores, votes, votes_needed, timeout, mode, teams}
    vote_update {index, votes, votes_needed, voters:[player_id], host_skipped?}
    results     {game, mode, teams, rankings:[{player_id,nickname,colour,is_bot,score,avg_ms,best_ms,team}]}
    kicked      {message}         # v1.2 reply to the kicked player only
    game_cancelled {reason}
  Server -> client (broadcast to the whole room unless noted):
    joined      {code, player_id, room}   # reply to this player only
    lobby       {room}                    # full room state
    error       {message}
    game_started {game, settings, mode}  # mode: "solo" | "competition"
    reaction_wait {round, total}
    reaction_go   {round, total}
    reaction_round_result {round, total, results:[{player_id,nickname,colour,is_bot,ms,false_start}]}
    question    {game, index, total, text, options, time_limit}
    question_result {index, total, correct_answer, points:[{player_id,points}], scores:[...], mode, teams}
    vote_break  {index, total, scores, votes, votes_needed, timeout, mode, teams}
    vote_update {index, votes, votes_needed, voters:[player_id], host_skipped?}
    # v2.1 Elimination breaks reuse vote_break with extra fields:
    #   elim_break, eliminated:[nicknames], remaining, finishing:[names], ranking:[names]
    #   — only players still alive may vote (eliminated spectators don't count).
    president_notice {message}   # v2.1: a player timed out in President —
                                  # their lowest card was auto-played / they passed
    results     {game, mode, teams, rankings:[{player_id,nickname,colour,is_bot,score,avg_ms,best_ms,team}]}
    kicked      {message}         # v1.2 reply to the kicked player only
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
import os
import random
import re
import secrets
import string
import time
import uuid
from fractions import Fraction
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

VOTE_TIMEOUT_DEFAULT = 60  # seconds to wait for NEXT votes after a question
RECONNECT_GRACE = 30.0     # seconds a disconnected seat is held
HEARTBEAT_SECONDS = 5      # frontend ping interval (v1.1, was 15s)

# Grace periods, overridable by env for tests (PARTY_GRACE_SECONDS,
# PARTY_ROOM_GRACE_SECONDS). Production defaults: a disconnected seat
# is held 30s; a room with no connected humans left is held 120s
# before it is deleted, so a brief host drop does not wipe the room.
DISCONNECT_GRACE = float(os.getenv("PARTY_GRACE_SECONDS", "30"))
ROOM_EMPTY_GRACE = float(os.getenv("PARTY_ROOM_GRACE_SECONDS", "120"))

# Teams mode (v1.2). Team colours are distinct from each other; a
# player's team badge uses these, not their personal colour.
TEAM_NAMES = ["Team 1", "Team 2", "Team 3", "Team 4"]
TEAM_COLOURS = ["#e74c3c", "#3498db", "#2ecc71", "#f1c40f"]

GAMES = {
    "reaction":    {"name": "Reaction Test",        "min_players": 1},
    "math":        {"name": "Speed Math",           "min_players": 1},
    "trivia":      {"name": "Speed Trivia",         "min_players": 1},
    "secret":      {"name": "Secret Word",          "min_players": 4},
    "president":   {"name": "President",            "min_players": 4},
    "uno":         {"name": "UNO",                  "min_players": 2},
    "memory":      {"name": "Memory",               "min_players": 2},
    "elimination": {"name": "Elimination",          "min_players": 4},
}
# Games the 🎲 Random picker may choose (Elimination is a series, not
# a single game, so it is never picked at random).
RANDOM_GAMES = [g for g in GAMES if g != "elimination"]
GAME_CHOICES = list(GAMES) + ["random"]

DEFAULT_SETTINGS = {
    "reaction": {"rounds": 5},
    "math":     {"difficulty": "Medium", "questions": 10, "time_per_question": 15},
    "trivia":   {"category": "Mixed", "difficulty": "Medium",
                 "questions": 10, "time_per_question": 15},
    "secret":   {},
    "president": {"turn_seconds": 30},
    "uno":      {"turn_seconds": 30},
    "memory":   {"memorize_seconds": 10, "recall_seconds": 15},
    # v2.1: the between-challenge NEXT vote break uses this timeout
    # (tests override it to a few seconds).
    "elimination": {"vote_timeout": 60},
}

# ---------------------------------------------------------------------------
# Secret Word / Impostor — word bank (v2)
# ---------------------------------------------------------------------------
# word -> 3-4 plausible one-word clues a bot (who knows the word) can give.
# Impostor bots instead give vague generic words from VAGUE_CLUES.
SECRET_WORDS = {
    # Animals (16)
    "dog": ["pet", "bark", "loyal", "furry"], "cat": ["pet", "meow", "whiskers", "furry"],
    "elephant": ["big", "trunk", "Africa", "ivory"], "lion": ["mane", "roar", "Africa", "king"],
    "tiger": ["stripes", "orange", "jungle", "roar"], "penguin": ["bird", "ice", "waddle", "Antarctica"],
    "snake": ["hiss", "scales", "venom", "slither"], "bear": ["forest", "honey", "claws", "hibernate"],
    "rabbit": ["carrots", "hop", "ears", "fluffy"], "shark": ["teeth", "ocean", "fin", "predator"],
    "eagle": ["bird", "talons", "soar", "America"], "dolphin": ["ocean", "smart", "flipper", "clicks"],
    "horse": ["gallop", "saddle", "mane", "stable"], "monkey": ["banana", "swing", "tail", "jungle"],
    "frog": ["croak", "pond", "green", "jump"], "owl": ["night", "hoot", "wise", "feathers"],
    # Food (16)
    "pizza": ["cheese", "Italy", "pepperoni", "slice"], "banana": ["yellow", "peel", "monkey", "potassium"],
    "chocolate": ["sweet", "cocoa", "brown", "bar"], "bread": ["wheat", "oven", "loaf", "toast"],
    "apple": ["red", "fruit", "orchard", "crunchy"], "cheese": ["milk", "dairy", "cheddar", "melt"],
    "ice cream": ["cold", "sweet", "cone", "scoop"], "spaghetti": ["pasta", "Italy", "sauce", "noodles"],
    "hamburger": ["bun", "beef", "fast-food", "grill"], "popcorn": ["movie", "kernel", "butter", "salty"],
    "sushi": ["rice", "Japan", "fish", "seaweed"], "cake": ["birthday", "sweet", "frosting", "candles"],
    "coffee": ["bean", "caffeine", "morning", "hot"], "egg": ["breakfast", "scramble", "shell", "omelette"],
    "strawberry": ["red", "berry", "sweet", "seeds"], "potato": ["fries", "mash", "vegetable", "spud"],
    # Places (16)
    "beach": ["sand", "ocean", "sun", "towel"], "school": ["teacher", "books", "students", "classroom"],
    "library": ["books", "quiet", "reading", "shelves"], "hospital": ["doctor", "nurse", "patients", "medicine"],
    "airport": ["planes", "travel", "runway", "luggage"], "park": ["trees", "bench", "playground", "grass"],
    "museum": ["art", "history", "exhibits", "tour"], "cinema": ["movie", "popcorn", "screen", "seats"],
    "mountain": ["peak", "climb", "snow", "hiking"], "desert": ["sand", "hot", "cactus", "dry"],
    "island": ["ocean", "palm", "isolated", "tropical"], "castle": ["king", "stone", "tower", "moat"],
    "farm": ["barn", "animals", "crops", "tractor"], "zoo": ["animals", "cages", "visitors", "exhibits"],
    "gym": ["weights", "exercise", "fitness", "treadmill"], "forest": ["trees", "wild", "green", "trail"],
    # Objects (16)
    "phone": ["call", "screen", "apps", "battery"], "bicycle": ["wheels", "pedal", "helmet", "ride"],
    "guitar": ["strings", "music", "rock", "frets"], "book": ["pages", "read", "story", "author"],
    "clock": ["time", "hands", "tick", "alarm"], "umbrella": ["rain", "wet", "cover", "handle"],
    "candle": ["wax", "flame", "light", "birthday"], "mirror": ["reflection", "glass", "look", "shiny"],
    "chair": ["sit", "legs", "wood", "cushion"], "car": ["wheels", "drive", "engine", "road"],
    "train": ["rail", "tracks", "station", "fast"], "kite": ["wind", "string", "sky", "fly"],
    "camera": ["photo", "lens", "flash", "picture"], "key": ["door", "lock", "metal", "unlock"],
    "scissors": ["cut", "paper", "sharp", "blades"], "pillow": ["sleep", "soft", "bed", "dream"],
    # Jobs (8)
    "teacher": ["school", "students", "lesson", "chalk"], "doctor": ["hospital", "patients", "stethoscope", "medicine"],
    "chef": ["kitchen", "cook", "recipe", "apron"], "pilot": ["plane", "fly", "uniform", "cockpit"],
    "farmer": ["crops", "animals", "tractor", "fields"], "firefighter": ["hose", "fire", "truck", "rescue"],
    "singer": ["music", "voice", "stage", "microphone"], "artist": ["paint", "canvas", "brush", "studio"],
}
VAGUE_CLUES = ["thing", "big", "useful", "common", "nice", "interesting",
               "everyday", "special"]

# Memory game (v2, retuned v2.1): per-digit recall error chance for
# bots — longer numbers are harder because every digit must be right,
# AND the per-digit error itself grows with length. p(digits) =
# base + growth * (digits - 4), so P(correct round) = (1 - p) ** digits.
# Simulated survival (mean last digit survived, 4000 bots each):
# Easy ~4.8 digits, Medium ~6.3, Hard ~8.3 — Easy is clearly weakest,
# Hard clearly strongest, at every length. The old flat rates (0.12 /
# 0.06 / 0.02) let a Hard bot lose within 3 rounds ~26% of the time,
# which is what Matthew saw; the new shape keeps bots alive through
# the early rounds and makes them fail gradually as digits grow.
BOT_MEMORY_ERROR = {"Easy": (0.05, 0.03), "Medium": (0.015, 0.012),
                    "Hard": (0.005, 0.005)}

# Elimination mode (v2): challenges are picked at random from the
# quick games that produce a clear per-challenge ranking. Secret Word,
# President and UNO are NOT in the pool: they don't produce a
# last-place ranking per challenge and take too long for a challenge
# round. Challenges are kept short (3 rounds / 3 questions).
ELIM_POOL = ["reaction", "math", "trivia", "memory"]
ELIM_CHALLENGE = {
    "reaction": {"rounds": 3},
    "math": {"questions": 3, "time_per_question": 10},
    "trivia": {"questions": 3, "time_per_question": 10},
    "memory": {"memorize_seconds": 3, "recall_seconds": 8},
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
    disconnected_at: float | None = None   # monotonic time of last disconnect
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
    # v1.2: game mode "ffa" (default) or "teams". teams maps
    # player_id -> team index (0..team_count-1) or None ("Alone").
    mode: str = "ffa"
    team_count: int = 2
    teams: dict = field(default_factory=dict)
    game_task: asyncio.Task | None = None
    # Per-game transient state (reset at each game start)
    current_game: str | None = None
    round_state: dict = field(default_factory=dict)

    def public(self):
        players = []
        for p in self.players:
            d = p.public()
            d["team"] = self.teams.get(p.id)  # None = Alone (teams mode)
            players.append(d)
        return {"code": self.code, "name": self.name,
                "max_players": self.max_players, "phase": self.phase,
                "game": self.game, "game_settings": self.game_settings,
                "current_game": self.current_game,
                "mode": self.mode, "team_count": self.team_count,
                "teams": {pid: t for pid, t in self.teams.items()},
                "players": players,
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


def votes_needed_humans(room: Room) -> int:
    """NEXT-vote threshold (v1.2): ceil(connected HUMANS / 2).

    Bots do not vote and do not count. With zero connected humans
    (should not happen in a live game) the threshold is 1 and the
    60s timeout is the only way the break ends.
    """
    humans = [p for p in room.players if not p.is_bot and p.connected]
    return max(1, (len(humans) + 1) // 2)


def team_standings(room: Room):
    """Team standings for Teams mode (v1.2).

    Team score = the AVERAGE of its members' individual scores.
    Every 'Alone' player (no team assigned) is their own one-person
    team. Returns a list ranked by average score, highest first, of
    {"team": index|None, "name": str, "colour": str,
     "average": float, "members": [player public dicts]}.
    In FFA mode this returns [] (individual rankings are used instead).
    """
    if room.mode != "teams":
        return []
    groups = {}
    for p in room.players:
        t = room.teams.get(p.id)
        groups.setdefault(t, []).append(p)
    out = []
    for t, members in groups.items():
        avg = round(sum(m.score for m in members) / len(members), 1) if members else 0.0
        if t is None:
            # Each Alone player is their own team; list them separately
            # so one Alone player's score is not averaged with another's.
            for m in members:
                out.append({"team": None, "name": f"{m.nickname} (Alone)",
                            "colour": "#95a5a6", "average": float(m.score),
                            "members": [m.public()]})
        else:
            out.append({"team": t, "name": TEAM_NAMES[t],
                        "colour": TEAM_COLOURS[t], "average": avg,
                        "members": [m.public() for m in members]})
    out.sort(key=lambda r: -r["average"])
    return out


def colour_taken(room: Room, colour: str, exclude_id: str | None = None) -> bool:
    """v1.2: no two players/bots in a room may share a colour."""
    return any(p.colour == colour and p.id != exclude_id
               for p in room.players)


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
                     "false_starts": p.false_starts,
                     "team": room.teams.get(p.id)})
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


async def schedule_room_deletion(room: Room, delay: float = ROOM_EMPTY_GRACE):
    """Delete a room with no connected humans left, after a grace period.

    v1.2: a room used to vanish ~30s after its (sole) host dropped —
    that is what killed room SNYN7F while Matthew was fighting
    reconnection errors. Now the room is held for ROOM_EMPTY_GRACE
    (default 120s) so a brief drop does not wipe it; it is only
    deleted if nobody has reconnected by then. A room with bots but
    no humans is deleted the same way (bots alone cannot play).
    """
    await asyncio.sleep(delay)
    if room.code not in ROOMS:
        return
    if any(p.connected for p in room.humans()):
        return
    ROOMS.pop(room.code, None)


def _remove_player(room: Room, player: Player):
    """Drop a player from the room, promoting a new host if needed."""
    was_host = player.is_host
    room.players = [p for p in room.players if p is not player]
    room.teams.pop(player.id, None)
    if was_host:
        for p in room.players:
            if not p.is_bot and p.connected:
                p.is_host = True
                break
        else:
            for p in room.players:
                if not p.is_bot:
                    p.is_host = True
                    break


async def schedule_disconnect_cleanup(room: Room, player: Player,
                                      delay: float = DISCONNECT_GRACE):
    """Remove a disconnected player after a grace period — in ANY phase.

    v1.2 root-cause fix for the stale-nickname bug (room LOM9NZ):
    the old code only ever removed a player who disconnected *in the
    lobby*. A player who disconnected after a game ended (phase
    "results") or mid-game hit a branch that scheduled NO cleanup
    at all, so they stayed in room.players forever with
    connected=False and their nickname stayed "already taken" for
    minutes — even after their browser was closed. Now every
    disconnect schedules this task: it waits `delay` seconds, and if
    a game is still playing it keeps waiting (checking every 5s)
    until the game ends, then removes the player if they never
    reconnected. Reconnecting cancels it (player.connected is True).
    A token guards against a stale task removing a player who
    disconnected, reconnected, and disconnected again.
    """
    token = player.disconnected_at
    await asyncio.sleep(delay)
    waited = 0.0
    while room.phase == "playing" and waited < 900:
        if player.connected or player not in room.players:
            return
        if player.disconnected_at != token:
            return
        await asyncio.sleep(5)
        waited += 5
    if player.connected or player not in room.players:
        return
    if player.disconnected_at != token:
        return
    _remove_player(room, player)
    if not room.humans():
        # Last human gone: hold the room briefly, then delete it.
        asyncio.create_task(schedule_room_deletion(room))
        return
    await broadcast_lobby(room)


async def remove_lobby_player_after_grace(room: Room, player: Player,
                                          delay: float = DISCONNECT_GRACE):
    """Backwards-compatible name for the disconnect cleanup task."""
    await schedule_disconnect_cleanup(room, player, delay=delay)


# ---------------------------------------------------------------------------
# Question generation
# ---------------------------------------------------------------------------

def _fmt_num(n: int) -> str:
    """Format an integer for display, parenthesised when negative so
    e.g. '8 - -7' is shown as '8 - (-7)'."""
    return f"({n})" if n < 0 else str(n)


def make_math_question(difficulty: str):
    """Return (text, correct_answer) — int, except Hard fraction
    questions, whose answer is a float (decimal to 2 places).

    Easy:   addition / subtraction only, operands and results in -30..30.
    Medium: addition / subtraction (wider), multiplication up to 12x12,
            division with exact integer results.
    Hard:   high-school level — basic functions f(x), solving for x,
            max/min of a quadratic (vertex form), advanced BEDMAS with
            brackets, and adding/subtracting fractions (answer as a
            decimal to 2 places, stated in the question).
    """
    if difficulty == "Easy":
        op = random.choice(["+", "-"])
        while True:
            a = random.randint(-30, 30)
            b = random.randint(-30, 30)
            ans = a + b if op == "+" else a - b
            if -30 <= ans <= 30:
                break
        if op == "+":
            return f"{_fmt_num(a)} + {_fmt_num(b)} = ?", ans
        return f"{_fmt_num(a)} − {_fmt_num(b)} = ?", ans
    if difficulty == "Medium":
        op = random.choice(["+", "-", "x", "÷"])
        if op == "x":
            a, b = random.randint(2, 12), random.randint(2, 12)
            return f"{a} × {b} = ?", a * b
        if op == "÷":
            b = random.randint(2, 12)
            ans = random.randint(2, 12)
            return f"{b * ans} ÷ {b} = ?", ans
        a, b = random.randint(-50, 50), random.randint(-50, 50)
        if op == "+":
            return f"{_fmt_num(a)} + {_fmt_num(b)} = ?", a + b
        return f"{_fmt_num(a)} − {_fmt_num(b)} = ?", a - b
    # Hard — pick one of five high-school types.
    kind = random.choice(["function", "solve", "vertex", "bedmas", "fraction"])
    if kind == "function":
        a = random.randint(2, 5)
        b = random.randint(-5, 10)
        x = random.randint(2, 9)
        b_txt = f" + {b}" if b >= 0 else f" - {-b}"
        return f"f(x) = {a}x{b_txt}. Find f({x}) = ?", a * x + b
    if kind == "solve":
        a = random.randint(2, 6)
        x = random.randint(2, 12)
        b = random.randint(-10, 10)
        c = a * x + b
        b_txt = f" + {b}" if b >= 0 else f" - {-b}"
        return f"Solve for x: {a}x{b_txt} = {c}, x = ?", x
    if kind == "vertex":
        # Vertex form f(x) = a(x-h)^2 + k; max/min value is k.
        a = random.choice([-2, -1, 1, 2])
        h = random.randint(-3, 3)
        k = random.randint(-5, 10)
        h_txt = f" - {h}" if h >= 0 else f" + {-h}"
        k_txt = f" + {k}" if k >= 0 else f" - {-k}"
        if a == 1:
            coef = ""
        elif a == -1:
            coef = "-"
        else:
            coef = str(a)
        word = "maximum" if a < 0 else "minimum"
        return (f"Find the {word} value of f(x) = {coef}(x{h_txt})^2{k_txt}",
                k)
    if kind == "bedmas":
        form = random.choice(["paren_add", "paren_mul", "mul_add"])
        if form == "paren_add":
            a, b, c, d = (random.randint(2, 9) for _ in range(4))
            return f"({_fmt_num(a)} + {b}) × {c} - {d} = ?", (a + b) * c - d
        if form == "paren_mul":
            a, b, c, d = (random.randint(2, 9) for _ in range(4))
            return f"{a} × ({b} + {c}) - {d} = ?", a * (b + c) - d
        a, b, c, d = (random.randint(2, 12) for _ in range(4))
        return f"{a} + {b} × {c} - {d} = ?", a + b * c - d
    # fraction — answer as a decimal to 2 places (stated in the text).
    n1 = random.randint(1, 5)
    d1 = random.choice([2, 3, 4, 5, 6, 8])
    n2 = random.randint(1, 5)
    d2 = random.choice([2, 3, 4, 5, 6, 8])
    op = random.choice(["+", "-"])
    val = Fraction(n1, d1) + Fraction(n2, d2) if op == "+" else Fraction(n1, d1) - Fraction(n2, d2)
    ans = round(float(val), 2)
    return (f"{n1}/{d1} {op} {n2}/{d2} = ? "
            f"Give your answer as a decimal to 2 decimal places", ans)


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
# President — deck helpers (v2)
# ---------------------------------------------------------------------------

PRES_SUITS = ["♣", "♦", "♥", "♠"]
PRES_RANKS = ["3", "4", "5", "6", "7", "8", "9", "10", "J", "Q", "K", "A", "2"]
PRES_RANK_VALUE = {r: i + 3 for i, r in enumerate(PRES_RANKS)}  # 3 -> 3, ..., 2 -> 15


def pres_rank(card: str) -> int:
    """Rank value of a President card like '10♦' or 'J♥' (3 low .. 2 high)."""
    return PRES_RANK_VALUE[card[:-1]]


def pres_rank_label(card: str) -> str:
    return card[:-1]


def build_president_deck():
    deck = [r + s for r in PRES_RANKS for s in PRES_SUITS]
    random.shuffle(deck)
    return deck


PRES_SIZE_WORDS = {1: "a single", 2: "a pair", 3: "three of a kind",
                   4: "four of a kind"}


def pres_rank_name(value: int) -> str:
    """Rank label for a rank value, e.g. 8 -> '8', 13 -> 'K', 15 -> '2'."""
    return PRES_RANKS[value - 3]


def president_illegal_reason(hand, cards, trick):
    """Why `cards` is NOT a legal President play, in plain English —
    or None if the play is legal. `trick` is None when leading a new
    trick, else {"size": n, "rank": v}.

    This is the authority the server sends back on a rejected play,
    so the player is never left guessing: the single most common
    confusion is the SIZE rule — one higher card does NOT beat a pair,
    you must match the trick's size with a higher rank.
    """
    if not cards:
        return "You haven't selected any cards — pick at least one card to play."
    if len(cards) > 4:
        return "You can play at most 4 cards at once — you picked %d." % len(cards)
    missing = [c for c in cards if c not in hand]
    if missing:
        return ("That card isn't in your hand anymore (%s) — "
                "your hand may have changed; pick again."
                % ", ".join(missing))
    ranks = {pres_rank(c) for c in cards}
    if len(ranks) != 1:
        return ("All the cards in a set must be the same rank — you "
                "picked different ranks.")
    rank = pres_rank(cards[0])
    if trick is None:
        return None  # leading: any set of 1-4 of the same rank
    size, trick_rank = trick["size"], trick["rank"]
    if len(cards) != size:
        return ("The trick is %s of %ss — you need to play %d card%s "
                "of the same rank, not %d."
                % (PRES_SIZE_WORDS[size], pres_rank_name(trick_rank),
                   size, "s" if size > 1 else "", len(cards)))
    if rank <= trick_rank:
        return ("%s of %ss doesn't beat %s of %ss — you need a "
                "higher rank." % (PRES_SIZE_WORDS[size],
                                  pres_rank_name(rank),
                                  PRES_SIZE_WORDS[size],
                                  pres_rank_name(trick_rank)))
    return None


def president_legal_play(hand, cards, trick):
    """Is `cards` (a list of card strings) a legal President play?

    `trick` is None when leading a new trick, else {"size": n, "rank": v}.
    Legal = every card is in the hand, all the same rank, and either
    leading (any set of 1-4) or matching the trick size with a higher rank.
    """
    return president_illegal_reason(hand, cards, trick) is None


def president_lowest_legal(hand, trick):
    """The bot's strategy: the lowest set that beats the trick, or the
    lowest set in hand when leading. Returns a list of cards, or []."""
    by_rank = {}
    for c in hand:
        by_rank.setdefault(pres_rank(c), []).append(c)
    for rank in sorted(by_rank):
        group = by_rank[rank]
        if trick is None:
            return group[:1]
        if len(group) >= trick["size"] and rank > trick["rank"]:
            return group[:trick["size"]]
    return []


def president_scores_for(n: int):
    """Points by finish order (President first ... Asshole last).

    Linear scale from 1000 down to 0 in equal steps, so with 5 players
    it is exactly 1000 / 750 / 500 / 250 / 0, and with 4 it is
    1000 / 667 / 333 / 0.
    """
    if n <= 1:
        return [1000]
    return [round(1000 * (n - 1 - i) / (n - 1)) for i in range(n)]


# ---------------------------------------------------------------------------
# UNO — deck helpers (v2)
# ---------------------------------------------------------------------------

UNO_COLOURS = ["R", "G", "B", "Y"]
UNO_COLOUR_NAMES = {"R": "Red", "G": "Green", "B": "Blue", "Y": "Yellow"}


def build_uno_deck():
    """A standard 108-card UNO deck. Cards are strings like 'R5',
    'Gskip', 'Bdraw2', 'Wwild', 'Wdraw4'."""
    deck = []
    for colour in UNO_COLOURS:
        deck.append(colour + "0")
        for n in range(1, 10):
            deck.extend([colour + str(n)] * 2)
        for action in ("skip", "reverse", "draw2"):
            deck.extend([colour + action] * 2)
    deck.extend(["Wwild"] * 4)
    deck.extend(["Wdraw4"] * 4)
    random.shuffle(deck)
    return deck


def uno_value(card: str) -> str:
    """The matchable value of an UNO card: '0'-'9', 'skip', 'reverse',
    'draw2', 'wild', 'draw4'."""
    if card.startswith("W"):
        return card[1:]
    return card[1:]


def uno_colour(card: str) -> str | None:
    return None if card.startswith("W") else card[0]


def uno_match(card: str, top: str, current_colour: str) -> bool:
    """Can `card` be played on `top`? Wilds are always playable; a card
    matches by colour (the colour in play, which a Wild may have set)
    or by value."""
    if card.startswith("W"):
        return True
    if uno_colour(card) == current_colour:
        return True
    return uno_value(card) == uno_value(top)


def uno_bot_choice(hand, top, current_colour):
    """The bot's UNO strategy: play a matching card if it has one,
    preferring number cards and saving Wilds for last. Returns the
    card string, or None to draw."""
    matching = [c for c in hand if uno_match(c, top, current_colour)]
    if not matching:
        return None
    numbers = [c for c in matching if uno_value(c).isdigit()]
    actions = [c for c in matching if not uno_value(c).isdigit()
               and not c.startswith("W")]
    wilds = [c for c in matching if c.startswith("W")]
    for pool in (numbers, actions, wilds):
        if pool:
            return pool[0]
    return None


def uno_best_colour(hand):
    """The colour a bot picks after playing a Wild: its most common
    colour in hand (random if it has none)."""
    counts = {}
    for c in hand:
        col = uno_colour(c)
        if col:
            counts[col] = counts.get(col, 0) + 1
    if not counts:
        return random.choice(UNO_COLOURS)
    return max(counts, key=lambda k: counts[k])


def secret_tally(votes, impostor_ids):
    """Pure helper for Secret Word voting. `votes` maps voter_id ->
    suspect_id. Returns (eliminated_id or None, word_team_won).

    Most votes wins; a tie means nobody is eliminated and the
    impostors win. Catching any impostor means the word team wins.
    """
    if not votes:
        return None, False
    counts = {}
    for suspect in votes.values():
        counts[suspect] = counts.get(suspect, 0) + 1
    top = max(counts.values())
    leaders = [pid for pid, n in counts.items() if n == top]
    if len(leaders) != 1:
        return None, False  # tie: no elimination, impostors win
    eliminated = leaders[0]
    return eliminated, eliminated in impostor_ids


def memory_bot_correct(difficulty: str, digits: int) -> bool:
    """Does a bot of this difficulty recall a `digits`-digit number?

    Per-digit error chance grows with length: p = base + growth *
    (digits - 4), capped at 0.5, so a bot that breezes through 4
    digits still starts failing as the number gets longer.
    """
    base, growth = BOT_MEMORY_ERROR.get(difficulty or "Medium",
                                        (0.015, 0.012))
    p = min(0.5, base + growth * max(0, digits - 4))
    return all(random.random() >= p for _ in range(digits))


# ---------------------------------------------------------------------------
# Game handlers — one coroutine per game. Add new games here in later phases.
# ---------------------------------------------------------------------------

async def run_reaction_game(room: Room, participants=None, challenge=False):
    """Reaction Test. Server controls the wait/go timing for everyone.

    v2: `participants` limits the game to a subset of players (used by
    Elimination challenges); `challenge=True` skips the NEXT vote
    between rounds (challenges auto-advance) and, instead of
    broadcasting the final results, returns the ranking (best average
    first) so the caller can use it. In a normal game the behaviour
    is unchanged, except rounds now end with the same NEXT vote break
    as Math/Trivia (v2): after each round except the last, humans vote
    NEXT, advancing at half of connected humans or the 60s timeout,
    with the host's Skip override. Bots do not vote.
    """
    settings = room.game_settings["reaction"]
    total = int(settings.get("rounds", 5))
    if challenge:
        total = int(ELIM_CHALLENGE["reaction"].get("rounds", 3))
    active = participants if participants is not None else list(room.players)
    room.round_state = {"round": 0, "go_at": None, "clicks": {},
                        "participants": {p.id for p in active}}
    try:
        for rnd in range(1, total + 1):
            room.round_state = {"round": rnd, "go_at": None, "clicks": {},
                                "participants": {p.id for p in active}}
            await broadcast(room, {"type": "reaction_wait",
                                   "round": rnd, "total": total})
            # Decide each bot's plan for this round up front.
            bot_plans = {}
            for p in active:
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
            for p in active:
                if p.is_bot and bot_plans[p.id]["false"]:
                    asyncio.create_task(bot_false_start(
                        p, delay * random.uniform(0.3, 0.9)))
            await asyncio.sleep(delay)
            if room.phase != "playing":
                return None
            room.round_state["go_at"] = time.monotonic()
            await broadcast(room, {"type": "reaction_go",
                                   "round": rnd, "total": total})
            # Schedule bot (on-time) clicks.
            async def bot_click(bot, ms):
                await asyncio.sleep(ms / 1000.0)
                if room.phase == "playing" and room.round_state.get("round") == rnd:
                    record_reaction(room, bot, ms)
            for p in active:
                if p.is_bot and not bot_plans[p.id]["false"]:
                    asyncio.create_task(bot_click(p, bot_plans[p.id]["ms"]))
            # Wait until everyone has clicked or 5s pass.
            # v1.1: a disconnected player does NOT end the round early —
            # if anyone is disconnected (within the reconnect grace),
            # hold the round for the full window so they can rejoin
            # and click. Only break early when every *connected*
            # player has clicked and nobody is disconnected.
            deadline = time.monotonic() + 5.0
            while time.monotonic() < deadline:
                clicked = {pid for pid in room.round_state["clicks"]}
                if any(not p.connected for p in active):
                    await asyncio.sleep(0.1)
                    continue
                remaining = [p for p in active
                             if p.id not in clicked and p.connected]
                if not remaining:
                    break
                await asyncio.sleep(0.1)
            results = []
            for p in active:
                c = room.round_state["clicks"].get(p.id)
                results.append({"player_id": p.id, "nickname": p.nickname,
                                "colour": p.colour, "is_bot": p.is_bot,
                                "ms": c["ms"] if c else None,
                                "false_start": bool(c and c["false_start"])})
            await broadcast(room, {"type": "reaction_round_result",
                                   "round": rnd, "total": total,
                                   "results": results})
            if rnd < total:
                if challenge:
                    await asyncio.sleep(1.2)
                else:
                    # v2: NEXT vote break between rounds (not after last).
                    await _vote_phase(room, "reaction", rnd, total)
        if challenge:
            return reaction_rankings(active)
        room.phase = "results"
        await broadcast(room, {"type": "results", "game": "reaction",
                               "mode": room.mode,
                               "teams": team_standings(room),
                               "rankings": rankings(room, by_avg=True)})
        await broadcast_lobby(room)
        return None
    except asyncio.CancelledError:
        return None


def reaction_rankings(players):
    """Rank players by average reaction time (fastest first). Players
    with no successful round go last. Returns [{player_id, avg_ms}]."""
    rows = []
    for p in players:
        avg = (round(sum(p.reaction_times) / len(p.reaction_times))
               if p.reaction_times else None)
        rows.append({"player_id": p.id, "nickname": p.nickname,
                     "colour": p.colour, "is_bot": p.is_bot,
                     "avg_ms": avg})
    rows.sort(key=lambda r: (r["avg_ms"] is None, r["avg_ms"] or 0))
    return rows


def record_reaction(room: Room, player: Player, ms, false_start=False):
    """Record one player's click for the current reaction round."""
    clicks = room.round_state.setdefault("clicks", {})
    parts = room.round_state.get("participants")
    if parts is not None and player.id not in parts:
        return  # eliminated/spectating in an Elimination challenge
    if player.id in clicks:
        return  # only the first click per round counts
    clicks[player.id] = {"ms": ms, "false_start": false_start}
    if false_start:
        player.false_starts += 1
    elif ms is not None:
        player.reaction_times.append(ms)


async def _vote_phase(room: Room, game: str, idx: int, total: int, *,
                      scores=None, extra=None, eligible=None):
    """NEXT vote break (v1.1, v1.2, v2.1).

    Advances when at least half of the CONNECTED HUMAN players have
    voted NEXT, or after the vote timeout (default 60s), or when the
    host uses the host-skip override (v1.2, see host_skip).
    v1.2: bots do NOT vote and do NOT count toward the threshold —
    they used to auto-vote, which let bots decide the break.
    v2.1: `scores` overrides the scoreboard rows shown on the break,
    `extra` adds fields to the vote_break message (used by
    Elimination breaks), and `eligible` restricts who may vote and
    who counts toward the threshold (Elimination: only players still
    alive — an eliminated spectator must not be able to hold the
    break hostage, and must not count toward the half).
    """
    settings = room.game_settings[game]
    timeout = int(settings.get("vote_timeout", VOTE_TIMEOUT_DEFAULT))
    room.round_state["votes"] = set()
    room.round_state["vote_index"] = idx
    room.round_state["host_skip"] = False
    if scores is None:
        scores = [{"player_id": pl.id, "nickname": pl.nickname,
                   "colour": pl.colour, "is_bot": pl.is_bot,
                   "score": pl.score, "team": room.teams.get(pl.id)}
                  for pl in room.players]
    if eligible is None:
        room.round_state["vote_eligible"] = None
    else:
        room.round_state["vote_eligible"] = {p.id for p in eligible}

    def votes_needed():
        if eligible is None:
            return votes_needed_humans(room)
        alive_humans = [p for p in room.players
                        if not p.is_bot and p.connected
                        and p.id in room.round_state["vote_eligible"]]
        return max(1, (len(alive_humans) + 1) // 2)

    msg = {"type": "vote_break", "index": idx, "total": total,
           "scores": scores, "votes": 0,
           "votes_needed": votes_needed(), "timeout": timeout,
           "mode": room.mode, "teams": team_standings(room)}
    if extra:
        msg.update(extra)
    await broadcast(room, msg)
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if room.round_state.get("host_skip"):
            break
        if len(room.round_state.get("votes", set())) >= votes_needed():
            break
        await asyncio.sleep(0.1)
    room.round_state.pop("votes", None)
    room.round_state.pop("vote_index", None)
    room.round_state.pop("host_skip", None)
    room.round_state.pop("vote_eligible", None)


async def run_quiz_game(room: Room, game: str, participants=None,
                      challenge=False):
    """Shared loop for Speed Math and Speed Trivia.

    v2: `participants` limits the game to a subset of players and
    `challenge=True` runs it as an Elimination challenge — shorter
    (3 questions), no NEXT vote between questions, and instead of
    broadcasting the final results it returns the ranking (highest
    score first; ties broken by total time spent on correct answers,
    fastest first) so the caller can use it.
    """
    settings = room.game_settings[game]
    if challenge:
        total = int(ELIM_CHALLENGE[game].get("questions", 3))
        time_limit = int(ELIM_CHALLENGE[game].get(
            "time_per_question", settings.get("time_per_question", 15)))
    else:
        total = int(settings.get("questions", 10))
        time_limit = int(settings.get("time_per_question", 15))
    active = participants if participants is not None else list(room.players)
    # Total time spent on correct answers, for tie-breaking in challenges.
    correct_time = {p.id: 0 for p in active}
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
                                "correct": q["answer"],
                                "participants": {p.id for p in active},
                                "question": {"text": q["text"],
                                             "options": q["options"],
                                             "time_limit": time_limit}}
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
                if st.get("participants") and bot.id not in st["participants"]:
                    return
                if game == "math":
                    if correct:
                        ans = q["answer"]
                    elif isinstance(q["answer"], float):
                        ans = round(float(q["answer"]) + random.choice([-0.3, -0.2, -0.1, 0.1, 0.2, 0.3]), 2)
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
            for p in active:
                if p.is_bot:
                    cfg = BOT_QUIZ[p.difficulty or "Medium"]
                    delay = random.uniform(*cfg["delay"])
                    delay = min(delay, time_limit - 0.5)
                    asyncio.create_task(bot_answer(
                        p, random.random() < cfg["accuracy"], max(0.5, delay)))
            # Wait for the timer, or until every connected player answered.
            # v1.1: a disconnected player does NOT end the round early.
            # If anyone is disconnected (within the reconnect grace),
            # hold the round for the full timer so they can rejoin and
            # answer. This was the real cause of the "ENTER ends the
            # round" report: with Max disconnected, Matthew answering
            # alone made "everyone answered" true immediately.
            deadline = time.monotonic() + time_limit
            while time.monotonic() < deadline:
                if any(not pl.connected for pl in active):
                    await asyncio.sleep(0.1)
                    continue
                answered = set(room.round_state["answers"])
                remaining = [pl for pl in active
                             if pl.id not in answered and pl.connected]
                if not remaining:
                    break
                await asyncio.sleep(0.1)
            # Reveal the answer and the points awarded this question.
            correct = q["answer"]
            points = []
            for pid, a in room.round_state["answers"].items():
                if a["points"]:
                    points.append({"player_id": pid, "points": a["points"]})
                    correct_time[pid] = correct_time.get(pid, 0) + a["elapsed_ms"]
            await broadcast(room, {"type": "question_result",
                                   "index": idx, "total": total,
                                   "correct_answer": correct,
                                   "points": points,
                                   "scores": [{"player_id": pl.id,
                                               "nickname": pl.nickname,
                                               "colour": pl.colour,
                                               "is_bot": pl.is_bot,
                                               "score": pl.score,
                                               "team": room.teams.get(pl.id)}
                                              for pl in active],
                                   "mode": room.mode,
                                   "teams": team_standings(room)})
            if idx < total:
                if challenge:
                    await asyncio.sleep(1.2)
                else:
                    # v1.1 NEXT QUESTION vote break (not after the last question).
                    await _vote_phase(room, game, idx, total)
            else:
                await asyncio.sleep(1.2)
        if challenge:
            rows = [{"player_id": p.id, "nickname": p.nickname,
                     "colour": p.colour, "is_bot": p.is_bot,
                     "score": p.score,
                     "time_ms": correct_time.get(p.id, 0)} for p in active]
            rows.sort(key=lambda r: (-r["score"], r["time_ms"]))
            return rows
        room.phase = "results"
        await broadcast(room, {"type": "results", "game": game,
                               "mode": room.mode,
                               "teams": team_standings(room),
                               "rankings": rankings(room)})
        await broadcast_lobby(room)
        return None
    except asyncio.CancelledError:
        return None


def record_answer(room: Room, player: Player, answer, index=None):
    """Score one quiz answer. Faster correct answers earn more points.

    A late answer must never leak into the next question: an answer whose
    question index does not match the current question, or which arrives
    after the question's time limit, is ignored (not recorded). Before
    this guard, a slow answer arriving during the 1.2s reveal / next
    question was recorded against the *next* question, and the player's
    real answer to that next question was then dropped as a duplicate.
    """
    st = room.round_state
    if not st or room.phase != "playing":
        return
    parts = st.get("participants")
    if parts is not None and player.id not in parts:
        return  # eliminated/spectating in an Elimination challenge
    if index is not None and st.get("index") != index:
        return  # stale answer for an earlier question
    if player.id in st.get("answers", {}):
        return  # first answer only
    correct = st["correct"]
    elapsed = time.monotonic() - st["start"]
    time_limit = 15
    # time_limit is in the broadcast question; recover from settings.
    game = room.current_game
    if game:
        time_limit = int(room.game_settings[game].get("time_per_question", 15))
    if elapsed > time_limit:
        return  # arrived after the timer; do not record it
    is_correct = False
    if game == "math":
        try:
            # Hard fraction questions have a float answer (decimal to
            # 2 places): accept within 0.01. All other math answers
            # are exact integers.
            if isinstance(correct, float):
                is_correct = abs(float(str(answer).strip()) - float(correct)) <= 0.01
            else:
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


def record_memory_answer(room: Room, player: Player, answer):
    """Record one Memory recall answer (first answer only, participants
    only). Stores the answer and how long the player took."""
    st = room.round_state
    if not st or room.phase != "playing":
        return
    parts = st.get("participants")
    if parts is not None and player.id not in parts:
        return
    if player.id in st.get("answers", {}):
        return
    start = st.get("start") or time.monotonic()
    st.setdefault("answers", {})[player.id] = {
        "answer": str(answer).strip(),
        "elapsed_ms": round((time.monotonic() - start) * 1000)}


def _wrong_number(number: str) -> str:
    """A wrong version of a number: change one digit to a different one."""
    i = random.randrange(len(number))
    digit = random.choice([d for d in "0123456789" if d != number[i]])
    return number[:i] + digit + number[i + 1:]


async def _bot_memory_answer(room: Room, bot: Player, recall_seconds: float):
    """A bot's Memory recall: it answers after a short delay with the
    number it (may or may not have) remembered."""
    await asyncio.sleep(random.uniform(0.5, min(3.0, recall_seconds - 0.5)))
    if room.phase != "playing":
        return
    st = room.round_state
    if bot.id in st.get("answers", {}):
        return
    ans = st.get("bot_answers", {}).get(bot.id)
    if ans is None:
        return
    record_memory_answer(room, bot, ans)


async def run_memory_game(room: Room, participants=None, challenge=False,
                          challenge_settings=None):
    """Memory game (v2, Matthew's design).

    Each surviving player gets a UNIQUE number to memorize (4 digits
    first, then 5, 6, 7, ...). Wrong answer or timeout = eliminated.
    Right answer = survive to the next round with one more digit.
    Rounds continue until ONE player remains. TIE RULE: if every
    remaining player is eliminated in the same round, those tied
    players redo the SAME digit-length round with NEW numbers, until
    one of them gets it right.
    Scoring: winner 1000; eliminated players 100 per round survived
    (eliminated in round 3 = 200).
    As a challenge (Elimination) it returns (ranking_ids, elim_order)
    — the full ranking, best first — instead of broadcasting results;
    challenges are also capped at 5 rounds (digits 4-8), after which
    survivors are ranked by total recall time, slowest last.
    """
    settings = room.game_settings["memory"]
    if challenge and challenge_settings:
        memorize = int(challenge_settings.get(
            "memorize_seconds", settings.get("memorize_seconds", 10)))
        recall = int(challenge_settings.get(
            "recall_seconds", settings.get("recall_seconds", 15)))
    else:
        memorize = int(settings.get("memorize_seconds", 10))
        recall = int(settings.get("recall_seconds", 15))
    alive = list(participants if participants is not None
                 else room.players)
    digits = 4
    elim_order = []
    survived = {p.id: 0 for p in alive}
    # Total time spent on correct recalls, for ranking survivors if a
    # challenge hits the round cap (below).
    recall_time = {p.id: 0 for p in alive}
    # As an Elimination challenge, Memory is capped at 5 rounds (digits
    # 4-8): two perfect memorizers would otherwise play forever, since
    # the digits just keep growing. A normal Memory game has no cap.
    max_rounds = 5 if challenge else None
    rounds_played = 0
    try:
        while len(alive) > 1 and (max_rounds is None
                                  or rounds_played < max_rounds):
            rounds_played += 1
            rnd = digits - 3
            # Unique numbers, one per surviving player.
            numbers = {}
            used = set()
            for p in alive:
                while True:
                    num = str(random.randint(1, 9)) + "".join(
                        str(random.randint(0, 9)) for _ in range(digits - 1))
                    if num not in used:
                        used.add(num)
                        numbers[p.id] = num
                        break
            room.round_state = {"participants": {p.id for p in alive},
                                "digits": digits, "round": rnd,
                                "numbers": numbers, "answers": {},
                                "bot_answers": {}, "start": None,
                                "phase": "memorize"}
            for p in alive:
                if p.is_bot:
                    ok = memory_bot_correct(p.difficulty, digits)
                    room.round_state["bot_answers"][p.id] = (
                        numbers[p.id] if ok else _wrong_number(numbers[p.id]))
                else:
                    await send(p, {"type": "memory_show", "digits": digits,
                                   "number": numbers[p.id],
                                   "seconds": memorize, "round": rnd})
            await broadcast(room, {"type": "memory_round_start",
                                   "digits": digits, "round": rnd,
                                   "seconds": memorize,
                                   "alive": [p.id for p in alive]})
            await asyncio.sleep(memorize)
            if room.phase != "playing":
                return (None, None) if challenge else None
            room.round_state["phase"] = "recall"
            room.round_state["start"] = time.monotonic()
            await broadcast(room, {"type": "memory_recall", "digits": digits,
                                   "round": rnd, "time_limit": recall})
            for p in alive:
                if p.is_bot:
                    asyncio.create_task(_bot_memory_answer(room, p, recall))
            deadline = time.monotonic() + recall
            while time.monotonic() < deadline:
                if any(not pl.connected for pl in alive):
                    await asyncio.sleep(0.1)
                    continue
                answered = set(room.round_state["answers"])
                if all(pl.id in answered for pl in alive if pl.connected):
                    break
                await asyncio.sleep(0.1)
            newly = []
            for p in alive:
                a = room.round_state["answers"].get(p.id)
                if a and a["answer"] == numbers[p.id]:
                    survived[p.id] += 1
                    recall_time[p.id] += a["elapsed_ms"]
                else:
                    newly.append((p, a["elapsed_ms"] if a else recall * 1000))
            if newly:
                # Fastest wrong answer was eliminated first (this order
                # is what an Elimination challenge uses for last place).
                newly.sort(key=lambda t: t[1])
                for p, _ in newly:
                    elim_order.append(p.id)
                    p.score = 100 * survived[p.id]
                eliminated_ids = {p.id for p, _ in newly}
                alive = [p for p in alive if p.id not in eliminated_ids]
            await broadcast(room, {"type": "memory_round_result",
                                   "round": rnd, "digits": digits,
                                   "survivors": [p.id for p in alive],
                                   "eliminated": [p.id for p, _ in newly],
                                   "numbers": numbers})
            if len(alive) == 0:
                # TIE: everyone out in the same round — the tied players
                # redo the SAME digit-length round with new numbers. They
                # are not actually eliminated, so undo their elim_order
                # entries (a player must appear exactly once in the
                # ranking / finishing order).
                alive = [p for p, _ in newly]
                for p, _ in newly:
                    if p.id in elim_order:
                        elim_order.remove(p.id)
                await asyncio.sleep(1.5)
                continue
            if len(alive) == 1:
                break
            digits += 1
            await asyncio.sleep(1.5)
        # Ranking, best first: survivors (fastest total recall first),
        # then the eliminated in reverse elimination order (the most
        # recently eliminated did best of the eliminated). The last
        # entry is the challenge's last place: the first player
        # eliminated, or — if the round cap was hit with nobody left
        # to eliminate — the slowest survivor.
        if len(alive) == 1:
            winner = alive[0]
            ranking_ids = [winner.id] + list(reversed(elim_order))
        else:
            by_time = sorted(alive, key=lambda p: recall_time.get(p.id, 0))
            ranking_ids = [p.id for p in by_time] + list(reversed(elim_order))
            winner = by_time[0]
        winner.score = 1000
        if challenge:
            return ranking_ids, elim_order
        room.phase = "results"
        await broadcast(room, {"type": "results", "game": "memory",
                               "mode": room.mode,
                               "teams": team_standings(room),
                               "rankings": rankings(room)})
        await broadcast_lobby(room)
        return None
    except asyncio.CancelledError:
        return (None, None) if challenge else None


async def run_secret_game(room: Room):
    """Secret Word / Impostor (v2).

    1 impostor with 4-5 players, 2 with 6-8. Non-impostors privately
    see the same secret word; impostors see only that they are the
    impostor. One clue round (each player gives one word), then a
    vote: most votes is revealed — if they are an impostor the word
    team wins (+1000 each), otherwise the impostors win (+1500 each).
    A tie in votes means nobody is eliminated and the impostors win.
    """
    players = list(room.players)
    n_impostors = 1 if len(players) <= 5 else 2
    word = random.choice(list(SECRET_WORDS))
    impostors = set(random.sample([p.id for p in players], n_impostors))
    room.round_state = {"participants": {p.id for p in players},
                        "secret": {}, "impostors": impostors, "word": word,
                        "clues": {}, "votes": {}}
    for p in players:
        is_imp = p.id in impostors
        room.round_state["secret"][p.id] = None if is_imp else word
        if not p.is_bot:
            await send(p, {"type": "secret_word", "word": None if is_imp else word,
                           "is_impostor": is_imp,
                           "impostor_count": n_impostors})
    # --- Clue round: one word per player, in join order. ---
    used_clues = set()
    for p in players:
        if p.is_bot:
            await asyncio.sleep(random.uniform(0.5, 1.5))
            if p.id in impostors:
                clue = random.choice(VAGUE_CLUES)
            else:
                pool = [c for c in SECRET_WORDS[word] if c not in used_clues]
                clue = random.choice(pool or SECRET_WORDS[word])
            room.round_state["clues"][p.id] = clue
            used_clues.add(clue)
        else:
            room.round_state["waiting_clue"] = p.id
            room.round_state["clue_event"] = asyncio.Event()
            await broadcast(room, {"type": "secret_clue_turn",
                                   "player_id": p.id, "nickname": p.nickname})
            try:
                await asyncio.wait_for(
                    room.round_state["clue_event"].wait(), 30)
            except asyncio.TimeoutError:
                room.round_state["clues"][p.id] = "thing"
                used_clues.add("thing")
            room.round_state.pop("waiting_clue", None)
        if room.phase != "playing":
            return
        clue = room.round_state["clues"].get(p.id)
        if clue:
            await broadcast(room, {"type": "secret_clue_shown",
                                   "player_id": p.id, "nickname": p.nickname,
                                   "clue": clue})
    # --- Vote round: who is the impostor? ---
    await broadcast(room, {"type": "secret_vote_start",
                           "players": [{"player_id": p.id, "nickname": p.nickname,
                                        "colour": p.colour, "is_bot": p.is_bot}
                                       for p in players]})
    for p in players:
        if p.is_bot:
            await asyncio.sleep(random.uniform(0.5, 2.0))
            clues = room.round_state["clues"]
            vague = [pid for pid, c in clues.items()
                     if c in VAGUE_CLUES and pid != p.id]
            pool = vague or [q.id for q in players if q.id != p.id]
            room.round_state["votes"][p.id] = random.choice(pool)
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        humans = [p for p in players if not p.is_bot and p.connected]
        if all(p.id in room.round_state["votes"] for p in humans):
            break
        await asyncio.sleep(0.1)
    eliminated, word_team_won = secret_tally(room.round_state["votes"],
                                             impostors)
    if word_team_won:
        for p in players:
            if p.id not in impostors:
                p.score += 1000
        winners = [p.id for p in players if p.id not in impostors]
    else:
        for p in players:
            if p.id in impostors:
                p.score += 1500
        winners = sorted(impostors)
    await broadcast(room, {"type": "secret_result", "word": word,
                           "impostors": sorted(impostors),
                           "eliminated": eliminated,
                           "word_team_won": word_team_won,
                           "winners": winners})
    room.phase = "results"
    await broadcast(room, {"type": "results", "game": "secret",
                           "mode": room.mode,
                           "teams": team_standings(room),
                           "rankings": rankings(room)})
    await broadcast_lobby(room)


async def send_president_state(room: Room):
    """Send every human their President state: their own hand (private)
    plus the public trick, turn, card counts and finish order."""
    st = room.round_state
    hands = st.get("hands", {})
    for p in room.players:
        if p.is_bot:
            continue
        await send(p, {"type": "president_state",
                       "turn_id": st.get("turn"),
                       "trick": st.get("trick"),
                       "last_played_by": st.get("last_played_by"),
                       "counts": {pid: len(h) for pid, h in hands.items()},
                       "finished": st.get("finished", []),
                       "hand": sorted(hands.get(p.id, []), key=pres_rank),
                       "can_pass": st.get("trick") is not None,
                       # v2.1: the client counts the turn down from
                       # this, so the player can see the timeout coming.
                       "turn_seconds": int(room.game_settings["president"]
                                           .get("turn_seconds", 30))})


async def run_president_game(room: Room):
    """President / Ditchdiggers / Asshole (v2).

    Standard 52-card deck, all cards dealt. The player with the 3 of
    clubs leads. Ranks 3 (low) to 2 (high); suits don't matter except
    for the lead. On your turn play a set of 1-4 cards of the same
    rank that beats the current trick (same size, higher rank), or
    pass. When everyone else passes, the last player to play leads a
    new trick. First out is President, last is Asshole. Points by
    finish order on a linear 1000-to-0 scale.
    """
    players = list(room.players)
    deck = build_president_deck()
    hands = {p.id: [] for p in players}
    for i, card in enumerate(deck):
        hands[players[i % len(players)].id].append(card)
    for h in hands.values():
        h.sort(key=pres_rank)
    leader = next(p for p in players if "3♣" in hands[p.id])
    room.round_state = {"participants": {p.id for p in players},
                        "hands": hands, "trick": None, "turn": leader.id,
                        "last_played_by": None, "passed": set(),
                        "finished": [], "turn_event": asyncio.Event(),
                        "turn_action": None}
    turn_seconds = int(room.game_settings["president"].get("turn_seconds", 30))
    try:
        while len(room.round_state["finished"]) < len(players):
            st = room.round_state
            current = room.get_player(st["turn"])
            if current is None or current.id in st["finished"]:
                # Turn holder finished (played their last cards) — move on.
                st["turn"] = _next_president_turn(room, current.id if current else None)
                continue
            await send_president_state(room)
            st["turn_event"] = asyncio.Event()
            st["turn_action"] = None
            if current.is_bot:
                await asyncio.sleep(random.uniform(1.0, 2.0))
                play = president_lowest_legal(hands[current.id], st["trick"])
                if play:
                    _president_apply_play(room, current, play)
                else:
                    _president_apply_pass(room, current)
            else:
                timed_out = False
                try:
                    await asyncio.wait_for(st["turn_event"].wait(), turn_seconds)
                except asyncio.TimeoutError:
                    timed_out = True
                action = st.get("turn_action")
                if action is None:
                    # Timeout: lead with the lowest set, or pass.
                    # v2.1: say so out loud — a silent auto-play looked
                    # like a "random card" being played for no reason.
                    if timed_out:
                        if st["trick"] is None:
                            what = "their lowest card was played automatically"
                        else:
                            what = "they passed automatically"
                        await broadcast(room, {
                            "type": "president_notice",
                            "message": "%s ran out of time (%ds) — %s."
                            % (current.nickname, turn_seconds, what)})
                    if st["trick"] is None:
                        play = president_lowest_legal(hands[current.id], None)
                        if play:
                            _president_apply_play(room, current, play)
                    else:
                        _president_apply_pass(room, current)
            if room.phase != "playing":
                return
            # One player left holding cards: they are the Asshole, the
            # game is over — no need for them to take another turn.
            if len(st["finished"]) >= len(players) - 1:
                break
            # Advance the turn.
            nxt = _next_president_turn(room, current.id)
            if nxt is None:
                break
            st["turn"] = nxt
        # One player left holding cards: they are the Asshole.
        st = room.round_state
        remaining = [p for p in players if p.id not in st["finished"]]
        for p in remaining:
            st["finished"].append(p.id)
        scores = president_scores_for(len(players))
        for i, pid in enumerate(st["finished"]):
            pl = room.get_player(pid)
            if pl:
                pl.score = scores[i]
        room.phase = "results"
        await broadcast(room, {"type": "results", "game": "president",
                               "mode": room.mode,
                               "teams": team_standings(room),
                               "rankings": rankings(room)})
        await broadcast_lobby(room)
    except asyncio.CancelledError:
        pass


def _president_apply_play(room: Room, player: Player, cards):
    """Apply a legal President play: remove the cards, set the trick,
    clear passes, and record a finish if the hand is now empty."""
    st = room.round_state
    for c in cards:
        st["hands"][player.id].remove(c)
    st["trick"] = {"size": len(cards), "rank": pres_rank(cards[0]),
                   "cards": list(cards), "by": player.id}
    st["last_played_by"] = player.id
    st["passed"] = set()
    if not st["hands"][player.id]:
        st["finished"].append(player.id)


def _president_apply_pass(room: Room, player: Player):
    """Apply a pass. If everyone else has now passed, the last player
    to play leads a new trick."""
    st = room.round_state
    st["passed"].add(player.id)
    in_game = [p for p in room.players if p.id not in st["finished"]]
    still_to_act = [p for p in in_game
                    if p.id not in st["passed"] and p.id != st["last_played_by"]]
    if not still_to_act and st["last_played_by"]:
        # Trick is over: the last player to play leads again.
        st["trick"] = None
        st["passed"] = set()
        st["turn"] = st["last_played_by"]


def _next_president_turn(room: Room, current_id):
    """The next player to act. Returns None if the game is over.

    Leading a new trick (trick is None): the last player to play
    leads — or, if they finished by playing their last cards, the
    next player after them in join order. Mid-trick: the next player
    after the current one, skipping finished and passed players.
    """
    st = room.round_state
    players = room.players
    if len(st["finished"]) >= len(players):
        return None
    if st["trick"] is None and st.get("last_played_by"):
        leader = room.get_player(st["last_played_by"])
        if leader and leader.id not in st["finished"]:
            return leader.id
        if leader:
            for k in range(1, len(players) + 1):
                p = players[(players.index(leader) + k) % len(players)]
                if p.id not in st["finished"]:
                    return p.id
            return None
    idx = 0
    if current_id:
        for i, p in enumerate(players):
            if p.id == current_id:
                idx = i + 1
                break
    for k in range(len(players)):
        p = players[(idx + k) % len(players)]
        if p.id in st["finished"] or p.id in st["passed"]:
            continue
        return p.id
    return None


async def send_uno_state(room: Room):
    """Send every human their UNO state: their own hand (private) plus
    the public top card, current colour, direction and card counts."""
    st = room.round_state
    hands = st.get("hands", {})
    for p in room.players:
        if p.is_bot:
            continue
        await send(p, {"type": "uno_state",
                       "turn_id": st.get("turn"),
                       "direction": st.get("direction", 1),
                       "top": st.get("discard", [None])[-1],
                       "current_colour": st.get("current_colour"),
                       "counts": {pid: len(h) for pid, h in hands.items()},
                       "hand": list(hands.get(p.id, [])),
                       "drawn": st.get("drawn", {}).get(p.id)})


def _uno_draw_cards(room: Room, player: Player, n: int):
    """Draw n cards for a player, reshuffling the discard pile (keeping
    the top card) if the draw pile runs out."""
    st = room.round_state
    for _ in range(n):
        if not st["draw"]:
            top = st["discard"].pop()
            st["draw"] = st["discard"]
            st["discard"] = [top]
            random.shuffle(st["draw"])
        st["hands"][player.id].append(st["draw"].pop())


async def run_uno_game(room: Room):
    """UNO (v2).

    Standard 108-card deck, 7 cards each. Match by colour, number or
    symbol; Wilds are playable anytime and the player picks the next
    colour. Draw 2: next player draws 2 and loses their turn. Skip:
    next player loses their turn. Reverse: reverses direction (with 2
    players it acts as a Skip). Wild Draw 4: next player draws 4 and
    loses their turn. No stacking, no Wild Draw 4 challenge. If the
    draw pile runs out, the discard pile is reshuffled (keeping the
    top card). First player to empty their hand wins (+1000); the
    results list everyone by cards remaining, fewest first.
    """
    players = list(room.players)
    deck = build_uno_deck()
    hands = {p.id: [deck.pop() for _ in range(7)] for p in players}
    discard = [deck.pop()]
    while uno_value(discard[-1]) == "draw4":
        deck.append(discard.pop())
        random.shuffle(deck)
        discard.append(deck.pop())
    top = discard[-1]
    current_colour = uno_colour(top)
    if current_colour is None:  # a Wild on top: pick a colour at random
        current_colour = random.choice(UNO_COLOURS)
    direction = 1
    turn_idx = 0
    pending_draw = 0
    skip_next = False
    # Apply the starting card's effect to the first player.
    if uno_value(top) == "skip":
        skip_next = True
    elif uno_value(top) == "draw2":
        pending_draw = 2
    elif uno_value(top) == "reverse":
        direction = -1
        if len(players) == 2:
            skip_next = True
    room.round_state = {"participants": {p.id for p in players},
                        "hands": hands, "draw": deck, "discard": discard,
                        "current_colour": current_colour,
                        "direction": direction, "turn": players[0].id,
                        "turn_event": asyncio.Event(), "turn_action": None,
                        "drawn": {}}
    turn_seconds = int(room.game_settings["uno"].get("turn_seconds", 30))
    winner = None
    try:
        while winner is None:
            st = room.round_state
            current = room.get_player(st["turn"])
            if current is None:
                break
            # Pending draw: this player draws and loses their turn.
            if pending_draw:
                _uno_draw_cards(room, current, pending_draw)
                pending_draw = 0
                st["turn"] = players[(players.index(current) + st["direction"]) % len(players)].id
                await send_uno_state(room)
                await asyncio.sleep(0.8)
                continue
            if skip_next:
                skip_next = False
                st["turn"] = players[(players.index(current) + st["direction"]) % len(players)].id
                await send_uno_state(room)
                await asyncio.sleep(0.8)
                continue
            await send_uno_state(room)
            st["turn_event"] = asyncio.Event()
            st["turn_action"] = None
            st["drawn"] = {}
            if current.is_bot:
                await asyncio.sleep(random.uniform(1.0, 2.0))
                card = uno_bot_choice(hands[current.id], st["discard"][-1],
                                      st["current_colour"])
                if card:
                    colour = (uno_best_colour(
                        [c for c in hands[current.id] if c != card])
                        if card.startswith("W") else None)
                    _uno_apply_play(room, current, card, colour)
                else:
                    _uno_draw_cards(room, current, 1)
                    drawn = hands[current.id][-1]
                    if uno_match(drawn, st["discard"][-1], st["current_colour"]):
                        colour = (uno_best_colour(hands[current.id][:-1])
                                  if drawn.startswith("W") else None)
                        _uno_apply_play(room, current, drawn, colour)
            else:
                # Human turn. They may play a card, or draw one. If the
                # drawn card matches, they may play it immediately or
                # end their turn — so this waits in a small loop until
                # the turn is over. A timeout on the first action draws
                # a card (and plays it if it matches); a timeout while
                # a drawn card is on offer just ends the turn.
                offered = False
                while True:
                    if st.get("turn_action") is None:
                        st["turn_event"] = asyncio.Event()
                        try:
                            await asyncio.wait_for(
                                st["turn_event"].wait(), turn_seconds)
                        except asyncio.TimeoutError:
                            pass
                    action = st.get("turn_action")
                    st["turn_action"] = None
                    if action is None:
                        if offered:
                            break  # timed out on the draw offer: end turn
                        _uno_draw_cards(room, current, 1)
                        drawn = hands[current.id][-1]
                        if uno_match(drawn, st["discard"][-1],
                                     st["current_colour"]):
                            _uno_apply_play(room, current, drawn,
                                            random.choice(UNO_COLOURS)
                                            if drawn.startswith("W") else None)
                        break
                    if action[0] == "play":
                        break  # the handler already applied the play
                    if action[0] == "draw":
                        _uno_draw_cards(room, current, 1)
                        drawn = hands[current.id][-1]
                        if uno_match(drawn, st["discard"][-1],
                                     st["current_colour"]):
                            # Offer the drawn card: the state shows it,
                            # and the player may play it or end the turn.
                            st["drawn"] = {current.id: drawn}
                            offered = True
                            await send_uno_state(room)
                            continue
                        break
                    if action[0] == "end_turn":
                        break
            if room.phase != "playing":
                return
            if not hands[current.id]:
                winner = current
                break
            # Advance to the next player.
            nxt_idx = (players.index(current) + st["direction"]) % len(players)
            if st.get("skip_next"):
                st["skip_next"] = False
                nxt_idx = (nxt_idx + st["direction"]) % len(players)
            st["turn"] = players[nxt_idx].id
            if st.get("pending_draw"):
                pending_draw = st.pop("pending_draw")
        for p in players:
            p.score = 1000 if p is winner else 0
        rows = []
        for p in players:
            rows.append({"player_id": p.id, "nickname": p.nickname,
                         "colour": p.colour, "is_bot": p.is_bot,
                         "score": p.score, "avg_ms": None, "best_ms": None,
                         "false_starts": 0, "team": room.teams.get(p.id),
                         "cards_left": len(room.round_state["hands"][p.id])})
        rows.sort(key=lambda r: (-r["score"], r["cards_left"]))
        room.phase = "results"
        await broadcast(room, {"type": "results", "game": "uno",
                               "mode": room.mode,
                               "teams": team_standings(room),
                               "rankings": rows})
        await broadcast_lobby(room)
    except asyncio.CancelledError:
        pass


def _uno_apply_play(room: Room, player: Player, card: str, colour=None):
    """Apply an UNO play: remove the card, put it on the discard pile,
    and set any effect (skip / reverse / draw 2 / draw 4 / wild colour)
    for the next player."""
    st = room.round_state
    st["hands"][player.id].remove(card)
    st["discard"].append(card)
    value = uno_value(card)
    if card.startswith("W"):
        st["current_colour"] = colour or random.choice(UNO_COLOURS)
        if value == "draw4":
            st["pending_draw"] = 4
    else:
        st["current_colour"] = uno_colour(card)
        if value == "skip":
            st["skip_next"] = True
        elif value == "reverse":
            st["direction"] = -st["direction"]
            if len(room.players) == 2:
                st["skip_next"] = True
        elif value == "draw2":
            st["pending_draw"] = 2


async def run_challenge(room: Room, game: str, alive):
    """Run one Elimination challenge and return its ranking (best
    first) as a list of {"player_id": ...} rows, or None if cancelled."""
    if game == "reaction":
        return await run_reaction_game(room, participants=alive,
                                       challenge=True)
    if game in ("math", "trivia"):
        return await run_quiz_game(room, game, participants=alive,
                                   challenge=True)
    if game == "memory":
        ranking_ids, elim_order = await run_memory_game(
            room, participants=alive, challenge=True,
            challenge_settings=ELIM_CHALLENGE["memory"])
        if not ranking_ids:
            return None
        return [{"player_id": pid} for pid in ranking_ids]
    return None


async def elimination_end_vote(room: Room):
    """Community end vote on the Elimination results screen.

    Humans only (bots don't vote). At least half of connected humans
    voting Play Again starts a NEW elimination series with the same
    players; at least half voting Return to Lobby (or a 60s timeout)
    goes back to the lobby. The host has NO override here — the
    community decides. Returns True (play again), False (lobby) or
    None (cancelled).
    """
    room.round_state = {"end_votes": {"again": set(), "lobby": set()}}
    needed = votes_needed_humans(room)
    await broadcast(room, {"type": "elimination_end_vote",
                           "votes_again": 0, "votes_lobby": 0,
                           "votes_needed": needed, "timeout": 60})
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        votes = room.round_state.get("end_votes")
        if not votes:
            return None
        if len(votes["again"]) >= needed:
            return True
        if len(votes["lobby"]) >= needed:
            return False
        await asyncio.sleep(0.1)
    return False


async def _elimination_break(room: Room, challenge_no: int, alive,
                             ranking, eliminated_names, remaining,
                             finishing_names):
    """Pacing break between Elimination challenges (v2.1).

    Shows the challenge ranking, who was eliminated, how many players
    remain, and the finishing order so far, then waits for the NEXT
    vote: at least half of the CONNECTED HUMAN players still alive
    vote NEXT, or the 60s timeout, or the host's Skip override. Bots
    do not vote. The series never advances to the next challenge
    until the break ends — this is what stops the mode feeling like
    a blur. `ranking` is the challenge's ranking rows
    ({"player_id": ...}); scores shown are each alive player's score
    from that challenge.
    """
    scores = []
    for p in alive:
        scores.append({"player_id": p.id, "nickname": p.nickname,
                       "colour": p.colour, "is_bot": p.is_bot,
                       "score": p.score, "team": None})
    # The ranking order is the challenge order (best first); the
    # scoreboard sorts by score, so keep the ranking visible too.
    rank_names = []
    for row in ranking:
        pl = room.get_player(row["player_id"])
        rank_names.append(pl.nickname if pl else "?")
    await _vote_phase(room, "elimination", challenge_no, challenge_no,
                      scores=scores, eligible=list(alive),
                      extra={"elim_break": True,
                             "eliminated": eliminated_names,
                             "remaining": remaining,
                             "finishing": finishing_names,
                             "ranking": rank_names})


async def run_elimination_game(room: Room):
    """Elimination mode (v2): a series of quick challenges.

    Each challenge is one quick game picked at random from the pool
    (Reaction, Speed Math, Speed Trivia, Memory) among the remaining
    players; the LAST-PLACE player of that challenge is eliminated.
    The series runs until exactly 3 players remain, then a FINALE
    challenge eliminates the bottom 2 and crowns the top player the
    victor. The results screen lists the full finishing order, then
    a community vote decides Play Again (new series, same players)
    or Return to Lobby. Runs in FFA only — team display is hidden
    for the series and individuals are scored.
    """
    all_players = list(room.players)
    try:
        while True:  # one loop iteration = one full series
            alive = list(all_players)
            elim_order = []
            challenge_no = 0
            while len(alive) > 3:
                challenge_no += 1
                pool = [g for g in ELIM_POOL
                        if GAMES[g]["min_players"] <= len(alive)]
                game = random.choice(pool)
                for p in alive:
                    p.score = 0
                    p.reaction_times = []
                    p.false_starts = 0
                room.current_game = game
                await broadcast(room, {"type": "elimination_challenge",
                                       "challenge": challenge_no,
                                       "game": game,
                                       "game_name": GAMES[game]["name"],
                                       "remaining": len(alive),
                                       "alive": [p.nickname for p in alive],
                                       "finale": False})
                # v2.1: challenge intro — 4 seconds to read what game
                # is coming and who is still in, before it starts.
                await asyncio.sleep(4)
                await broadcast(room, {"type": "game_started", "game": game,
                                       "settings": room.game_settings[game],
                                       "mode": "competition",
                                       "party_mode": "ffa",
                                       "team_count": room.team_count,
                                       "teams": {}})
                ranking = await run_challenge(room, game, alive)
                if ranking is None or room.phase != "playing":
                    return
                last = ranking[-1]
                elim_order.append(last["player_id"])
                alive = [p for p in alive if p.id != last["player_id"]]
                loser = room.get_player(last["player_id"])
                await broadcast(room, {"type": "elimination_eliminated",
                                       "player_id": last["player_id"],
                                       "nickname": loser.nickname if loser else "?",
                                       "remaining": len(alive)})
                # v2.1: pacing break — ranking, who went out, who is
                # left — with a NEXT vote. Nothing advances until the
                # vote passes, the host skips, or the timeout hits.
                await _elimination_break(
                    room, challenge_no, alive, ranking,
                    [loser.nickname if loser else "?"], len(alive),
                    [room.get_player(pid).nickname
                     for pid in reversed(elim_order)
                     if room.get_player(pid)])
                if room.phase != "playing":
                    return
            # --- Finale: 3 remain, one last challenge. ---
            challenge_no += 1
            pool = [g for g in ELIM_POOL
                    if GAMES[g]["min_players"] <= len(alive)]
            game = random.choice(pool)
            for p in alive:
                p.score = 0
                p.reaction_times = []
                p.false_starts = 0
            room.current_game = game
            await broadcast(room, {"type": "elimination_challenge",
                                   "challenge": challenge_no,
                                   "game": game,
                                   "game_name": GAMES[game]["name"],
                                   "remaining": len(alive),
                                   "alive": [p.nickname for p in alive],
                                   "finale": True})
            # v2.1: finale intro — same 4-second breather as every
            # other challenge, so the finale doesn't ambush anyone.
            await asyncio.sleep(4)
            await broadcast(room, {"type": "game_started", "game": game,
                                   "settings": room.game_settings[game],
                                   "mode": "competition", "party_mode": "ffa",
                                   "team_count": room.team_count, "teams": {}})
            ranking = await run_challenge(room, game, alive)
            if ranking is None or room.phase != "playing":
                return
            finishing = [ranking[0]["player_id"], ranking[1]["player_id"],
                         ranking[2]["player_id"]] + list(reversed(elim_order))
            # v2.1: one last pacing break after the finale challenge,
            # before the final results — same NEXT vote as the rest.
            await _elimination_break(
                room, challenge_no, [room.get_player(finishing[0])],
                ranking,
                [room.get_player(pid).nickname
                 for pid in finishing[1:3] if room.get_player(pid)],
                1,
                [room.get_player(pid).nickname
                 for pid in finishing if room.get_player(pid)])
            if room.phase != "playing":
                return
            place_scores = {finishing[0]: 1000, finishing[1]: 500,
                            finishing[2]: 250}
            for p in all_players:
                p.score = place_scores.get(p.id, 0)
            order_rows = []
            for place, pid in enumerate(finishing, start=1):
                pl = room.get_player(pid)
                order_rows.append({"place": place, "player_id": pid,
                                   "nickname": pl.nickname,
                                   "colour": pl.colour, "is_bot": pl.is_bot,
                                   "score": place_scores.get(pid, 0),
                                   "avg_ms": None, "best_ms": None,
                                   "false_starts": 0,
                                   "team": room.teams.get(pid)})
            room.phase = "results"
            await broadcast(room, {"type": "results", "game": "elimination",
                                   "mode": "ffa", "teams": [],
                                   "rankings": order_rows,
                                   "finishing_order": order_rows,
                                   "victor": finishing[0]})
            await broadcast_lobby(room)
            again = await elimination_end_vote(room)
            if again is None:
                return
            if again:
                continue  # new series, same players
            # Community chose Return to Lobby.
            room.phase = "lobby"
            room.current_game = None
            for p in room.players:
                p.score = 0
                p.reaction_times = []
                p.false_starts = 0
            await broadcast_lobby(room)
            return
    except asyncio.CancelledError:
        pass


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
        game = random.choice(RANDOM_GAMES)
    if len(room.players) < min_players_for(room, game):
        await send(player, {"type": "error", "message":
            f"{GAMES[game]['name']} needs at least "
            f"{min_players_for(room, game)} player(s). Add bots or wait for players."})
        return
    if room.mode == "teams" and game != "elimination":
        # v1.2: each team must have at least one player/bot, and the
        # number of teams cannot exceed the number of players+bots.
        # Elimination (v2) runs in FFA only, so it skips this check.
        # v1.2: each team must have at least one player/bot, and the
        # number of teams cannot exceed the number of players+bots.
        if room.team_count > len(room.players):
            await send(player, {"type": "error", "message":
                f"Teams mode needs at least {room.team_count} players/bots "
                f"for {room.team_count} teams — add bots or lower the team count."})
            return
        assigned = {room.teams.get(p.id) for p in room.players}
        empty = [t for t in range(room.team_count) if t not in assigned]
        if empty:
            await send(player, {"type": "error", "message":
                "Teams mode needs at least one player/bot on every team — "
                "assign players to " + ", ".join(TEAM_NAMES[t] for t in empty) +
                ", or use Randomize teams."})
            return
    # Reset scores for a fresh game.
    for p in room.players:
        p.score = 0
        p.reaction_times = []
        p.false_starts = 0
    room.current_game = game
    room.phase = "playing"
    mode = "solo" if len(room.players) == 1 else "competition"
    # Elimination runs in FFA only (v2): team display is hidden for
    # the series and individuals are scored.
    party_mode = "ffa" if game == "elimination" else room.mode
    await broadcast(room, {"type": "game_started", "game": game,
                           "settings": room.game_settings[game],
                           "mode": mode, "party_mode": party_mode,
                           "team_count": room.team_count,
                           "teams": {pid: t for pid, t in room.teams.items()}})
    await broadcast_lobby(room)
    if game == "reaction":
        room.game_task = asyncio.create_task(run_reaction_game(room))
    elif game in ("math", "trivia"):
        room.game_task = asyncio.create_task(run_quiz_game(room, game))
    elif game == "secret":
        room.game_task = asyncio.create_task(run_secret_game(room))
    elif game == "president":
        room.game_task = asyncio.create_task(run_president_game(room))
    elif game == "uno":
        room.game_task = asyncio.create_task(run_uno_game(room))
    elif game == "memory":
        room.game_task = asyncio.create_task(run_memory_game(room))
    elif game == "elimination":
        room.game_task = asyncio.create_task(run_elimination_game(room))


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
        # v1.2: FFA / Teams mode + number of teams (host-only, lobby).
        if "mode" in msg and msg["mode"] in ("ffa", "teams"):
            room.mode = msg["mode"]
        if "team_count" in msg:
            try:
                tc = int(msg["team_count"])
                if 2 <= tc <= 4 and tc <= len(room.players):
                    # Drop assignments to teams that no longer exist.
                    room.teams = {pid: t for pid, t in room.teams.items()
                                  if t is None or t < tc}
                    room.team_count = tc
                elif 2 <= tc <= 4:
                    await send(player, {"type": "error", "message":
                        f"Need at least {tc} players/bots for {tc} teams."})
            except (ValueError, TypeError):
                pass
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
            # v1.2: colours are unique per room — reject a taken colour.
            if msg["colour"] != bot.colour and colour_taken(room, msg["colour"], bot.id):
                await send(player, {"type": "error", "message":
                    "That colour is already taken, pick another."})
                return
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
        try:
            idx = int(msg.get("index")) if msg.get("index") is not None else None
        except (ValueError, TypeError):
            idx = None
        record_answer(room, player, msg.get("answer"), index=idx)

    elif mtype == "secret_clue":
        # v2 Secret Word: one-word clue, only when it is your turn.
        if room.phase != "playing" or room.current_game != "secret":
            return
        st = room.round_state
        if st.get("waiting_clue") != player.id:
            return
        clue = str(msg.get("clue", "")).strip().split()[0][:20] if str(msg.get("clue", "")).strip() else ""
        if not clue:
            return
        st["clues"][player.id] = clue
        ev = st.get("clue_event")
        if ev is not None:
            ev.set()

    elif mtype == "secret_vote":
        # v2 Secret Word: vote for who you think the impostor is.
        if room.phase != "playing" or room.current_game != "secret":
            return
        st = room.round_state
        if "votes" not in st or player.is_bot:
            return
        target = msg.get("player_id")
        if not target or target == player.id or not room.get_player(target):
            return
        st["votes"][player.id] = target

    elif mtype == "president_play":
        # v2 President: play a set of cards on your turn. The handler
        # applies the play (server-validated) and wakes the game loop.
        if room.phase != "playing" or room.current_game != "president":
            return
        st = room.round_state
        if st.get("turn") != player.id or st.get("turn_action") is not None:
            return
        cards = msg.get("cards")
        if not isinstance(cards, list):
            return
        hand = st.get("hands", {}).get(player.id, [])
        # v2.1: send the SPECIFIC reason (size / rank / mixed ranks /
        # not in hand / nothing selected) instead of the old generic
        # "that play isn't legal". Nothing is played, the turn is
        # still theirs, and the client keeps the selection so the
        # player can fix it instead of starting over.
        reason = president_illegal_reason(hand, cards, st.get("trick"))
        if reason is not None:
            await send(player, {"type": "error", "message": reason})
            return
        _president_apply_play(room, player, list(cards))
        st["turn_action"] = ("play", list(cards))
        ev = st.get("turn_event")
        if ev is not None:
            ev.set()

    elif mtype == "president_pass":
        # v2 President: pass on your turn (only when a trick is live).
        if room.phase != "playing" or room.current_game != "president":
            return
        st = room.round_state
        if st.get("turn") != player.id or st.get("turn_action") is not None:
            return
        if st.get("trick") is None:
            await send(player, {"type": "error",
                                "message": "You lead the trick — you have to play a card."})
            return
        _president_apply_pass(room, player)
        st["turn_action"] = ("pass",)
        ev = st.get("turn_event")
        if ev is not None:
            ev.set()

    elif mtype == "uno_play":
        # v2 UNO: play a card on your turn. Wilds need a colour.
        if room.phase != "playing" or room.current_game != "uno":
            return
        st = room.round_state
        if st.get("turn") != player.id or st.get("turn_action") is not None:
            return
        card = msg.get("card")
        hand = st.get("hands", {}).get(player.id, [])
        if card not in hand:
            return
        if not uno_match(card, st["discard"][-1], st["current_colour"]):
            await send(player, {"type": "error",
                                "message": "That card doesn't match the colour or value on top."})
            return
        colour = msg.get("colour")
        if card.startswith("W"):
            if colour not in UNO_COLOURS:
                await send(player, {"type": "error",
                                    "message": "Pick a colour for the Wild card."})
                return
        _uno_apply_play(room, player, card, colour)
        st["turn_action"] = ("play", card)
        ev = st.get("turn_event")
        if ev is not None:
            ev.set()

    elif mtype == "uno_draw":
        # v2 UNO: draw a card on your turn (the game loop draws it and,
        # if it matches, offers it back to you to play or keep).
        if room.phase != "playing" or room.current_game != "uno":
            return
        st = room.round_state
        if st.get("turn") != player.id or st.get("turn_action") is not None:
            return
        st["turn_action"] = ("draw",)
        ev = st.get("turn_event")
        if ev is not None:
            ev.set()

    elif mtype == "uno_end_turn":
        # v2 UNO: keep the drawn card and end your turn.
        if room.phase != "playing" or room.current_game != "uno":
            return
        st = room.round_state
        if st.get("turn") != player.id or st.get("turn_action") is not None:
            return
        if player.id not in st.get("drawn", {}):
            return
        st["turn_action"] = ("end_turn",)
        ev = st.get("turn_event")
        if ev is not None:
            ev.set()

    elif mtype == "memory_answer":
        # v2 Memory: type the number back from memory.
        if room.phase != "playing" or room.current_game != "memory":
            return
        record_memory_answer(room, player, msg.get("answer"))

    elif mtype == "elimination_vote":
        # v2 Elimination end vote (results screen, humans only): Play
        # Again starts a new series, Return to Lobby goes back. The
        # host has no override here — the community decides.
        if room.phase != "results" or room.current_game != "elimination":
            return
        st = room.round_state
        votes = st.get("end_votes")
        if votes is None or player.is_bot:
            return
        choice = msg.get("choice")
        if choice not in ("again", "lobby"):
            return
        votes["again"].discard(player.id)
        votes["lobby"].discard(player.id)
        votes[choice].add(player.id)
        needed = votes_needed_humans(room)
        await broadcast(room, {"type": "elimination_end_vote_update",
                               "votes_again": len(votes["again"]),
                               "votes_lobby": len(votes["lobby"]),
                               "votes_needed": needed})

    elif mtype == "update_profile":
        # v1.1: every human can change their OWN nickname/colour in the
        # lobby (and between games, in the results phase) — not other
        # players', not bots (bots stay host-only via update_bot).
        # Not allowed mid-game.
        if room.phase not in ("lobby", "results"):
            await send(player, {"type": "error",
                                "message": "You can only change your nickname or colour in the lobby."})
            return
        changed = False
        if isinstance(msg.get("nickname"), str) and msg["nickname"].strip():
            nick = msg["nickname"].strip()[:20]
            taken = {p.nickname for p in room.players if p is not player}
            if nick != player.nickname and nick in taken:
                await send(player, {"type": "error",
                                    "message": f"Nickname '{nick}' is already taken in this room."})
                return
            if nick != player.nickname:
                player.nickname = nick
                changed = True
        if isinstance(msg.get("colour"), str) and msg["colour"] in PALETTE:
            # v1.2: colours are unique per room — reject a taken colour.
            if msg["colour"] != player.colour and colour_taken(room, msg["colour"], player.id):
                await send(player, {"type": "error", "message":
                    "That colour is already taken, pick another."})
                return
            if msg["colour"] != player.colour:
                player.colour = msg["colour"]
                changed = True
        if changed:
            await broadcast_lobby(room)

    elif mtype == "vote_next":
        # v1.1 NEXT QUESTION vote. Only counts during a vote break,
        # only once per player per question, humans only (bots auto-vote).
        if room.phase != "playing":
            return
        st = room.round_state
        if "votes" not in st or player.is_bot:
            return
        # v2.1: during an Elimination break only players still alive
        # may vote — eliminated spectators don't count.
        eligible = st.get("vote_eligible")
        if eligible is not None and player.id not in eligible:
            return
        if msg.get("index") is not None:
            try:
                if int(msg["index"]) != st.get("vote_index"):
                    return
            except (ValueError, TypeError):
                return
        if player.id in st["votes"]:
            return
        st["votes"].add(player.id)
        needed = votes_needed_humans(room)
        await broadcast(room, {"type": "vote_update",
                               "index": st.get("vote_index"),
                               "votes": len(st["votes"]),
                               "votes_needed": needed,
                               "voters": sorted(st["votes"])})

    elif mtype == "host_skip":
        # v1.2: the host's NEXT override — advances the vote break
        # immediately, regardless of votes. Host-only, vote break only.
        if not player.is_host:
            await send(player, {"type": "error",
                                "message": "Only the host can skip ahead."})
            return
        st = room.round_state
        if room.phase != "playing" or "votes" not in st:
            return
        st["host_skip"] = True
        await broadcast(room, {"type": "vote_update",
                               "index": st.get("vote_index"),
                               "votes": len(st["votes"]),
                               "votes_needed": votes_needed_humans(room),
                               "voters": sorted(st["votes"]),
                               "host_skipped": True})

    elif mtype == "kick_player":
        # v1.2: the host can kick a human player from the lobby (or
        # the results screen, between games). Not themselves, not bots
        # (bots already have host remove). The kicked client is told
        # and its socket is closed; everyone else gets a fresh lobby.
        if not player.is_host:
            await send(player, {"type": "error",
                                "message": "Only the host can kick players."})
            return
        if room.phase not in ("lobby", "results"):
            await send(player, {"type": "error",
                                "message": "Players can only be kicked in the lobby."})
            return
        target = room.get_player(msg.get("player_id", ""))
        if not target or target.is_bot or target is player:
            return
        cancel_game_task(room)
        await send(target, {"type": "kicked",
                            "message": "You were kicked from the room by the host."})
        if target.ws is not None:
            try:
                await target.ws.close()
            except Exception:
                pass
        _remove_player(room, target)
        if not room.humans():
            asyncio.create_task(schedule_room_deletion(room))
            return
        await broadcast_lobby(room)

    elif mtype == "randomize_teams":
        # v1.2: host randomly assigns every player/bot to a team,
        # balanced round-robin after a shuffle, so every team gets
        # at least one member when there are enough players.
        if not player.is_host or room.phase != "lobby":
            return
        if room.mode != "teams":
            await send(player, {"type": "error",
                                "message": "Switch to Teams mode first."})
            return
        if room.team_count > len(room.players):
            await send(player, {"type": "error", "message":
                f"Need at least {room.team_count} players/bots for "
                f"{room.team_count} teams."})
            return
        shuffled = list(room.players)
        random.shuffle(shuffled)
        room.teams = {p.id: i % room.team_count
                      for i, p in enumerate(shuffled)}
        await broadcast_lobby(room)

    elif mtype == "assign_team":
        # v1.2: host manually assigns one player/bot to a team.
        # team: 0..team_count-1, or null/"alone" for Alone.
        if not player.is_host or room.phase != "lobby":
            return
        target = room.get_player(msg.get("player_id", ""))
        if not target:
            return
        team = msg.get("team")
        if team is None or team == "alone":
            room.teams[target.id] = None
        else:
            try:
                t = int(team)
            except (ValueError, TypeError):
                return
            if 0 <= t < room.team_count:
                room.teams[target.id] = t
            else:
                return
        await broadcast_lobby(room)

    elif mtype == "ping":
        await send(player, {"type": "pong"})

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
            # Rejoin: a human who dropped out is held for a 30s grace
            # period (see remove_lobby_player_after_grace). Reclaim their
            # old seat — same player id, host status and score — instead
            # of creating a duplicate entry. Check this before the
            # room-full check, since their seat still counts.
            # v1.1: match by player_id first (the frontend sends its
            # id on reconnect), then by nickname — so a player who
            # changed their nickname in the lobby still reclaims
            # their seat, score and colour.
            rejoined = None
            pid = first.get("player_id")
            if pid:
                rejoined = next(
                    (p for p in room.players
                     if not p.is_bot and not p.connected and p.id == pid),
                    None)
            if rejoined is None:
                rejoined = next(
                    (p for p in room.players
                     if not p.is_bot and not p.connected
                     and p.nickname == nickname), None)
            if rejoined is None and len(room.players) >= room.max_players:
                await ws.send_text(json.dumps(
                    {"type": "error", "message": "That room is full."}))
                await ws.close()
                return
            if rejoined is not None:
                player = rejoined
                player.ws = ws
                player.connected = True
                player.disconnected_at = None
                # Keep their chosen colour if it is still free; only
                # reassign if someone else took it while they were away.
                if player.colour != colour and colour in {
                        p.colour for p in room.players if p is not player}:
                    pass  # keep existing colour
                elif colour:
                    player.colour = colour
            else:
                if nickname in {p.nickname for p in room.players}:
                    await ws.send_text(json.dumps(
                        {"type": "error",
                         "message": f"Nickname '{nickname}' is already taken in this room. Pick another."}))
                    await ws.close()
                    return
                # v1.2: colours are unique per room. Reject a taken
                # colour with a clear error (the client greys taken
                # colours out, so this should rarely trigger).
                if colour in {p.colour for p in room.players}:
                    await ws.send_text(json.dumps(
                        {"type": "error",
                         "message": "That colour is already taken, pick another."}))
                    await ws.close()
                    return
                player = Player(id=uuid.uuid4().hex[:8], nickname=nickname,
                                colour=colour, ws=ws)
                room.players.append(player)
        else:
            await ws.send_text(json.dumps(
                {"type": "error", "message": "First message must be create_room or join_room."}))
            await ws.close()
            return

        await send(player, {"type": "joined", "code": room.code,
                            "player_id": player.id, "room": room.public()})
        await broadcast_lobby(room)
        # v1.1 resync: a player rejoining mid-game was previously sent
        # to the lobby view with no way back to the current question.
        # Resend the current game state so they land back in the game.
        if room.phase == "playing" and room.current_game:
            await send(player, {"type": "game_started",
                                "game": room.current_game,
                                "settings": room.game_settings[room.current_game],
                                "mode": "solo" if len(room.players) == 1 else "competition",
                                "party_mode": room.mode,
                                "team_count": room.team_count,
                                "teams": {pid: t for pid, t in room.teams.items()}})
            st = room.round_state
            if room.current_game == "reaction":
                if st.get("go_at") is None:
                    await send(player, {"type": "reaction_wait",
                                        "round": st.get("round", 1),
                                        "total": int(room.game_settings["reaction"].get("rounds", 5))})
                else:
                    await send(player, {"type": "reaction_go",
                                        "round": st.get("round", 1),
                                        "total": int(room.game_settings["reaction"].get("rounds", 5))})
            elif room.current_game == "secret":
                # v2: re-send this player's private word/impostor status.
                secret = st.get("secret", {})
                impostors = st.get("impostors", set())
                await send(player, {"type": "secret_word",
                                    "word": secret.get(player.id),
                                    "is_impostor": player.id in impostors,
                                    "impostor_count": len(impostors)})
            elif room.current_game == "president":
                # v2: re-send the current President state (own hand).
                await send_president_state(room)
            elif room.current_game == "uno":
                # v2: re-send the current UNO state (own hand).
                await send_uno_state(room)
            elif room.current_game == "memory":
                # v2: if still in the memorize phase, re-send the number
                # with the time left; in the recall phase, re-send the
                # recall prompt so the player can still guess.
                if st.get("phase") == "memorize" and player.id in st.get("numbers", {}):
                    await send(player, {"type": "memory_show",
                                        "digits": st.get("digits", 4),
                                        "number": st["numbers"][player.id],
                                        "seconds": int(room.game_settings["memory"].get("memorize_seconds", 10)),
                                        "round": st.get("round", 1)})
                elif st.get("phase") == "recall":
                    await send(player, {"type": "memory_recall",
                                        "digits": st.get("digits", 4),
                                        "round": st.get("round", 1),
                                        "time_limit": int(room.game_settings["memory"].get("recall_seconds", 15))})
            elif st.get("question"):
                remaining = 1
                if st.get("start") is not None:
                    tl = int(room.game_settings[room.current_game].get("time_per_question", 15))
                    remaining = max(1, tl - int(time.monotonic() - st["start"]))
                await send(player, {"type": "question",
                                    "game": room.current_game,
                                    "index": st.get("index", 1),
                                    "total": int(room.game_settings[room.current_game].get("questions", 10)),
                                    "text": st["question"]["text"],
                                    "options": st["question"]["options"],
                                    "time_limit": remaining})

        while True:
            msg = json.loads(await ws.receive_text())
            await handle_message(room, player, msg)
    except WebSocketDisconnect:
        pass
    except Exception:
        pass
    finally:
        # Graceful disconnect handling (v1.2).
        if room is not None and player is not None:
            player.disconnected_at = time.monotonic()
            player.connected = False
            player.ws = None
            # v1.2: EVERY disconnect schedules cleanup, in every phase.
            # The old code only scheduled removal for lobby disconnects
            # and scheduled nothing for results/mid-game disconnects —
            # that is why a nickname stayed taken for minutes after a
            # game ended (LOM9NZ). The cleanup waits out the grace,
            # waits for any running game to finish, then removes the
            # player and frees their nickname + colour.
            asyncio.create_task(schedule_disconnect_cleanup(room, player))
            if room.phase in ("playing", "results"):
                # Mid-game / between games: freeze their score; the
                # round is held for the full timer (v1.1 wait loops),
                # so a reconnecting player can still answer/click.
                # Promote a new host immediately so the game can go on.
                if player.is_host:
                    player.is_host = False
                    for p in room.players:
                        if not p.is_bot and p.connected:
                            p.is_host = True
                            break
                if not any(p.connected for p in room.humans()):
                    # v1.2: nobody left connected — do NOT delete the
                    # room on the spot (that is what wiped SNYN7F).
                    # Stop the game, drop back to the lobby, and hold
                    # the room for ROOM_EMPTY_GRACE so the host can
                    # reconnect and the room survives a brief drop.
                    cancel_game_task(room)
                    room.phase = "lobby"
                    room.current_game = None
                    asyncio.create_task(schedule_room_deletion(room))
                    return
            await broadcast_lobby(room)
