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
    president_exchange {cards}    # v2.2 President: cards to give back in the exchange
    uno_play    {card, colour?}   # v2 UNO: play a card (colour for Wilds)
                                  # v2.2: also {cards: [...]} for multi-play
    uno_draw    {}                # v2 UNO: draw a card
    uno_end_turn {}               # v2.2 UNO: pass without drawing, end turn
    memory_answer {answer}        # v2 Memory: type the number back
    elimination_vote {choice}     # v2 Elimination end vote: "again" | "lobby"
    ping        {}                # keep-alive; server replies {"type": "pong"}
    replay_vote {choice}          # v2.2 end-of-game choice: "again" | "lobby"
    host_replay {choice}          # v2.2 host override for the replay / Elimination end vote
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

# v2.2 Secret Word clue data (written by a parallel worker). Fall back
# to the in-file word bank if the module is absent, so the server
# still boots; the real file provides 30-40 clues/word + 80 vague words.
try:
    from secret_clues import SECRET_CLUES, SECRET_CATEGORY, VAGUE_CLUES_80
except Exception:  # pragma: no cover - secret_clues.py is normally present
    SECRET_CLUES = {}
    SECRET_CATEGORY = {}
    VAGUE_CLUES_80 = []

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
BOT_NAMES = ["Bot #1", "Bot #2", "Bot #3", "Bot #4", "Bot #5",
             "Bot #6", "Bot #7", "Bot #8"] # Matthew changed bot names to bot numbers.
DIFFICULTIES = ["Easy", "Medium", "Hard"]

# Bot behaviour per difficulty.
# reaction: (min_ms, max_ms) simulated reaction time; false_start_chance.
# quiz:    (min_delay_s, max_delay_s, accuracy) for Math/Trivia answers.
BOT_REACTION = {
    "Easy":   {"ms": (390, 590), "false_start": 0.15}, # Matthew changed (380, 600) reaction to (390, 590) reaction.
    "Medium": {"ms": (375, 530), "false_start": 0.05}, # Matthew changed (250, 380) reaction to (375, 530) reaction.
    "Hard":   {"ms": (320, 430), "false_start": 0.0}, # Matthew changed (180, 280) reaction to (320, 430) reaction.
}
BOT_QUIZ = {
    "Easy":   {"delay": (4.0, 8.0), "accuracy": 0.55}, # Matthew changed (3.0, 8.0) delay to (4.0, 8.0) delay.
    "Medium": {"delay": (3.0, 6.0), "accuracy": 0.75}, # Matthew changed (2.0, 5.0) delay to (3.0, 6.0) delay.
    "Hard":   {"delay": (2.0, 4.0), "accuracy": 0.92}, # Matthew changed (1.0, 3.0) delay to (2.0, 4.0) delay.
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
    "secret":   {"impostors": 1},
    "president": {"turn_seconds": 30},
    "uno":      {"turn_seconds": 30, "draw_until_playable": False,
                 "stacking": False, "multi_play": False},
    "memory":   {"memorize_seconds": 10, "recall_seconds": 15},
    # v2.1: the between-challenge NEXT vote break uses this timeout
    # (tests override it to a few seconds).
    # v2.3 item 14: Elimination "mode" — "normal" (default) or
    # "group" (Group Elimination mode).
    "elimination": {"vote_timeout": 60, "mode": "normal"},
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

# v2.2: fill the secret_clues fallbacks from the in-file bank when the
# module above was absent (SECRET_WORDS is defined just above this
# point, in bank order Animals/Food/Places/Objects/Jobs).
if not SECRET_CLUES:
    SECRET_CLUES = {w: list(c) for w, c in SECRET_WORDS.items()}
if not SECRET_CATEGORY:
    _cats = (["Animals"] * 16 + ["Food"] * 16 + ["Places"] * 16
             + ["Objects"] * 16 + ["Jobs"] * 8)
    SECRET_CATEGORY = {w: c for w, c in zip(SECRET_WORDS, _cats)}
if not VAGUE_CLUES_80:
    VAGUE_CLUES_80 = list(VAGUE_CLUES)

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
    reaction_times: list = field(default_factory=list)  # ms, one entry per round played (1000 = false-start / too-slow penalty, v2.6 item 5)
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
    # v2.2 room state
    turn_order_mode: str = "random"   # "random" | "manual"
    manual_order: list = field(default_factory=list)  # host-set player ids
    turn_order: list = field(default_factory=list)    # order used this game
    team_scoring: str = "average"    # "average" | "sum" | "best"
    championship: bool = False
    # v2.6 item 3: Championship forced OFF while Elimination is on
    # is a SUSPENSION, not an end — the previous setting is
    # remembered here and restored when a normal game is picked
    # again. While set, room.championship is False but the champ
    # history / game counter are untouched.
    champ_suspended: bool = False
    champ_games: int = 0
    champ_history: list = field(default_factory=list)
    president_finish_order: list = field(default_factory=list)
    score_at_game_start: dict = field(default_factory=dict)
    # v2.3 item 14 (Group Elimination mode): host-set group per
    # player (0 = Red, 1 = Blue, 2 = Black — 2 is only ever set by
    # the merge, never by the client) and the current phase of a
    # Group series ("groups" | "black").
    elim_groups: dict = field(default_factory=dict)
    elim_phase: str = "groups"
    # v2.4 item 3: persistent per-series Elimination tally
    # {player_id: total challenge points so far}. Challenge points
    # live in p.score only until the next challenge resets it to 0,
    # so the tally is the only record of where everyone stands
    # between games. Reset at each series start, accumulated after
    # each run_challenge returns. Display-only: the worst-team /
    # worst-player picks keep using the just-finished challenge's
    # p.score.
    elim_tally: dict = field(default_factory=dict)
    # v2.4 item 4: the latest elimination_ceremony stage message,
    # kept so a reconnecting player can be re-sent it (the break
    # that follows re-syncs everyone anyway).
    last_ceremony: dict | None = None
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
                "turn_order_mode": self.turn_order_mode,
                "turn_order": list(self.manual_order),
                "team_scoring": self.team_scoring,
                "championship": self.championship,
                # v2.6 item 3: tells the client the championship is
                # suspended (Elimination) rather than off, so it can
                # restore it when a normal game is picked again.
                "championship_suspended": self.champ_suspended,
                "champ_games": self.champ_games,
                "champ_history": self.champ_history,
                # v2.3 item 14 / group protocol: the LOBBY payload
                # includes president_roles whenever a finish order is
                # stored, regardless of the selected game ({} when no
                # finish order exists, so the client shows greyed-out
                # badge slots). president_roles_for itself is NOT
                # changed — it still returns {} unless President is
                # the selected game, which the game itself needs
                # (all-Citizen roles in match 1).
                "president_roles": self._lobby_president_roles(),
                # v2.3 item 14: Group Elimination mode state.
                "elimination_mode": self.game_settings.get(
                    "elimination", {}).get("mode", "normal"),
                "elim_groups": dict(self.elim_groups),
                "elim_phase": self.elim_phase,
                "min_players": GAMES.get(
                    self.current_game or self.game, {"min_players": 1}
                )["min_players"] if self.game != "random" else 1}

    def _lobby_president_roles(self):
        """v2.3 item 14 / group protocol: lobby-payload President
        roles — the stored finish order mapped to titles, with NO
        selected-game gate (unlike president_roles_for, which the
        game itself still uses). {} when no finish order is stored
        (match 1 / never played President); the client renders
        greyed-out badge slots for the {} case."""
        if not self.president_finish_order:
            return {}
        ordered = president_ordered_ids(self)
        if not ordered:
            return {}
        n = len(ordered)
        titles = PRESIDENT_ROLE_MAP.get(
            n, ["President"] + ["Citizen"] * max(0, n - 2) + ["Peasant"]
            if n >= 2 else ["President"])
        return {pid: titles[i] for i, pid in enumerate(ordered)}

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


def compute_turn_order(room: Room):
    """v2.2 §4: the turn order actually used this game. Random = a
    fresh shuffle of the current players; manual = the host's list
    filtered to current players, with newcomers appended in join order."""
    ids = [p.id for p in room.players]
    if room.turn_order_mode == "manual":
        ordered = [pid for pid in room.manual_order if pid in ids]
        ordered += [pid for pid in ids if pid not in ordered]
        return ordered
    shuffled = ids[:]
    random.shuffle(shuffled)
    return shuffled


# v2.2 §6: President role by position in the finish-order list
# (index 0 = first out = President). Titles are the role names.
PRESIDENT_ROLE_MAP = {
    4: ["President", "Citizen", "Citizen", "Peasant"],
    5: ["President", "Vice President", "Citizen", "Vice Peasant", "Peasant"],
    6: ["President", "Vice President", "Citizen", "Citizen",
        "Vice Peasant", "Peasant"],
    7: ["President", "Vice President", "Citizen", "Citizen", "Citizen",
        "Vice Peasant", "Peasant"],
    8: ["President", "Vice President", "Citizen", "Citizen", "Citizen",
        "Citizen", "Vice Peasant", "Peasant"],
}


def president_ordered_ids(room: Room, players=None):
    """v2.2 §6: the stored finish order filtered to current players,
    with newcomers appended at the end (as if they finished last).
    Returns [] when there is no stored finish order (match 1)."""
    if players is None:
        players = room.players
    ids = [p.id for p in players]
    if not room.president_finish_order:
        return []
    ordered = [pid for pid in room.president_finish_order if pid in ids]
    ordered += [pid for pid in ids if pid not in ordered]
    return ordered


def president_roles_for(room: Room, players=None):
    """v2.2 §6: {player_id: role} derived from the stored finish
    order. All Citizen (match 1) when no finish order is stored, and
    {} in the lobby payload unless President is the selected game."""
    if players is None:
        players = room.players
    if room.game != "president" and room.current_game != "president":
        return {}
    ordered = president_ordered_ids(room, players)
    if not ordered:
        return {p.id: "Citizen" for p in players}
    n = len(ordered)
    titles = PRESIDENT_ROLE_MAP.get(
        n, ["President"] + ["Citizen"] * max(0, n - 2) + ["Peasant"]
        if n >= 2 else ["President"])
    return {pid: titles[i] for i, pid in enumerate(ordered)}


def president_titles_for_finish(n: int):
    """Role title for each position of a finished match (position 0
    = first out). Same mapping as the roles above."""
    return PRESIDENT_ROLE_MAP.get(
        n, ["President"] + ["Citizen"] * max(0, n - 2) + ["Peasant"]
        if n >= 2 else ["President"])


def _assign_score(room: Room, player: Player, value: int):
    """v2.2 §10: games that ASSIGN a score (President finish scale,
    UNO finish scale, Memory, Elimination places, Reaction points)
    must ADD to the championship-persisted score instead of
    overwriting it, so the history delta (score - score at game
    start) works. Outside championship mode this is a plain assign."""
    if room.championship:
        player.score = room.score_at_game_start.get(player.id, 0) + value
    else:
        player.score = value


def _record_champ_game(room: Room, game: str):
    """v2.2 §10: at each completed game in championship mode, record
    every player who played: delta = score - score at game start,
    into the row keyed by (player_id, team at game time). New rows
    pad earlier games with 0; switching back reuses the old row."""
    if not room.championship:
        return
    room.champ_games += 1
    n = room.champ_games
    # Every player in the room played the game (round_state's
    # participants can be a subset — e.g. Memory's final round only
    # lists the survivors — so the room list is the authority here).
    played = list(room.players)
    for p in played:
        delta = p.score - room.score_at_game_start.get(p.id, 0)
        team = room.teams.get(p.id) if room.mode == "teams" else None
        row = next((r for r in room.champ_history
                    if r["player_id"] == p.id and r["team"] == team), None)
        if row is None:
            room.champ_history.append({
                "player_id": p.id, "nickname": p.nickname, "team": team,
                "per_game": [0] * (n - 1) + [delta], "tally": delta,
                "created": len(room.champ_history)})
        else:
            while len(row["per_game"]) < n - 1:
                row["per_game"].append(0)
            row["per_game"].append(delta)
            row["tally"] += delta
            row["nickname"] = p.nickname
    # Pad every row to the current game count: a row for a team the
    # player has left (or a player who sat this game out) still gets
    # a 0 for this game, so per_game length == champ_games for all rows.
    for row in room.champ_history:
        while len(row["per_game"]) < n:
            row["per_game"].append(0)


def team_standings(room: Room):
    """Team standings for Teams mode (v1.2, reworked v2.2 §9).

    Team score follows the host-picked `team_scoring` style:
    "average" (default) = sum of member scores / member count,
    "sum" = total of member scores, "best" = best member's score
    (a team with no surviving members totals 0). Eliminated members
    count as 0 in the numerator AND in the denominator of the
    average (they are members of the team; their score is 0).
    Every 'Alone' player (no team assigned) is their own one-person
    team. Returns a list ranked by team score, highest first, of
    {"team": index|None, "name": str, "colour": str,
     "average": float, "score": float, "style": str, "unit": str,
     "members": [player public dicts]}.
    In FFA mode this returns [] (individual rankings are used instead).

    v2.6 item 1: during a Reaction game (phase "playing") points are
    only assigned at game end, so a points-based standing would read
    0 mid-game. While the game is still playing the standings are
    instead computed from the members' running average reaction
    times — the team value is the average of the members' averages
    (Matthew, v2.6), shown as a time in ms, fastest team first.
    Members with no valid reaction times yet are excluded from the
    average; a team with no valid times at all has no value (None).
    The host-picked team_scoring style still applies to the member
    averages (sum = sum of the averages, best = fastest member
    average). Each row carries "unit": "ms" for these time-based
    standings vs "unit": "pts" for points-based ones, so the client
    can render them correctly. Once the game ends (phase "results")
    Reaction points are assigned, so the normal points-based
    standings apply again. The mid-game vote_break carries these
    standings via _vote_phase, which calls team_standings(room).
    """
    if room.mode != "teams":
        return []
    style = room.team_scoring or "average"
    # v2.6 item 1: time-based Reaction standings while the game is
    # still playing (Elimination challenges run FFA, so this only
    # bites in standalone Reaction with Teams on).
    time_based = room.current_game == "reaction" \
        and room.phase == "playing"
    groups = {}
    for p in room.players:
        t = room.teams.get(p.id)
        groups.setdefault(t, []).append(p)

    def member_avg(p):
        return (round(sum(p.reaction_times) / len(p.reaction_times))
                if p.reaction_times else None)

    def team_value(members):
        if time_based:
            avgs = [a for a in (member_avg(m) for m in members)
                    if a is not None]
            if not avgs:
                return None
            if style == "sum":
                return float(sum(avgs))
            if style == "best":
                return float(min(avgs))
            return round(sum(avgs) / len(avgs), 1)
        scores = [m.score for m in members]
        if style == "sum":
            return float(sum(scores))
        if style == "best":
            return float(max(scores)) if scores else 0.0
        return round(sum(scores) / len(scores), 1) if scores else 0.0

    out = []
    for t, members in groups.items():
        val = team_value(members)
        if t is None:
            # Each Alone player is their own team; list them separately
            # so one Alone player's score is not averaged with another's.
            for m in members:
                mv = member_avg(m) if time_based else float(m.score)
                out.append({"team": None, "name": f"{m.nickname} (Alone)",
                            "colour": "#95a5a6", "average": mv,
                            "score": mv, "style": style,
                            "unit": "ms" if time_based else "pts",
                            "members": [m.public()]})
        else:
            out.append({"team": t, "name": TEAM_NAMES[t],
                        "colour": TEAM_COLOURS[t], "average": val,
                        "score": val, "style": style,
                        "unit": "ms" if time_based else "pts",
                        "members": [m.public() for m in members]})
    if time_based:
        # Fastest team first; teams with no valid time yet go last.
        out.sort(key=lambda r: (r["average"] is None, r["average"] or 0))
    else:
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


def uno_bot_choice(hand, top, current_colour, *, pending_type=None,
                   multi_play=False, difficulty="Medium"):
    """The bot's UNO strategy. Returns a list of cards to play (one
    card normally, two for a multi-play pair), or None to draw.

    v2.2 §5: with a pending draw and stacking on, the bot may stack
    a same-type +2/+4 instead of drawing (Easy declines sometimes,
    Hard always stacks — difficulty-flavored probability, no hard
    cutoffs beyond that). With multi-play on, the bot sometimes
    plays a same-value pair when it has one (Hard most likely).
    Otherwise it plays a matching card, preferring number cards and
    saving Wilds for last.
    """
    if pending_type:
        stackable = [c for c in hand if uno_value(c) == pending_type]
        if not stackable:
            return None
        prob = {"Easy": 0.5, "Medium": 0.8, "Hard": 1.0}.get(
            difficulty or "Medium", 0.8)
        return [stackable[0]] if random.random() < prob else None
    matching = [c for c in hand if uno_match(c, top, current_colour)]
    if not matching:
        return None
    if multi_play:
        by_value = {}
        for c in matching:
            by_value.setdefault(uno_value(c), []).append(c)
        pairs = [g for g in by_value.values() if len(g) >= 2]
        if pairs:
            prob = {"Easy": 0.3, "Medium": 0.5, "Hard": 0.7}.get(
                difficulty or "Medium", 0.5)
            if random.random() < prob:
                return pairs[0][:2]
    numbers = [c for c in matching if uno_value(c).isdigit()]
    actions = [c for c in matching if not uno_value(c).isdigit()
               and not c.startswith("W")]
    wilds = [c for c in matching if c.startswith("W")]
    for pool in (numbers, actions, wilds):
        if pool:
            return [pool[0]]
    return None


def uno_validate_play(hand, cards, top, current_colour, *,
                      pending_draw=0, pending_type=None, multi_play=False):
    """v2.2 §5: why a (possibly multi-card) UNO play is illegal, in
    plain English — or None if it is legal. Multi-play: all cards
    the same value, and AT LEAST ONE must match the top card (v2.3
    item 6: the set is legal whichever of its cards the player picked
    first — the apply step reorders so a top-matching card leads).
    Stacking: the play must be the same type as the pending draw (+2
    on +2, +4 on +4)."""
    if not cards:
        return "You haven't selected any cards — pick at least one card to play."
    missing = [c for c in cards if c not in hand]
    if missing:
        return ("That card isn't in your hand anymore (%s) — "
                "your hand may have changed; pick again."
                % ", ".join(missing))
    if len(cards) > 1 and not multi_play:
        return ("Multi-card plays are turned off for this game — "
                "play one card at a time.")
    values = {uno_value(c) for c in cards}
    if len(values) != 1:
        return ("All the cards in a multi-play must have the same "
                "value — you picked different values.")
    if pending_draw > 0:
        if uno_value(cards[0]) != pending_type:
            return ("There's a +%d to take — you can only stack a %s "
                    "on it, or draw the +%d."
                    % (pending_draw,
                       "+2" if pending_type == "draw2" else "+4",
                       pending_draw))
        return None
    if not any(uno_match(c, top, current_colour) for c in cards):
        return "None of the cards match the colour or value on top."
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

    Most votes wins; a tie means nobody is eliminated (v2.2: the
    game continues to the next clue round). Catching an impostor
    means the word team won this vote.
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

def _reaction_points(active):
    """v2.2 §9 interpolation: best average -> 1000, worst -> 0; a
    player with no valid time (disconnected mid-round and never
    recorded one — v2.6 item 5: false starts now record 1000ms, so
    they no longer leave a player with no time) -> 0; all-equal
    averages -> 1000. Returns {player_id: points}; empty when
    nobody has a valid time."""
    avgs = {}
    for p in active:
        avgs[p.id] = (round(sum(p.reaction_times) / len(p.reaction_times))
                      if p.reaction_times else None)
    valid = [a for a in avgs.values() if a is not None]
    if not valid:
        return {}
    best, worst = min(valid), max(valid)
    pts = {}
    for p in active:
        avg = avgs[p.id]
        if avg is None:
            pts[p.id] = 0
        elif best == worst:
            pts[p.id] = 1000
        else:
            pts[p.id] = round(1000 * (worst - avg) / (worst - best))
    return pts


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
            room.round_state["reaction_open"] = True
            await broadcast(room, {"type": "reaction_go",
                                   "round": rnd, "total": total})
            # v2.6 item 5: 1-second limit — any connected player who
            # has not clicked within 1s of the signal gets 1000ms
            # submitted automatically; a later click is ignored
            # (record_reaction keeps only the first click per round).
            # Bots always click in <600ms, so this only bites humans.
            # The reaction_open flag is cleared when the round's wait
            # ends, so a task waking late (e.g. during the vote break)
            # can never submit for a closed round.
            async def reaction_timeout():
                await asyncio.sleep(1.0)
                if (room.phase == "playing"
                        and room.round_state.get("round") == rnd
                        and room.round_state.get("reaction_open")):
                    for p in active:
                        if (p.connected
                                and p.id not in room.round_state.get(
                                    "clicks", {})):
                            record_reaction(room, p, 1000)
            asyncio.create_task(reaction_timeout())
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
            # v2.6 item 5: the round is closed — the 1s auto-submit
            # task must not fire for this round any more.
            room.round_state["reaction_open"] = False
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
            if challenge:
                # v2.2 §3: Elimination challenges no longer skip the
                # vote break — one after every round, including the
                # last (the per-challenge _elimination_break stays too).
                # v2.4 item 3: rows carry the running tally (still 0
                # here — Reaction points are only assigned at the
                # challenge end — but the field is present so the
                # client can render it uniformly).
                await _vote_phase(
                    room, "reaction", rnd, total, eligible=list(active),
                    timeout=int(room.game_settings["elimination"].get(
                        "vote_timeout", VOTE_TIMEOUT_DEFAULT)),
                    scores=_elim_score_rows(room, active))
            elif rnd < total:
                # v2: NEXT vote break between rounds (not after last —
                # the last round's vote is the end-of-game vote).
                await _vote_phase(room, "reaction", rnd, total)
        if challenge:
            # v2.3 item 14: Reaction challenges must assign points —
            # Group mode picks the worst team / worst player by
            # p.score, and without this every total is 0 and the pick
            # is a random tie-break. Same interpolation as a normal
            # game (best average 1000, worst 0). Plain assign, not
            # _assign_score: challenge scores are transient (reset
            # each challenge) and the break shows the challenge
            # points, like quiz challenges.
            pts = _reaction_points(active)
            for p in active:
                p.score = pts.get(p.id, 0)
            return reaction_rankings(active)
        # v2.2 §9: Reaction now scores — points interpolated by
        # average time between the best average (1000) and the worst
        # (0). All-equal averages give everyone 1000; a player with
        # no valid time (disconnected, never recorded one) counts as
        # worst, i.e. 0.
        avgs = {}
        for p in active:
            avgs[p.id] = (round(sum(p.reaction_times)
                                / len(p.reaction_times))
                          if p.reaction_times else None)
        valid = [a for a in avgs.values() if a is not None]
        if valid:
            best, worst = min(valid), max(valid)
            for p in active:
                avg = avgs[p.id]
                if avg is None:
                    pts = 0
                elif best == worst:
                    pts = 1000
                else:
                    pts = round(1000 * (worst - avg) / (worst - best))
                _assign_score(room, p, pts)
        await _end_of_game_flow(room, "reaction",
                                rankings(room, by_avg=True))
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
    """Record one player's click for the current reaction round.

    v2.6 item 5: a false start records 1000ms (and still counts as a
    false start); any real click more than 1s after the signal is
    capped at 1000ms (covers a player who rejoins after a disconnect
    and clicks late). The first click per round wins — later clicks
    are ignored, so the round is closed for that player once a time
    (real or penalty) is in."""
    clicks = room.round_state.setdefault("clicks", {})
    parts = room.round_state.get("participants")
    if parts is not None and player.id not in parts:
        return  # eliminated/spectating in an Elimination challenge
    if player.id in clicks:
        return  # only the first click per round counts
    if false_start:
        ms = 1000
    elif ms is not None:
        ms = min(ms, 1000)
    clicks[player.id] = {"ms": ms, "false_start": false_start}
    if false_start:
        player.false_starts += 1
    if ms is not None:
        player.reaction_times.append(ms)


async def _vote_phase(room: Room, game: str, idx: int, total: int, *,
                      scores=None, extra=None, eligible=None, timeout=None,
                      end_of_game=False, label=None):
    """NEXT vote break (v1.1, v1.2, v2.1, v2.2).

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
    v2.2: works when `room.phase` is "results" (the end-of-game vote
    is a _vote_phase with `end_of_game=True`); `timeout` overrides
    the per-game vote timeout (Elimination challenges use the
    elimination vote timeout); `label` tags the break (e.g.
    "secret_round"). If no eligible connected human exists (e.g. an
    all-bot challenge), the break ends immediately instead of
    waiting out the timeout.
    """
    if timeout is None:
        settings = room.game_settings.get(game, {})
        timeout = int(settings.get("vote_timeout", VOTE_TIMEOUT_DEFAULT))
    room.round_state["votes"] = set()
    room.round_state["vote_index"] = idx
    room.round_state["host_skip"] = False
    if scores is None:
        scores = [{"player_id": pl.id, "nickname": pl.nickname,
                   "colour": pl.colour, "is_bot": pl.is_bot,
                   "score": pl.score,
                   # v2.5 item 3: Reaction's native running scores
                   # (see _elim_score_rows) — None for other games.
                   "avg_ms": (round(sum(pl.reaction_times)
                                    / len(pl.reaction_times))
                              if pl.reaction_times else None),
                   "best_ms": (min(pl.reaction_times)
                               if pl.reaction_times else None),
                   "team": room.teams.get(pl.id)}
                  for pl in room.players]
    if eligible is None:
        room.round_state["vote_eligible"] = None
        eligible_ids = None
    else:
        eligible_ids = {p.id if hasattr(p, "id") else p for p in eligible}
        room.round_state["vote_eligible"] = eligible_ids

    def votes_needed():
        if eligible_ids is None:
            return votes_needed_humans(room)
        alive_humans = [p for p in room.players
                        if not p.is_bot and p.connected
                        and p.id in eligible_ids]
        return max(1, (len(alive_humans) + 1) // 2)

    def eligible_humans():
        if eligible_ids is None:
            return [p for p in room.players
                    if not p.is_bot and p.connected]
        return [p for p in room.players
                if not p.is_bot and p.connected
                and p.id in eligible_ids]

    msg = {"type": "vote_break", "index": idx, "total": total,
           "scores": scores, "votes": 0,
           "votes_needed": votes_needed(), "timeout": timeout,
           "mode": room.mode, "teams": team_standings(room)}
    if end_of_game:
        msg["end_of_game"] = True
    if label:
        msg["label"] = label
    if extra:
        msg.update(extra)
    await broadcast(room, msg)
    if not eligible_humans():
        room.round_state.pop("votes", None)
        room.round_state.pop("vote_index", None)
        room.round_state.pop("host_skip", None)
        room.round_state.pop("vote_eligible", None)
        return
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if room.round_state.get("host_skip"):
            break
        if len(room.round_state.get("votes", set())) >= votes_needed():
            break
        if not eligible_humans():
            break
        await asyncio.sleep(0.1)
    room.round_state.pop("votes", None)
    room.round_state.pop("vote_index", None)
    room.round_state.pop("host_skip", None)
    room.round_state.pop("vote_eligible", None)


async def _await_replay(room: Room):
    """v2.2: the end-of-game replay decision, after `end_choice` has
    been broadcast. Humans vote `replay_vote` ("again" | "lobby");
    >= half of connected humans for either choice decides it, the
    host's `host_replay` overrides immediately, and a 60s timeout
    goes back to the lobby. Returns True for "again", False for
    "lobby". With no connected humans, returns False at once."""
    room.round_state = {"replay_votes": {"again": set(), "lobby": set()},
                        "replay_choice": None}
    needed = votes_needed_humans(room)
    if not any(p.connected for p in room.humans()):
        return False
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        votes = room.round_state.get("replay_votes")
        if votes is None:
            return False
        if room.round_state.get("replay_choice") == "again":
            return True
        if room.round_state.get("replay_choice") == "lobby":
            return False
        if len(votes["again"]) >= needed:
            return True
        if len(votes["lobby"]) >= needed:
            return False
        await asyncio.sleep(0.1)
    return False


async def _end_of_game_flow(room: Room, game: str, rankings_rows,
                            *, results_extra=None, scores_hidden=False):
    """v2.2 uniform game-end flow (§ Game-end flow): record the
    championship game (if on), broadcast `results`, run the
    end-of-game NEXT vote (phase "results"), broadcast `end_choice`
    with the FINAL standings, then the replay vote. On "again" the
    game is replayed via `replay_game`; otherwise the room returns
    to the lobby. Returns True if the game was replayed."""
    if room.championship:
        _record_champ_game(room, game)
    room.phase = "results"
    results_rows = rankings_rows
    if scores_hidden:
        # Secret Word: points are hidden until end_choice — omit the
        # per-player score values from the results rankings.
        results_rows = [{k: v for k, v in r.items() if k != "score"}
                        for r in rankings_rows]
    msg = {"type": "results", "game": game, "mode": room.mode,
           "teams": team_standings(room), "rankings": results_rows,
           "team_scoring": room.team_scoring,
           "championship": room.championship,
           "champ_games": room.champ_games}
    if scores_hidden:
        msg["scores_hidden"] = True
    if results_extra:
        msg.update(results_extra)
    await broadcast(room, msg)
    await broadcast_lobby(room)
    await _vote_phase(room, game, 1, 1, end_of_game=True,
                      scores=[] if scores_hidden else None)
    await broadcast(room, {"type": "end_choice", "game": game,
                           "rankings": rankings_rows,
                           "teams": team_standings(room),
                           "team_scoring": room.team_scoring,
                           "championship": room.championship,
                           "champ_games": room.champ_games,
                           "champ_history": room.champ_history})
    again = await _await_replay(room)
    if again:
        await replay_game(room)
        return True
    room.phase = "lobby"
    room.current_game = None
    if not room.championship:
        for p in room.players:
            p.score = 0
            p.reaction_times = []
            p.false_starts = 0
    await broadcast_lobby(room)
    return False


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
            if challenge:
                # v2.2 §3: Elimination challenges no longer skip the
                # vote break — one after every question, including
                # the last (the per-challenge break stays too).
                # v2.4 item 3: rows carry the running tally.
                await _vote_phase(
                    room, game, idx, total, eligible=list(active),
                    timeout=int(room.game_settings["elimination"].get(
                        "vote_timeout", VOTE_TIMEOUT_DEFAULT)),
                    scores=_elim_score_rows(room, active))
            elif idx < total:
                # v1.1 NEXT QUESTION vote break (not after the last
                # question — that vote is the end-of-game vote).
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
        await _end_of_game_flow(room, game, rankings(room))
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
            # v2.3 item 5: broadcast memory_round_start BEFORE the
            # per-player memory_show sends — the v2.2 client clears
            # the canvas on round_start, so sending it after the
            # shows erased the just-drawn numbers.
            await broadcast(room, {"type": "memory_round_start",
                                   "digits": digits, "round": rnd,
                                   "seconds": memorize,
                                   "alive": [p.id for p in alive]})
            for p in alive:
                if p.is_bot:
                    ok = memory_bot_correct(p.difficulty, digits)
                    room.round_state["bot_answers"][p.id] = (
                        numbers[p.id] if ok else _wrong_number(numbers[p.id]))
                else:
                    await send(p, {"type": "memory_show", "digits": digits,
                                   "number": numbers[p.id],
                                   "seconds": memorize, "round": rnd})
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
                    _assign_score(room, p, 100 * survived[p.id])
                eliminated_ids = {p.id for p, _ in newly}
                alive = [p for p in alive if p.id not in eliminated_ids]
            await broadcast(room, {"type": "memory_round_result",
                                   "round": rnd, "digits": digits,
                                   "survivors": [p.id for p in alive],
                                   "eliminated": [p.id for p, _ in newly],
                                   "numbers": numbers})
            tied = False
            if len(alive) == 0:
                # TIE: everyone out in the same round — the tied players
                # redo the SAME digit-length round with new numbers. They
                # are not actually eliminated, so undo their elim_order
                # entries (a player must appear exactly once in the
                # ranking / finishing order).
                tied = True
                alive = [p for p, _ in newly]
                for p, _ in newly:
                    if p.id in elim_order:
                        elim_order.remove(p.id)
            # v2.2 §3: NEXT vote break after each digit round (and
            # after the final round). In a challenge the break runs
            # even after the deciding round (the per-challenge break
            # stays too); in a normal game the deciding round's vote
            # is the end-of-game vote, so it is skipped here.
            if challenge:
                # v2.4 item 3: rows carry the running tally. Only the
                # still-alive players are listed (as before); the
                # eliminated players' 100-per-round-survived scores
                # are assigned at elimination and feed the tally.
                await _vote_phase(
                    room, "memory", rnd, rnd, eligible=list(alive),
                    timeout=int(room.game_settings["elimination"].get(
                        "vote_timeout", VOTE_TIMEOUT_DEFAULT)),
                    scores=_elim_score_rows(room, alive))
            elif len(alive) > 1:
                await _vote_phase(room, "memory", rnd, rnd)
            else:
                await asyncio.sleep(1.5)
                if len(alive) == 1:
                    break
                digits += 1
                continue
            if len(alive) == 1:
                break
            if not tied:
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
        if challenge:
            # v2.4 item 3: survivors (other than the winner) earn
            # 100 per round survived — previously they scored 0, so
            # an Elimination break after a Memory challenge showed
            # every survivor at 0. Eliminated players already got
            # 100 x survived at elimination (above). The winner's
            # 1000 below overrides the winner's survivor value.
            # Challenge-only: a normal Memory game keeps its
            # winner-1000 / everyone-else-0 scoring.
            for p in alive:
                _assign_score(room, p, 100 * survived[p.id])
        _assign_score(room, winner, 1000)
        if challenge:
            return ranking_ids, elim_order
        await _end_of_game_flow(room, "memory", rankings(room))
        return None
    except asyncio.CancelledError:
        return (None, None) if challenge else None


async def run_secret_game(room: Room):
    """Secret Word / Impostor (v2.2 §8 rework).

    Roles: Citizen / Imposter. Both roles get the category; only
    citizens get the word. Host option `impostors` (1 or 2,
    game_settings.secret.impostors, default 1). With 2 impostors and
    fewer than 5 players the game starts, then ends immediately with
    `secret_ended` + aborted results.

    Loop: clue round (each ALIVE player gives ONE clue, in turn
    order; 30s timeout, default clue "thing"; citizen bots use
    SECRET_CLUES, impostor bots use VAGUE_CLUES_80; exactly one clue
    per bot per round) -> vote (each alive player votes once, cannot
    vote self; bots vote for a vague clue-giver else random) ->
    resolve: most votes eliminated; TIE = nobody out, next clue
    round. Voted-out players SPECTATE (no clues, no votes; they
    still receive public round results).

    End: citizens win when ALL impostors are caught; impostors win
    when only 2 CITIZENS remain alive. Scoring (per round, hidden
    until end_choice): each alive player +200 per round survived; a
    citizen who voted for a wrongly eliminated citizen that round
    gets +100 instead of 200 that round. No win bonus. Points live
    in p.score but are NEVER broadcast mid-game: results carries
    scores_hidden=True with no score values, vote breaks pass empty
    scores; points are revealed only in end_choice.
    """
    players = list(room.players)
    order_ids = room.turn_order or [p.id for p in players]
    ordered = [room.get_player(pid) for pid in order_ids]
    ordered += [p for p in players if p not in ordered]
    ordered = [p for p in ordered if p is not None]
    n_impostors = int(room.game_settings.get("secret", {})
                      .get("impostors", 1))
    n_impostors = 2 if n_impostors == 2 else 1
    word = random.choice(list(SECRET_WORDS))
    category = SECRET_CATEGORY.get(word, "")
    impostors = set(random.sample([p.id for p in players], n_impostors))
    alive = [p for p in ordered]
    eliminated_ids = []
    room.round_state = {"participants": {p.id for p in players},
                        "secret": {}, "impostors": impostors, "word": word,
                        "category": category, "clues": {}, "votes": {},
                        "alive": {p.id for p in alive},
                        "eliminated": [], "round": 0}
    for p in players:
        is_imp = p.id in impostors
        room.round_state["secret"][p.id] = None if is_imp else word
        if not p.is_bot:
            await send(p, {"type": "secret_word",
                           "word": None if is_imp else word,
                           "category": category,
                           "role": "imposter" if is_imp else "citizen",
                           "is_impostor": is_imp,
                           "impostor_count": n_impostors,
                           "impostors": n_impostors,
                           "round": 1,
                           "alive": [q.id for q in alive],
                           # v2.3: turn order = alive list in clue order.
                           "turn_order": [q.id for q in alive],
                           "eliminated": []})
    # 2 impostors with <5 players: ends immediately.
    if n_impostors == 2 and len(players) < 5:
        await broadcast(room, {"type": "secret_ended",
                               "reason": "too_few_for_two_impostors",
                               "message": "Two impostors need at least "
                               "5 players — the game ends here."})
        await broadcast(room, {"type": "secret_result", "word": word,
                               "category": category,
                               "impostors": sorted(impostors),
                               "eliminated": None,
                               "word_team_won": False,
                               "winners": []})
        await _end_of_game_flow(
            room, "secret", rankings(room), scores_hidden=True,
            results_extra={"aborted": True, "word": word,
                           "category": category,
                           "impostors": sorted(impostors),
                           "word_team_won": False, "winners": []})
        return
    used_clues = set()
    round_no = 0
    word_team_won = False
    last_eliminated = None
    try:
        while True:
            round_no += 1
            room.round_state["round"] = round_no
            room.round_state["clues"] = {}
            room.round_state["votes"] = {}
            # --- Clue round: one clue per ALIVE player, in turn order.
            for p in list(alive):
                if room.phase != "playing":
                    return
                if p.is_bot:
                    await asyncio.sleep(random.uniform(0.5, 1.5))
                    if p.id in impostors:
                        pool = [c for c in VAGUE_CLUES_80
                                if c not in used_clues]
                        clue = random.choice(pool or VAGUE_CLUES_80)
                    else:
                        pool = [c for c in SECRET_CLUES.get(
                            word, SECRET_WORDS[word])
                            if c not in used_clues]
                        clue = random.choice(
                            pool or SECRET_CLUES.get(word, SECRET_WORDS[word]))
                    room.round_state["clues"][p.id] = clue
                    used_clues.add(clue)
                else:
                    room.round_state["waiting_clue"] = p.id
                    room.round_state["clue_event"] = asyncio.Event()
                    await broadcast(room, {"type": "secret_clue_turn",
                                           "player_id": p.id,
                                           "nickname": p.nickname,
                                           # v2.3: turn order = alive list
                                           # in clue order.
                                           "turn_order": [q.id for q in alive],
                                           "round": round_no})
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
                                           "player_id": p.id,
                                           "nickname": p.nickname,
                                           "clue": clue, "round": round_no,
                                           # v2.3: turn order = alive list
                                           # in clue order.
                                           "turn_order": [q.id for q in alive]})
            # --- Vote round: who is the impostor? ---
            room.round_state["votes"] = {}
            await broadcast(room, {"type": "secret_vote_start",
                                   "round": round_no,
                                   "players": [{"player_id": p.id,
                                                "nickname": p.nickname,
                                                "colour": p.colour,
                                                "is_bot": p.is_bot}
                                               for p in alive]})
            for p in list(alive):
                if p.is_bot:
                    await asyncio.sleep(random.uniform(0.5, 2.0))
                    clues = room.round_state["clues"]
                    vague = [pid for pid, c in clues.items()
                             if (c in VAGUE_CLUES_80 or c in VAGUE_CLUES)
                             and pid != p.id]
                    pool = vague or [q.id for q in alive if q.id != p.id]
                    if pool:
                        room.round_state["votes"][p.id] = random.choice(pool)
            deadline = time.monotonic() + 30
            while time.monotonic() < deadline:
                humans = [p for p in alive if not p.is_bot and p.connected]
                if all(p.id in room.round_state["votes"] for p in humans):
                    break
                await asyncio.sleep(0.1)
            votes = dict(room.round_state["votes"])
            # Resolve: most votes eliminated; a tie eliminates nobody.
            counts = {}
            for suspect in votes.values():
                counts[suspect] = counts.get(suspect, 0) + 1
            eliminated = None
            tie = False
            if counts:
                top = max(counts.values())
                leaders = [pid for pid, n in counts.items() if n == top]
                if len(leaders) == 1:
                    eliminated = leaders[0]
                else:
                    tie = True
            last_eliminated = eliminated
            if eliminated is not None:
                eliminated_ids.append(eliminated)
                alive = [p for p in alive if p.id != eliminated]
                room.round_state["alive"] = {p.id for p in alive}
                room.round_state["eliminated"] = list(eliminated_ids)
            # Per-round scoring (hidden): each player still alive
            # after the vote gets +200 for surviving the round; a
            # citizen who voted for a wrongly eliminated citizen
            # gets +100 instead of 200 that round.
            wrong_victim = (eliminated is not None
                            and eliminated not in impostors)
            for p in alive:
                if wrong_victim and votes.get(p.id) == eliminated \
                        and p.id not in impostors:
                    p.score += 100
                else:
                    p.score += 200
            citizens_left = sum(1 for p in alive if p.id not in impostors)
            impostors_left = sum(1 for p in alive if p.id in impostors)
            await broadcast(room, {"type": "secret_round_result",
                                   "round": round_no,
                                   "eliminated": eliminated, "tie": tie,
                                   "votes": votes,
                                   "alive": [p.id for p in alive],
                                   "eliminated_ids": list(eliminated_ids),
                                   "citizens_left": citizens_left,
                                   "impostors_left": impostors_left})
            # End conditions: all impostors caught, or only 2
            # citizens remain alive.
            if impostors_left == 0:
                word_team_won = True
                break
            if citizens_left <= 2:
                word_team_won = False
                break
            # Vote break before the next clue round (scores hidden).
            await _vote_phase(room, "secret", round_no, round_no,
                              scores=[], eligible=list(alive),
                              label="secret_round")
            if room.phase != "playing":
                return
        winners = ([p.id for p in players if p.id not in impostors]
                   if word_team_won else sorted(impostors))
        await broadcast(room, {"type": "secret_result", "word": word,
                               "category": category,
                               "impostors": sorted(impostors),
                               "eliminated": last_eliminated,
                               "word_team_won": word_team_won,
                               "winners": winners})
        await _end_of_game_flow(
            room, "secret", rankings(room), scores_hidden=True,
            results_extra={"word": word, "category": category,
                           "impostors": sorted(impostors),
                           "word_team_won": word_team_won,
                           "winners": winners})
    except asyncio.CancelledError:
        return


async def send_president_state(room: Room):
    """Send every human their President state: their own hand (private)
    plus the public trick, turn, card counts and finish order.

    v2.2: also sends `roles` {player_id: role}, `turn_order` (the
    rotation for this match), `exchange` (the pending card exchange,
    or null), and `titles` (role title per finished player, from the
    server — the client must not compute titles locally)."""
    st = room.round_state
    hands = st.get("hands", {})
    roles = st.get("roles", {})
    finished = st.get("finished", [])
    titles = {}
    finish_titles = president_titles_for_finish(len(room.players))
    for i, pid in enumerate(finished):
        if i < len(finish_titles):
            titles[pid] = finish_titles[i]
    for p in room.players:
        if p.is_bot:
            continue
        await send(p, {"type": "president_state",
                       "turn_id": st.get("turn"),
                       "trick": st.get("trick"),
                       "last_played_by": st.get("last_played_by"),
                       "counts": {pid: len(h) for pid, h in hands.items()},
                       "finished": list(finished),
                       "titles": titles,
                       "roles": dict(roles),
                       "turn_order": list(st.get("order", [])),
                       "exchange": st.get("exchange"),
                       "hand": sorted(hands.get(p.id, []), key=pres_rank),
                       "can_pass": st.get("trick") is not None,
                       # v2.1: the client counts the turn down from
                       # this, so the player can see the timeout coming.
                       "turn_seconds": int(room.game_settings["president"]
                                           .get("turn_seconds", 30))})


async def _president_do_exchange(room: Room, giver_id: str,
                                 partner_id: str, count: int):
    """v2.2 §6 card exchange: `giver` gives `count` cards (their
    choice — human picks via `president_exchange`, bot picks its
    lowest ranks, timeout auto-picks lowest ranks) to `partner`;
    `partner` gives their best `count` cards (highest ranks,
    automatic) back. Waits at most `turn_seconds` for a human giver."""
    st = room.round_state
    hands = st["hands"]
    turn_seconds = int(room.game_settings["president"]
                       .get("turn_seconds", 30))
    giver = room.get_player(giver_id)
    st["exchange"] = {"giver_id": giver_id, "count": count,
                      "partner_id": partner_id}
    st["exchange_event"] = asyncio.Event()
    st["exchange_cards"] = None
    await send_president_state(room)
    cards = None
    if giver is not None and giver.is_bot:
        await asyncio.sleep(random.uniform(0.5, 1.0))
        cards = sorted(hands[giver_id], key=pres_rank)[:count]
    else:
        try:
            await asyncio.wait_for(st["exchange_event"].wait(),
                                   turn_seconds)
        except asyncio.TimeoutError:
            pass
        cards = st.get("exchange_cards")
        if (not cards or len(cards) != count
                or any(c not in hands[giver_id] for c in cards)):
            cards = sorted(hands[giver_id], key=pres_rank)[:count]
    # Partner's best cards are automatic: the highest ranks.
    best = sorted(hands[partner_id], key=pres_rank,
                  reverse=True)[:count]
    for c in cards:
        hands[giver_id].remove(c)
    for c in best:
        hands[partner_id].remove(c)
    hands[giver_id].extend(best)
    hands[partner_id].extend(cards)
    for h in hands.values():
        h.sort(key=pres_rank)
    st["exchange"] = None
    st["exchange_cards"] = None
    st["exchange_event"] = None
    await send_president_state(room)


async def run_president_game(room: Room):
    """President / Ditchdiggers / Asshole (v2, roles v2.2 §6).

    Standard 52-card deck, all cards dealt. Ranks 3 (low) to 2
    (high); suits don't matter. On your turn play a set of 1-4 cards
    of the same rank that beats the current trick (same size, higher
    rank), or pass. v2.3 item 2: one pass per stack — the leader
    plays, then each other unfinished player acts exactly once
    (play or pass); the stack winner (highest play) leads the next
    stack. First out is President, last is Peasant.
    Points by finish order on a linear 1000-to-0 scale (added to the
    championship score in championship mode).

    v2.2: match 1 everyone is a Citizen, turn order is the room turn
    order and the first player in it leads with ANY cards (the old
    3 of clubs lead rule is gone, all matches). Match 2+: roles come
    from `room.president_finish_order`, turn order is that list
    strictly (President first ... Peasant last) and the President
    leads with any cards. After dealing (match 2+ only) the card
    exchange runs: Peasant gives best 2 (1 with no vices) to the
    President, who gives worst 2 (1) back; with vices, VP and Vice
    Peasant also swap best/worst 1. Finish order is stored after
    each match for the next one.
    """
    players = list(room.players)
    ordered_ids = president_ordered_ids(room, players)
    has_roles = bool(ordered_ids)
    if has_roles:
        ordered = [room.get_player(pid) for pid in ordered_ids]
    else:
        order_ids = room.turn_order or [p.id for p in players]
        ordered = [room.get_player(pid) for pid in order_ids]
        ordered += [p for p in players if p not in ordered]
    ordered = [p for p in ordered if p is not None]
    roles = president_roles_for(room, players) if has_roles else \
        {p.id: "Citizen" for p in players}
    deck = build_president_deck()
    hands = {p.id: [] for p in players}
    for i, card in enumerate(deck):
        hands[ordered[i % len(ordered)].id].append(card)
    for h in hands.values():
        h.sort(key=pres_rank)
    leader = ordered[0]
    room.round_state = {"participants": {p.id for p in players},
                        "hands": hands, "trick": None, "turn": leader.id,
                        "last_played_by": None, "passed": set(),
                        "acted": set(),
                        "finished": [], "turn_event": asyncio.Event(),
                        "turn_action": None, "order": [p.id for p in ordered],
                        "roles": dict(roles), "exchange": None}
    turn_seconds = int(room.game_settings["president"].get("turn_seconds", 30))
    try:
        # --- Card exchange (match 2+ only; match 1 has no roles). ---
        if has_roles:
            by_role = {}
            for pid, role in roles.items():
                by_role[role] = pid
            n = len(players)
            n_swap = 1 if n == 4 else 2
            if "President" in by_role and "Peasant" in by_role:
                await _president_do_exchange(
                    room, by_role["President"], by_role["Peasant"], n_swap)
                if room.phase != "playing":
                    return
            if n >= 5 and "Vice President" in by_role \
                    and "Vice Peasant" in by_role:
                await _president_do_exchange(
                    room, by_role["Vice President"],
                    by_role["Vice Peasant"], 1)
                if room.phase != "playing":
                    return
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
            # One player left holding cards: they are the Peasant, the
            # game is over — no need for them to take another turn.
            if len(st["finished"]) >= len(players) - 1:
                break
            # Advance the turn.
            nxt = _next_president_turn(room, current.id)
            if nxt is None:
                break
            st["turn"] = nxt
        # One player left holding cards: they are the Peasant.
        st = room.round_state
        remaining = [p for p in players if p.id not in st["finished"]]
        for p in remaining:
            st["finished"].append(p.id)
        # v2.2 §6: store the finish order for the next match's roles.
        room.president_finish_order = list(st["finished"])
        scores = president_scores_for(len(players))
        for i, pid in enumerate(st["finished"]):
            pl = room.get_player(pid)
            if pl:
                _assign_score(room, pl, scores[i])
        titles = president_titles_for_finish(len(players))
        # v2.3 item 8: send a final president_state after the last
        # finisher is appended, so clients see the Peasant title
        # (in-game the last player was never in `finished` before).
        # Point the turn at the winner — the old turn_id can name a
        # finished player (3rd finisher + stack-end), which the client
        # would render as a stale "your turn / waiting for…".
        if st["finished"]:
            st["turn"] = st["finished"][0]
        await send_president_state(room)
        await _end_of_game_flow(
            room, "president", rankings(room),
            results_extra={"finish_order": list(st["finished"]),
                           "titles": {pid: titles[i]
                                      for i, pid in enumerate(st["finished"])
                                      if i < len(titles)}})
    except asyncio.CancelledError:
        pass


def _president_maybe_end_stack(room: Room):
    """v2.3 item 2: a stack ends once every UNFINISHED player has
    acted exactly once (play or pass) — the per-stack `acted` set.
    Clear the trick and the per-stack sets; the next turn goes to
    the stack winner (`last_played_by`, the highest play) via
    _next_president_turn, or to the next unfinished player after
    them if the winner finished by playing their last cards."""
    st = room.round_state
    unfinished = [p.id for p in room.players
                  if p.id not in st["finished"]]
    acted = st.get("acted", set())
    if unfinished and all(pid in acted for pid in unfinished):
        st["trick"] = None
        st["passed"] = set()
        st["acted"] = set()


def _president_apply_play(room: Room, player: Player, cards):
    """Apply a legal President play: remove the cards, set the trick,
    and record a finish if the hand is now empty.

    v2.3 item 2: one pass per stack — this play is the player's one
    action for the stack (added to `acted`); the stack ends once
    every unfinished player has acted."""
    st = room.round_state
    for c in cards:
        st["hands"][player.id].remove(c)
    st["trick"] = {"size": len(cards), "rank": pres_rank(cards[0]),
                   "cards": list(cards), "by": player.id}
    st["last_played_by"] = player.id
    st.setdefault("acted", set()).add(player.id)
    if not st["hands"][player.id]:
        st["finished"].append(player.id)
    _president_maybe_end_stack(room)


def _president_apply_pass(room: Room, player: Player):
    """Apply a pass: the player's one action for this stack (added
    to `acted`). The stack ends once every unfinished player has
    acted; the last player to play then leads the next stack."""
    st = room.round_state
    st["passed"].add(player.id)
    st.setdefault("acted", set()).add(player.id)
    _president_maybe_end_stack(room)


def _next_president_turn(room: Room, current_id):
    """The next player to act. Returns None if the game is over.

    v2.2: rotation follows `round_state["order"]` — the room turn
    order in match 1, the stored finish order (President first ...
    Peasant last) in match 2+. Leading a new trick (trick is None):
    the last player to play leads — or, if they finished by playing
    their last cards, the next player after them in the rotation.
    v2.3 item 2: one pass per stack — mid-trick, the next player
    after the current one, skipping finished, passed AND already-
    acted players (each unfinished player acts exactly once per
    stack; the `acted` set is cleared when the stack ends).
    """
    st = room.round_state
    order = st.get("order") or [p.id for p in room.players]
    if len(st["finished"]) >= len(order):
        return None
    if st["trick"] is None and st.get("last_played_by"):
        leader_id = st["last_played_by"]
        if leader_id not in st["finished"]:
            return leader_id
        if leader_id in order:
            idx = order.index(leader_id)
            for k in range(1, len(order) + 1):
                pid = order[(idx + k) % len(order)]
                if pid not in st["finished"]:
                    return pid
            return None
    idx = 0
    if current_id and current_id in order:
        idx = order.index(current_id) + 1
    acted = st.get("acted", set())
    for k in range(len(order)):
        pid = order[(idx + k) % len(order)]
        if pid in st["finished"] or pid in st["passed"] or pid in acted:
            continue
        return pid
    return None


async def send_uno_state(room: Room):
    """Send every human their UNO state: their own hand (private) plus
    the public top card, current colour, direction and card counts.

    v2.2: also sends the pending draw total/type (stacking) and the
    house-rule flags, so the client can show "+N — stack or take"."""
    st = room.round_state
    hands = st.get("hands", {})
    settings = room.game_settings.get("uno", {})
    for p in room.players:
        if p.is_bot:
            continue
        await send(p, {"type": "uno_state",
                       "turn_id": st.get("turn"),
                       "turn_order": list(st.get("order") or
                                          [q.id for q in room.players]),
                       "direction": st.get("direction", 1),
                       "top": st.get("discard", [None])[-1],
                       "current_colour": st.get("current_colour"),
                       "counts": {pid: len(h) for pid, h in hands.items()},
                       "hand": list(hands.get(p.id, [])),
                       "drawn": st.get("drawn", {}).get(p.id),
                       "pending_draw": st.get("pending_draw", 0),
                       "pending_type": st.get("pending_type"),
                       "draw_until_playable": bool(
                           settings.get("draw_until_playable")),
                       "stacking": bool(settings.get("stacking")),
                       "multi_play": bool(settings.get("multi_play"))})


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


def _uno_handle_draw(room: Room, player: Player):
    """v2.2 §5 draw rules, shared by humans, bots and timeouts.

    Pending draw (stacking): draw the FULL accumulated total and the
    turn ends. Draw-until-playable ON: draw repeatedly until a
    playable card appears, then that card MUST be played — auto-play
    it (a Wild's colour is the player's most common colour). OFF:
    draw exactly 1; if playable it MUST be played (auto-play, same
    Wild rule); if not playable, the turn ends.
    """
    st = room.round_state
    settings = room.game_settings.get("uno", {})
    if st.get("pending_draw", 0) > 0:
        n = st["pending_draw"]
        _uno_draw_cards(room, player, n)
        st["pending_draw"] = 0
        st["pending_type"] = None
        return
    if settings.get("draw_until_playable"):
        for _ in range(200):  # safety cap; a playable card appears first
            _uno_draw_cards(room, player, 1)
            drawn = st["hands"][player.id][-1]
            if uno_match(drawn, st["discard"][-1], st["current_colour"]):
                colour = (uno_best_colour(st["hands"][player.id])
                          if drawn.startswith("W") else None)
                _uno_apply_play_multi(room, player, [drawn], colour)
                return
        return
    _uno_draw_cards(room, player, 1)
    drawn = st["hands"][player.id][-1]
    if uno_match(drawn, st["discard"][-1], st["current_colour"]):
        colour = (uno_best_colour(st["hands"][player.id])
                  if drawn.startswith("W") else None)
        _uno_apply_play_multi(room, player, [drawn], colour)


def _uno_advance(room: Room, current: Player, steps=None):
    """Move the turn on. `steps` defaults to 1 + the accumulated skip
    count (two skips skip two players); an explicit step count (the
    pending-draw turn) consumes the skip count too. Turn order walks
    `round_state["order"]` (the room turn order, §4)."""
    st = room.round_state
    order = st.get("order") or [p.id for p in room.players]
    if steps is None:
        steps = 1 + st.pop("skip_count", 0)
    else:
        st["skip_count"] = 0
    st["skip_next"] = False
    idx = order.index(current.id) if current.id in order else 0
    nxt = order[(idx + st.get("direction", 1) * steps) % len(order)]
    st["turn"] = nxt


async def run_uno_game(room: Room):
    """UNO (v2, house rules v2.2 §5).

    Standard 108-card deck, 7 cards each. Match by colour, number or
    symbol; Wilds are playable anytime and the player picks the next
    colour. Draw 2: next player draws 2 and loses their turn. Skip:
    next player loses their turn. Reverse: reverses direction (with 2
    players it acts as a Skip). Wild Draw 4: next player draws 4 and
    loses their turn. If the draw pile runs out, the discard pile is
    reshuffled (keeping the top card). Turn order walks the room turn
    order (§4); the first player in it starts.

    House rules (host toggles, defaults OFF): draw-until-playable
    (draw until a playable card appears, then it MUST be played),
    stacking (same-type +2/+4 only; the player who cannot/will not
    stack draws the accumulated total), multi-play (several cards of
    the same value at once; ALL effects trigger; Wild colour picked
    once). Scoring is purely the finishing-order scale by cards left
    (fewest first), no winner bonus.
    """
    order_ids = room.turn_order or [p.id for p in room.players]
    players = [room.get_player(pid) for pid in order_ids]
    players += [p for p in room.players if p not in players]
    players = [p for p in players if p is not None]
    settings = room.game_settings.get("uno", {})
    stacking = bool(settings.get("stacking"))
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
    pending_draw = 0
    pending_type = None
    skip_count = 0
    # Apply the starting card's effect to the first player.
    if uno_value(top) == "skip":
        skip_count = 1
    elif uno_value(top) == "draw2":
        pending_draw = 2
        pending_type = "draw2"
    elif uno_value(top) == "reverse":
        direction = -1
        if len(players) == 2:
            skip_count = 1
    room.round_state = {"participants": {p.id for p in players},
                        "hands": hands, "draw": deck, "discard": discard,
                        "current_colour": current_colour,
                        "direction": direction, "turn": players[0].id,
                        "turn_event": asyncio.Event(), "turn_action": None,
                        "drawn": {}, "pending_draw": pending_draw,
                        "pending_type": pending_type,
                        "skip_count": skip_count, "skip_next": skip_count > 0,
                        "order": [p.id for p in players]}
    turn_seconds = int(settings.get("turn_seconds", 30))
    winner = None
    try:
        while winner is None:
            st = room.round_state
            current = room.get_player(st["turn"])
            if current is None:
                break
            # Pending draw with stacking OFF: draw the total and lose
            # the turn, automatically (as before v2.2).
            if st.get("pending_draw", 0) > 0 and not stacking:
                _uno_draw_cards(room, current, st["pending_draw"])
                st["pending_draw"] = 0
                st["pending_type"] = None
                _uno_advance(room, current, steps=1)
                await send_uno_state(room)
                await asyncio.sleep(0.8)
                continue
            await send_uno_state(room)
            st["turn_event"] = asyncio.Event()
            st["turn_action"] = None
            st["drawn"] = {}
            if current.is_bot:
                await asyncio.sleep(random.uniform(1.0, 2.0))
                hand = hands[current.id]
                if st.get("pending_draw", 0) > 0:
                    # Stacking: the bot may stack or take the total.
                    choice = uno_bot_choice(
                        hand, st["discard"][-1], st["current_colour"],
                        pending_type=st.get("pending_type"),
                        multi_play=bool(settings.get("multi_play")),
                        difficulty=current.difficulty)
                    if choice:
                        colour = (uno_best_colour(
                            [c for c in hand if c not in choice])
                            if any(c.startswith("W") for c in choice)
                            else None)
                        _uno_apply_play_multi(room, current, choice, colour)
                    else:
                        _uno_handle_draw(room, current)
                else:
                    choice = uno_bot_choice(
                        hand, st["discard"][-1], st["current_colour"],
                        multi_play=bool(settings.get("multi_play")),
                        difficulty=current.difficulty)
                    if choice:
                        colour = (uno_best_colour(
                            [c for c in hand if c not in choice])
                            if any(c.startswith("W") for c in choice)
                            else None)
                        _uno_apply_play_multi(room, current, choice, colour)
                    else:
                        _uno_handle_draw(room, current)
            else:
                # Human turn. They may play card(s), draw, or pass
                # without drawing (uno_end_turn). A timeout draws a
                # card (and plays it if it must be played, §5).
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
                        _uno_handle_draw(room, current)
                        break
                    if action[0] == "play":
                        break  # the handler already applied the play
                    if action[0] == "draw":
                        _uno_handle_draw(room, current)
                        break
                    if action[0] == "end_turn":
                        break
            if room.phase != "playing":
                return
            if not hands[current.id]:
                winner = current
                break
            # Advance to the next player (skips included).
            _uno_advance(room, current)
        # v2.2 §9: purely the finishing-order scale by cards left
        # (fewest first). NO winner bonus.
        by_cards = sorted(players,
                          key=lambda p: len(room.round_state["hands"][p.id]))
        scores = president_scores_for(len(players))
        for i, p in enumerate(by_cards):
            _assign_score(room, p, scores[i])
        rows = []
        for p in by_cards:
            rows.append({"player_id": p.id, "nickname": p.nickname,
                         "colour": p.colour, "is_bot": p.is_bot,
                         "score": p.score, "avg_ms": None, "best_ms": None,
                         "false_starts": 0, "team": room.teams.get(p.id),
                         "cards_left": len(room.round_state["hands"][p.id])})
        await _end_of_game_flow(room, "uno", rows)
    except asyncio.CancelledError:
        pass


def _uno_apply_play_multi(room: Room, player: Player, cards, colour=None):
    """v2.2 §5: apply an UNO play of one or more cards (same value).
    ALL effects trigger in order played: two +2s add +4 to the
    pending draw, two skips skip two players, two reverses flip the
    direction twice. A Wild colour is picked ONCE for the whole play.
    Sets pending_draw / pending_type (stacking) and skip_count.
    v2.3 item 6: when there is no pending draw, a top-matching card
    is moved to the front first (the rest keep their given order),
    so a multi-play is legal whichever card the player picked first."""
    st = room.round_state
    if not st.get("pending_draw"):
        top = st["discard"][-1]
        current_colour = st.get("current_colour")
        for i, card in enumerate(cards):
            if uno_match(card, top, current_colour):
                cards = [card] + cards[:i] + cards[i + 1:]
                break
    order = st.get("order") or [p.id for p in room.players]
    colour_set = False
    for card in cards:
        st["hands"][player.id].remove(card)
        st["discard"].append(card)
        value = uno_value(card)
        if card.startswith("W"):
            if not colour_set:
                st["current_colour"] = colour or random.choice(UNO_COLOURS)
                colour_set = True
            if value == "draw4":
                st["pending_draw"] = st.get("pending_draw", 0) + 4
                st["pending_type"] = "draw4"
        else:
            st["current_colour"] = uno_colour(card)
            if value == "skip":
                st["skip_count"] = st.get("skip_count", 0) + 1
            elif value == "reverse":
                st["direction"] = -st.get("direction", 1)
                if len(order) == 2:
                    st["skip_count"] = st.get("skip_count", 0) + 1
            elif value == "draw2":
                st["pending_draw"] = st.get("pending_draw", 0) + 2
                st["pending_type"] = "draw2"
    st["skip_next"] = st.get("skip_count", 0) > 0


def _uno_apply_play(room: Room, player: Player, card: str, colour=None):
    """Apply a single-card UNO play (kept for compatibility; the
    game loop uses _uno_apply_play_multi)."""
    _uno_apply_play_multi(room, player, [card], colour)


# ---------------------------------------------------------------------------
# Group Elimination mode (v2.3 item 14)
# ---------------------------------------------------------------------------

def _elim_balance_groups(room: Room, players):
    """v2.3 item 14: group assignment for a Group series start.

    Starts from room.elim_groups (host-set, 0 = Red / 1 = Blue;
    a stored 2 = Black from a previous merge is treated as
    unassigned — Black only exists inside a series). Players with
    no group are assigned at random, always to the currently
    smaller group, so sizes stay balanced. Judgment call
    (documented per the group protocol): if one group ends up
    empty — e.g. the host put everyone in Red — the players are
    split evenly at random instead. Returns {player_id: 0|1}."""
    ids = [p.id for p in players]
    groups = {pid: room.elim_groups[pid] for pid in ids
              if room.elim_groups.get(pid) in (0, 1)}
    unassigned = [pid for pid in ids if pid not in groups]
    random.shuffle(unassigned)
    for pid in unassigned:
        c0 = sum(1 for v in groups.values() if v == 0)
        c1 = sum(1 for v in groups.values() if v == 1)
        groups[pid] = 0 if c0 <= c1 else 1
    if groups and all(v == 0 for v in groups.values()) or \
            groups and all(v == 1 for v in groups.values()):
        shuffled = list(groups)
        random.shuffle(shuffled)
        half = len(shuffled) // 2
        groups = {pid: (0 if i < half else 1)
                  for i, pid in enumerate(shuffled)}
    return groups


def _elim_pick_worst_team(group_scores):
    """v2.3 item 14: worst team of a match from
    group_scores = {0: [member scores], 1: [member scores]}.

    Worst = lowest TOTAL points. Tie-breaks (judgment calls,
    documented per the group protocol): totals tied -> the team
    with the lowest single member score is worst; still tied ->
    random. An empty group (no alive members) is skipped — it has
    nobody to eliminate. Deterministic setups (no ties) give a
    deterministic answer; tests must use those."""
    non_empty = {g: s for g, s in group_scores.items() if s}
    if len(non_empty) == 1:
        return next(iter(non_empty))
    totals = {g: sum(s) for g, s in non_empty.items()}
    worst_total = min(totals.values())
    tied = [g for g, t in totals.items() if t == worst_total]
    if len(tied) == 1:
        return tied[0]
    worst_single = min(min(non_empty[g]) for g in tied)
    tied = [g for g in tied if min(non_empty[g]) == worst_single]
    return random.choice(tied)


def _elim_pick_worst_player(alive, group=None, groups=None):
    """v2.3 item 14: worst player among `alive` — lowest p.score.
    With `group` set, only members of that group (per `groups`)
    are candidates. Tie-break (judgment call, documented per the
    group protocol): scores tied -> random among the tied lowest.
    Tests must use deterministic setups (a unique lowest score)."""
    candidates = [p for p in alive
                  if group is None or groups.get(p.id) == group]
    worst = min(p.score for p in candidates)
    tied = [p for p in candidates if p.score == worst]
    return random.choice(tied)


def _elim_rebalance_groups(room: Room, alive, groups):
    """v2.3 item 14: rebalance after an elimination (groups phase
    only). While the size gap between the two groups is > 1, move
    a random player from the bigger group to the smaller one.
    `groups` is the alive-only {player_id: 0|1} map and is mutated
    in place. Returns the transfers as
    [{player_id, nickname, from: 0|1, to: 1|0}] (possibly empty)
    so the break can show them to the players."""
    transfers = []
    while True:
        members0 = [pid for pid, g in groups.items() if g == 0]
        members1 = [pid for pid, g in groups.items() if g == 1]
        if abs(len(members0) - len(members1)) <= 1:
            break
        if len(members0) > len(members1):
            bigger, smaller, members = 0, 1, members0
        else:
            bigger, smaller, members = 1, 0, members1
        pid = random.choice(members)
        groups[pid] = smaller
        pl = room.get_player(pid)
        transfers.append({"player_id": pid,
                          "nickname": pl.nickname if pl else "?",
                          "from": bigger, "to": smaller})
    return transfers


def _elim_score_rows(room: Room, players, teams=None):
    """v2.4 item 3: scoreboard rows for an Elimination break or a
    per-round break inside a challenge. `score` is the just-finished
    challenge's (or so-far-this-challenge's) points; `tally` is the
    running per-series total from room.elim_tally. `teams` overrides
    the team/group shown per row (Group mode passes the groups map,
    {} for "no group" — the None the break used to send); when
    omitted the room's teams map is used, as before."""
    rows = []
    for p in players:
        rows.append({"player_id": p.id, "nickname": p.nickname,
                     "colour": p.colour, "is_bot": p.is_bot,
                     "score": p.score,
                     # v2.5 item 3: Reaction's native running scores —
                     # the average / best time so far. Only Reaction
                     # populates reaction_times, so other games get
                     # None here and are unaffected.
                     "avg_ms": (round(sum(p.reaction_times)
                                      / len(p.reaction_times))
                                if p.reaction_times else None),
                     "best_ms": (min(p.reaction_times)
                                 if p.reaction_times else None),
                     "tally": room.elim_tally.get(p.id, 0),
                     "team": (teams.get(p.id) if teams is not None
                              else room.teams.get(p.id))})
    return rows


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
    goes back to the lobby. v2.2: the host CAN force the outcome
    with `host_replay` (host_choice). Returns True (play again),
    False (lobby) or None (cancelled).
    """
    room.round_state = {"end_votes": {"again": set(), "lobby": set()},
                        "host_choice": None}
    needed = votes_needed_humans(room)
    await broadcast(room, {"type": "elimination_end_vote",
                           "votes_again": 0, "votes_lobby": 0,
                           "votes_needed": needed, "timeout": 60})
    if not any(p.connected for p in room.humans()):
        return False
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        votes = room.round_state.get("end_votes")
        if not votes:
            return None
        if room.round_state.get("host_choice") == "again":
            return True
        if room.round_state.get("host_choice") == "lobby":
            return False
        if len(votes["again"]) >= needed:
            return True
        if len(votes["lobby"]) >= needed:
            return False
        await asyncio.sleep(0.1)
    return False


async def _ceremony_stage(room: Room, payload: dict, eligible,
                          timeout=4):
    """v2.4 item 4: one elimination-ceremony stage. Broadcasts the
    stage message, then waits up to `timeout` seconds, cut short by
    the NEXT vote (>= half of the connected alive humans) or the
    host skip — reusing room.round_state["votes"] / "vote_eligible"
    / "host_skip" exactly like _vote_phase does, so the existing
    vote_next / host_skip handlers drive it (the ceremony NEXT
    button sends vote_next with NO index; the index check is
    skipped when no index is sent). With NO eligible humans (an
    all-bot series) the full timeout runs — unlike _vote_phase,
    which returns at once, the ceremony must still pace. The vote
    keys are cleaned up after each stage so the next stage and the
    following _elimination_break start clean. round_state["vote_index"]
    is set to the challenge number (display only)."""
    room.round_state["votes"] = set()
    room.round_state["vote_index"] = payload.get("challenge")
    room.round_state["host_skip"] = False
    room.round_state["ceremony"] = True
    eligible_ids = {p.id for p in eligible}
    room.round_state["vote_eligible"] = eligible_ids

    def votes_needed():
        alive_humans = [p for p in room.players
                        if not p.is_bot and p.connected
                        and p.id in eligible_ids]
        return max(1, (len(alive_humans) + 1) // 2)

    msg = dict(payload)
    msg["votes"] = 0
    msg["votes_needed"] = votes_needed()
    msg["timeout"] = timeout
    # Kept for the reconnect re-send path (below).
    room.last_ceremony = dict(msg)
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
    room.round_state.pop("ceremony", None)


async def _elimination_ceremony(room: Room, challenge_no: int, game: str,
                                ranking, loser_p, alive, elim_order,
                                group_info, is_group):
    """v2.4 item 4: the staged elimination ceremony, run AFTER the
    loser is picked and the merge / rebalance are computed, and
    BEFORE _elimination_break. Presentation only — the elimination
    logic itself (who is eliminated, merge, rebalance) is unchanged.
    Stages, in order: "list" (ordered list by the previous game's
    result, best first), "drop" (the eliminated player drops into
    the eliminated pile), "merge" (Group only, when 5 remain and
    the groups merged into the Black Team), "transfer" (Group only,
    when the rebalance moved players). Merge and transfer are
    mutually exclusive per challenge. Normal mode gets list + drop
    only. 4 seconds per stage, skippable by the NEXT vote / host
    skip (see _ceremony_stage)."""
    if group_info:
        # v2.5 item 4: the ranking rows show the teams as played
        # (groups_as_played, snapshotted before any merge/rebalance),
        # not the post-transfer map in group_info["groups"]. The
        # "groups" wire field below keeps the current map, as before
        # (the client does not read it in the ceremony).
        groups_current = group_info["groups"]
        groups = group_info.get("groups_as_played", groups_current)
        group_totals = group_info["group_totals"]
        worst_team = group_info["worst_team"]
        transfers = group_info["transfers"]
        merged = group_info["merged_to_black"]
    else:
        groups_current = {}
        groups = {}
        group_totals = {}
        worst_team = None
        transfers = []
        merged = False
    # The ordered list follows the challenge ranking (best first).
    # Ranking rows differ per game (quiz rows carry score, reaction
    # rows avg_ms, memory rows just the id), so the display values
    # come from the players' just-finished challenge scores.
    ranking_rows = []
    for row in ranking:
        pl = room.get_player(row["player_id"])
        if pl is None:
            continue
        if pl.id in groups:
            grp = groups.get(pl.id)
        elif is_group and pl.id == loser_p.id:
            # The eliminated player was already popped from the
            # groups map; their group is the worst team (groups
            # phase) or Black (black phase).
            grp = group_info["worst_team"] if group_info else None
        else:
            grp = None
        ranking_rows.append({
            "player_id": pl.id, "nickname": pl.nickname,
            "colour": pl.colour, "is_bot": pl.is_bot,
            "score": pl.score,
            "tally": room.elim_tally.get(pl.id, 0),
            "group": grp})
    # v2.5 item 1: the pile must contain each eliminated player
    # exactly once at every stage. elim_order already includes the
    # current victim, who only joins the pile when they drop — so
    # the list and drop stages send the pile WITHOUT the victim,
    # and the merge / transfer stages (reached after the drop) send
    # the full pile with the victim in it.
    pile_before_drop = [pid for pid in elim_order
                        if pid != loser_p.id]
    base = {"type": "elimination_ceremony",
            "challenge": challenge_no, "game": game,
            "elimination_mode": "group" if is_group else "normal",
            "elim_phase": room.elim_phase,
            "ranking": ranking_rows,
            "eliminated": {"player_id": loser_p.id,
                           "nickname": loser_p.nickname},
            "remaining": len(alive),
            "alive": [p.id for p in alive],
            "eliminated_pile": pile_before_drop,
            "groups": dict(groups_current) if is_group else {},
            "group_totals": group_totals,
            "worst_team": worst_team,
            "merged_to_black": merged,
            "transfers": transfers}
    for stage in ("list", "drop"):
        msg = dict(base)
        msg["stage"] = stage
        await _ceremony_stage(room, msg, eligible=list(alive))
        if room.phase != "playing":
            return
    if is_group and merged:
        msg = dict(base)
        msg["eliminated_pile"] = list(elim_order)
        msg["stage"] = "merge"
        await _ceremony_stage(room, msg, eligible=list(alive))
        if room.phase != "playing":
            return
    elif is_group and transfers:
        msg = dict(base)
        msg["eliminated_pile"] = list(elim_order)
        msg["stage"] = "transfer"
        await _ceremony_stage(room, msg, eligible=list(alive))


async def _elimination_break(room: Room, challenge_no: int, alive,
                             ranking, eliminated_names, remaining,
                             finishing_names, group_info=None):
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

    v2.3 item 14: in Group mode `group_info` carries the just-
    finished match's group data — the break's `scores` rows then
    carry the player's group in the `team` field (was None), and
    the break extra gains `group_totals`, `worst_team`,
    `transfers` and `merged_to_black` (Group only).
    """
    scores = _elim_score_rows(
        room, alive,
        # v2.5 item 4: the break's score rows show the teams as
        # played (snapshotted before merge/rebalance), not the
        # post-transfer map.
        teams=(group_info.get("groups_as_played",
                              group_info["groups"])
               if group_info else {}))
    # The ranking order is the challenge order (best first); the
    # scoreboard sorts by score, so keep the ranking visible too.
    rank_names = []
    for row in ranking:
        pl = room.get_player(row["player_id"])
        rank_names.append(pl.nickname if pl else "?")
    extra = {"elim_break": True,
             "eliminated": eliminated_names,
             "remaining": remaining,
             "finishing": finishing_names,
             "ranking": rank_names}
    if group_info:
        extra.update({"group_totals": group_info["group_totals"],
                      "worst_team": group_info["worst_team"],
                      "transfers": group_info["transfers"],
                      "merged_to_black": group_info["merged_to_black"]})
    await _vote_phase(room, "elimination", challenge_no, challenge_no,
                      scores=scores, eligible=list(alive),
                      extra=extra)


async def run_elimination_game(room: Room):
    """Elimination mode (v2): a series of quick challenges.

    Each challenge is one quick game picked at random from the pool
    (Reaction, Speed Math, Speed Trivia, Memory) among the remaining
    players; the LAST-PLACE player of that challenge is eliminated.
    v2.3 item 15: the series runs until exactly 1 player remains —
    one elimination per challenge, NO finale. The results screen
    lists the full finishing order, then a community vote decides
    Play Again (new series, same players) or Return to Lobby.

    v2.3 item 16: start_game forces room.mode = "ffa" for
    Elimination (Teams auto-switches off), so a series always runs
    FFA; eliminated players contribute 0 to team totals.

    v2.3 item 14 (Group mode): when
    room.game_settings["elimination"]["mode"] == "group", the
    players fight in two teams (Red 0 / Blue 1). After each
    challenge the team with the LOWEST TOTAL points loses its
    worst player (lowest p.score in that team); groups are then
    rebalanced until the size gap is <= 1 (transfers shown in the
    break). When 5 players remain they merge into the Black Team
    (group 2, phase "black") and from then on the worst player
    overall is eliminated per match until 1 remains. Groups
    persist across Play Again series in the same room (stored in
    room.elim_groups); at each series start missing groups are
    auto-balanced. Tie-breaks are in _elim_pick_worst_team /
    _elim_pick_worst_player.
    """
    all_players = list(room.players)
    try:
        while True:  # one loop iteration = one full series
            # A new series starts back in "playing": the end vote left
            # phase == "results", and every game loop bails on its
            # phase checks (Reaction returns before reaction_go, quiz
            # answers are ignored, run_challenge's ranking is dropped)
            # if it is not reset here — series 2+ would silently die.
            room.phase = "playing"
            # v2.2 §10: the championship baseline for this series.
            room.score_at_game_start = {p.id: p.score
                                        for p in all_players}
            # v2.4 item 3: a new series starts the running tally
            # from zero (Play Again resets it).
            room.elim_tally = {}
            alive = list(all_players)
            elim_order = []
            challenge_no = 0
            # v2.3 item 14: Group series setup — groups from
            # room.elim_groups, auto-balanced at series start.
            is_group = room.game_settings.get(
                "elimination", {}).get("mode") == "group"
            groups = {}
            if is_group:
                groups = _elim_balance_groups(room, alive)
                room.elim_groups = dict(groups)
                if len(alive) <= 5:
                    # Started with 5 or fewer (players left after
                    # the mode was set): straight into the Black
                    # phase — there is no groups phase to play.
                    for pid in groups:
                        groups[pid] = 2
                    room.elim_groups = dict(groups)
                    room.elim_phase = "black"
                else:
                    room.elim_phase = "groups"
            else:
                room.elim_phase = "groups"
            while len(alive) > 1:
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
                                       "finale": False,
                                       # v2.3 item 14: Group wire.
                                       "elimination_mode":
                                           "group" if is_group else "normal",
                                       "elim_phase": room.elim_phase,
                                       "groups": dict(groups)
                                           if is_group else {}})
                # v2.1: challenge intro — 4 seconds to read what game
                # is coming and who is still in, before it starts.
                await asyncio.sleep(4)
                await broadcast(room, {"type": "game_started", "game": game,
                                       "settings": room.game_settings[game],
                                       "mode": "competition",
                                       "party_mode": room.mode,
                                       "team_count": room.team_count,
                                       "teams": {pid: t for pid, t in room.teams.items()},
                                       "turn_order": list(room.turn_order),
                                       "team_scoring": room.team_scoring,
                                       "championship": room.championship,
                                       "champ_games": room.champ_games})
                ranking = await run_challenge(room, game, alive)
                if ranking is None or room.phase != "playing":
                    return
                # v2.4 item 3: fold this challenge's points into
                # the persistent per-series tally BEFORE the next
                # challenge resets p.score to 0. Every alive
                # participant's challenge points count, including
                # Memory survivors (100 x survived) and the winner
                # (1000). The worst-team / worst-player picks below
                # keep using the just-finished challenge's p.score.
                for p in alive:
                    room.elim_tally[p.id] = (
                        room.elim_tally.get(p.id, 0) + p.score)
                # --- pick the eliminated player (v2.3 item 14) ---
                group_info = None
                if is_group and room.elim_phase == "black":
                    # Black phase: worst player OVERALL.
                    loser_p = _elim_pick_worst_player(alive)
                    worst_team = 2
                    group_totals = {2: sum(p.score for p in alive)}
                    transfers = []
                    merged = False
                elif is_group:
                    # Groups phase: worst team by TOTAL points loses
                    # its worst player (lowest p.score in that team).
                    # Totals are from the just-finished challenge,
                    # before this elimination.
                    group_scores = {
                        0: [p.score for p in alive
                            if groups.get(p.id) == 0],
                        1: [p.score for p in alive
                            if groups.get(p.id) == 1]}
                    group_totals = {g: sum(s)
                                    for g, s in group_scores.items()}
                    worst_team = _elim_pick_worst_team(group_scores)
                    loser_p = _elim_pick_worst_player(
                        alive, group=worst_team, groups=groups)
                    transfers = []
                    merged = False
                else:
                    # Normal mode (v2.3 item 15): last place goes out.
                    last = ranking[-1]
                    loser_p = room.get_player(last["player_id"])
                elim_order.append(loser_p.id)
                alive = [p for p in alive if p.id != loser_p.id]
                if is_group:
                    groups.pop(loser_p.id, None)
                # v2.5 item 4: snapshot the teams as played, right
                # after the loser is picked and popped and BEFORE
                # any merge / rebalance mutation below. The ceremony
                # ranking rows and the break's score rows read this
                # snapshot; group_info["groups"] keeps the current
                # (post-transfer) map.
                groups_as_played = dict(groups) if is_group else {}
                # v2.4 item 4: the elimination_eliminated broadcast
                # is gone from the series flow — the staged ceremony
                # below replaces it (the client handler stays as a
                # harmless fallback).
                if is_group:
                    if room.elim_phase == "groups":
                        if len(alive) == 5:
                            # Merge: the final 5 become the Black Team.
                            merged = True
                            for p in alive:
                                groups[p.id] = 2
                            room.elim_phase = "black"
                        else:
                            transfers = _elim_rebalance_groups(
                                room, alive, groups)
                    # Persist the (possibly moved/merged) groups so
                    # they survive into the next series in this room;
                    # eliminated players keep their last group entry.
                    room.elim_groups.update(groups)
                    group_info = {"groups": dict(groups),
                                  "groups_as_played": groups_as_played,
                                  "group_totals": group_totals,
                                  "worst_team": worst_team,
                                  "transfers": transfers,
                                  "merged_to_black": merged}
                # v2.4 item 4: the staged elimination ceremony —
                # ordered list, the drop, then (Group only) the
                # merge / transfer — runs BEFORE the pacing break,
                # which follows it as before. Presentation only.
                await _elimination_ceremony(
                    room, challenge_no, game, ranking, loser_p, alive,
                    elim_order, group_info, is_group)
                if room.phase != "playing":
                    return
                # v2.1: pacing break — ranking, who went out, who is
                # left — with a NEXT vote. Nothing advances until the
                # vote passes, the host skips, or the timeout hits.
                await _elimination_break(
                    room, challenge_no, alive, ranking,
                    [loser_p.nickname], len(alive),
                    [room.get_player(pid).nickname
                     for pid in reversed(elim_order)
                     if room.get_player(pid)],
                    group_info=group_info)
                if room.phase != "playing":
                    return
            # v2.3 item 15: no finale — the last remaining player is
            # the victor; finishing order is winner first, then the
            # eliminated in reverse elimination order.
            # v2.6 item 2: REVERT of the v2.5 linear place scores —
            # the series-end results are an ordered ranking only
            # (winner first, then eliminated most-recent-first), with
            # each row showing the player's ACCUMULATED series tally
            # (room.elim_tally), not linear place points. No place
            # scores are assigned here and _assign_score is not
            # called for place scores.
            finishing = [alive[0].id] + list(reversed(elim_order))
            order_rows = []
            for place, pid in enumerate(finishing, start=1):
                pl = room.get_player(pid)
                order_rows.append({"place": place, "player_id": pid,
                                   "nickname": pl.nickname,
                                   "colour": pl.colour, "is_bot": pl.is_bot,
                                   "score": room.elim_tally.get(pid, 0),
                                   "avg_ms": None, "best_ms": None,
                                   "false_starts": 0,
                                   "team": room.teams.get(pid)})
            # v2.6 item 3: already guarded — with Championship forced
            # off at Elimination start room.championship is False, so
            # this does not fire for a series. Left as the guard in
            # case a series ever runs with championship on.
            if room.championship:
                _record_champ_game(room, "elimination")
            room.phase = "results"
            await broadcast(room, {"type": "results", "game": "elimination",
                                   "mode": room.mode,
                                   "teams": team_standings(room),
                                   "rankings": order_rows,
                                   "finishing_order": order_rows,
                                   "victor": finishing[0],
                                   "team_scoring": room.team_scoring,
                                   "championship": room.championship,
                                   "champ_games": room.champ_games})
            await broadcast_lobby(room)
            # The end-vote handler requires current_game == "elimination",
            # but the last challenge left it set to the challenge game.
            room.current_game = "elimination"
            again = await elimination_end_vote(room)
            if again is None:
                return
            if again:
                continue  # new series, same players
            # Community chose Return to Lobby.
            room.phase = "lobby"
            room.current_game = None
            if not room.championship:
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
    # v2.3 item 16: Elimination auto-switches Teams OFF — force FFA
    # before the game starts (after the checks above, so a failed
    # start leaves the lobby mode untouched).
    if game == "elimination":
        room.mode = "ffa"
        # v2.6 item 3: Elimination also forces Championship OFF.
        # Belt-and-braces — the client suspends it via settings, but
        # a stale/older client may not. Suspend, don't end: stash
        # the flag and switch off WITHOUT clearing the champ history
        # (the v2.2 end-championship wipe does not run here); the
        # client restores it via settings when a normal game is
        # picked again. Do NOT auto-restore server-side on a normal
        # game start. With championship off here, _begin_game resets
        # scores per game, which is correct for Elimination.
        if room.championship:
            room.champ_suspended = True
            room.championship = False
    await _begin_game(room, game)


async def _begin_game(room: Room, game: str):
    """v2.2: shared game entry for fresh starts (start_game) and
    replays (replay_game). Scores reset for a fresh game, EXCEPT in
    championship mode, where they persist (§10); reaction times /
    false starts reset per game either way. The turn order is
    (re-)computed here (§4), and score_at_game_start is recorded for
    the championship history delta."""
    if room.championship:
        room.score_at_game_start = {p.id: p.score for p in room.players}
    else:
        for p in room.players:
            p.score = 0
            p.reaction_times = []
            p.false_starts = 0
        room.score_at_game_start = {p.id: 0 for p in room.players}
    room.turn_order = compute_turn_order(room)
    room.current_game = game
    room.phase = "playing"
    mode = "solo" if len(room.players) == 1 else "competition"
    # v2.3 item 16: start_game forces room.mode = "ffa" for
    # Elimination, so party_mode here is "ffa" for that game.
    await broadcast(room, {"type": "game_started", "game": game,
                           "settings": room.game_settings[game],
                           "mode": mode, "party_mode": room.mode,
                           "team_count": room.team_count,
                           "teams": {pid: t for pid, t in room.teams.items()},
                           "turn_order": list(room.turn_order),
                           "team_scoring": room.team_scoring,
                           "championship": room.championship,
                           "champ_games": room.champ_games})
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


async def replay_game(room: Room):
    """v2.2 §1: re-enter start_game logic WITHOUT the lobby/host
    checks. Same game and settings; if the selected game was
    "random", RE-ROLL a new random game (stored in
    room.current_game via _begin_game); the turn order is
    re-randomized; President roles persist (they live in
    room.president_finish_order); championship scores are NOT reset
    (§10). Elimination replays through its own end vote, not here."""
    game = room.game
    if game == "random":
        game = random.choice(RANDOM_GAMES)
    await _begin_game(room, game)


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
                    if game == "secret" and "impostors" in patch:
                        try:
                            patch = dict(patch)
                            patch["impostors"] = (
                                2 if int(patch["impostors"]) == 2 else 1)
                        except (ValueError, TypeError):
                            patch = {k: v for k, v in patch.items()
                                     if k != "impostors"}
                    room.game_settings[game].update(patch)
        # v2.2: turn order, team scoring style, championship toggle.
        if msg.get("turn_order_mode") in ("random", "manual"):
            room.turn_order_mode = msg["turn_order_mode"]
        order_msg = msg.get("turn_order", msg.get("manual_order"))
        if isinstance(order_msg, list):
            ids = {p.id for p in room.players}
            room.manual_order = [pid for pid in order_msg if pid in ids]
        if msg.get("team_scoring") in ("average", "sum", "best"):
            room.team_scoring = msg["team_scoring"]
        if isinstance(msg.get("championship"), bool):
            suspended = msg.get("championship_suspended") is True
            if suspended and room.championship:
                # v2.6 item 3: forced suspension for Elimination —
                # the client sends championship: false with
                # championship_suspended: true when Elimination is
                # picked and Championship was on. Stash the flag and
                # switch off WITHOUT running the v2.2 end-championship
                # path below: the history and game counter survive,
                # and the client restores it when a normal game is
                # picked again.
                room.champ_suspended = True
                room.championship = False
            elif room.championship and not msg["championship"]:
                # v2.2 §10: turning championship OFF ends it — clear
                # the history and the game counter; per-game scoring
                # resumes at the next start_game. An explicit off
                # (no suspended flag) also ends a suspension.
                room.champ_history = []
                room.champ_games = 0
                room.champ_suspended = False
                room.championship = False
            else:
                # Turning it on (or re-confirming it off while it is
                # already off): switching it back on clears any
                # suspension flag — the championship is restored by
                # the client's value.
                if msg["championship"]:
                    room.champ_suspended = False
                room.championship = msg["championship"]
        elif msg.get("championship_suspended") is True \
                and room.championship:
            # Bare suspended flag with no championship value: same
            # suspension as above (client normally sends both).
            room.champ_suspended = True
            room.championship = False
        # v2.6 item 3: picking a normal (non-Elimination) game while
        # suspended restores the championship. The client normally
        # sends championship: true with the game change (handled
        # above); this covers a game-only message. An explicit
        # championship value in this message wins (handled above),
        # so this never overrides an explicit off.
        if room.champ_suspended and room.game != "elimination" \
                and not isinstance(msg.get("championship"), bool):
            room.champ_suspended = False
            room.championship = True
        # v2.3 item 14: Elimination Group mode (host only, lobby).
        if "elimination_mode" in msg:
            mode_val = msg["elimination_mode"]
            if mode_val in ("normal", "group"):
                if mode_val == "group" and len(room.players) < 6:
                    await send(player, {"type": "error", "message":
                        "Group mode needs at least 6 players — "
                        "add bots or wait for players."})
                else:
                    room.game_settings.setdefault(
                        "elimination", {})["mode"] = mode_val
                    if mode_val == "group":
                        room.elim_phase = "groups"
        if isinstance(msg.get("elim_groups"), dict):
            # Full map {player_id: 0|1}; values other than 0/1
            # (including 2 = Black, which only the merge sets) and
            # unknown player ids are ignored.
            ids = {p.id for p in room.players}
            room.elim_groups = {
                pid: int(v) for pid, v in msg["elim_groups"].items()
                if pid in ids and isinstance(v, int)
                and not isinstance(v, bool) and v in (0, 1)}
        if isinstance(msg.get("teams"), dict):
            # v2.3 item 14: full Teams map {player_id: 0..team_count-1 |
            # None} from the tap-to-move columns UI. Merged over the
            # stored map (players missing from the map keep their team);
            # values outside the team range and unknown ids are ignored.
            ids = {p.id for p in room.players}
            for pid, v in msg["teams"].items():
                if pid not in ids:
                    continue
                if v is None:
                    room.teams[pid] = None
                elif isinstance(v, int) and not isinstance(v, bool) \
                        and 0 <= v < room.team_count:
                    room.teams[pid] = int(v)
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
        # v2.2: only ALIVE players give clues (voted-out spectate).
        if room.phase != "playing" or room.current_game != "secret":
            return
        st = room.round_state
        if st.get("waiting_clue") != player.id:
            return
        if st.get("alive") is not None and player.id not in st["alive"]:
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
        # v2.2: only ALIVE players vote, and only for an alive player.
        if room.phase != "playing" or room.current_game != "secret":
            return
        st = room.round_state
        if "votes" not in st or player.is_bot:
            return
        if st.get("alive") is not None and player.id not in st["alive"]:
            return
        target = msg.get("player_id")
        if not target or target == player.id or not room.get_player(target):
            return
        if st.get("alive") is not None and target not in st["alive"]:
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

    elif mtype == "president_exchange":
        # v2.2 §6: a human giver picks the cards to give back in the
        # President card exchange (the worst cards of their choice).
        # The server validates the count and that the cards are in
        # hand; an invalid pick is rejected and the timeout auto-pick
        # (lowest ranks) applies instead.
        if room.phase != "playing" or room.current_game != "president":
            return
        st = room.round_state
        exchange = st.get("exchange")
        if not exchange or exchange.get("giver_id") != player.id:
            return
        cards = msg.get("cards")
        if not isinstance(cards, list):
            return
        hand = st.get("hands", {}).get(player.id, [])
        if len(cards) != exchange.get("count") or \
                any(c not in hand for c in cards):
            await send(player, {"type": "error", "message":
                "Pick exactly %d card(s) from your hand to give back."
                % exchange.get("count", 0)})
            return
        st["exchange_cards"] = list(cards)
        ev = st.get("exchange_event")
        if ev is not None:
            ev.set()

    elif mtype == "uno_play":
        # v2 UNO: play a card on your turn. Wilds need a colour.
        # v2.2 §5: `cards: [...]` (multi-play) as well as legacy
        # `card: "..."`; the Wild colour is picked once per play.
        if room.phase != "playing" or room.current_game != "uno":
            return
        st = room.round_state
        if st.get("turn") != player.id or st.get("turn_action") is not None:
            return
        cards = msg.get("cards")
        if isinstance(cards, list):
            cards = list(cards)
        elif msg.get("card"):
            cards = [msg.get("card")]
        else:
            return
        hand = st.get("hands", {}).get(player.id, [])
        settings = room.game_settings.get("uno", {})
        reason = uno_validate_play(
            hand, cards, st["discard"][-1], st["current_colour"],
            pending_draw=st.get("pending_draw", 0),
            pending_type=st.get("pending_type"),
            multi_play=bool(settings.get("multi_play")))
        if reason is not None:
            await send(player, {"type": "error", "message": reason})
            return
        colour = msg.get("colour")
        if any(c.startswith("W") for c in cards):
            if colour not in UNO_COLOURS:
                await send(player, {"type": "error",
                                    "message": "Pick a colour for the Wild card."})
                return
        _uno_apply_play_multi(room, player, cards, colour)
        st["turn_action"] = ("play", cards)
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
        # v2.2 §5: pass without drawing and end your turn. (With
        # must-play, a drawn playable card is auto-played, so there
        # is no drawn-card offer to keep anymore.)
        if room.phase != "playing" or room.current_game != "uno":
            return
        st = room.round_state
        if st.get("turn") != player.id or st.get("turn_action") is not None:
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
        # host can force the outcome with `host_replay` (v2.2).
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
        # v2.2: also accepted in phase "results" (end-of-game vote).
        if room.phase not in ("playing", "results"):
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
        if eligible is not None:
            alive_humans = [p for p in room.players
                            if not p.is_bot and p.connected
                            and p.id in eligible]
            needed = max(1, (len(alive_humans) + 1) // 2)
        else:
            needed = votes_needed_humans(room)
        await broadcast(room, {"type": "vote_update",
                               "index": st.get("vote_index"),
                               "votes": len(st["votes"]),
                               "votes_needed": needed,
                               "voters": sorted(st["votes"])})

    elif mtype == "host_skip":
        # v1.2: the host's NEXT override — advances the vote break
        # immediately, regardless of votes. Host-only, vote break only.
        # v2.2: also works during the end-of-game vote (phase "results").
        if not player.is_host:
            await send(player, {"type": "error",
                                "message": "Only the host can skip ahead."})
            return
        st = room.round_state
        if room.phase not in ("playing", "results") or "votes" not in st:
            return
        st["host_skip"] = True
        await broadcast(room, {"type": "vote_update",
                               "index": st.get("vote_index"),
                               "votes": len(st["votes"]),
                               "votes_needed": votes_needed_humans(room),
                               "voters": sorted(st["votes"]),
                               "host_skipped": True})

    elif mtype == "replay_vote":
        # v2.2: a player's vote on the end-of-game choice ("again" |
        # "lobby"), after end_choice. Humans only; bots never vote.
        # >= half of connected humans for either choice decides it.
        if room.phase != "results":
            return
        st = room.round_state
        votes = st.get("replay_votes")
        if votes is None or player.is_bot:
            return
        choice = msg.get("choice")
        if choice not in ("again", "lobby"):
            return
        votes["again"].discard(player.id)
        votes["lobby"].discard(player.id)
        votes[choice].add(player.id)
        needed = votes_needed_humans(room)
        await broadcast(room, {"type": "replay_vote_update",
                               "votes_again": len(votes["again"]),
                               "votes_lobby": len(votes["lobby"]),
                               "votes_needed": needed})

    elif mtype == "host_replay":
        # v2.2: the host's override for the replay vote — and for the
        # Elimination end vote (elimination_vote flow). Host-only.
        if not player.is_host:
            await send(player, {"type": "error",
                                "message": "Only the host can decide the replay."})
            return
        choice = msg.get("choice")
        if choice not in ("again", "lobby"):
            return
        st = room.round_state
        if room.phase != "results":
            return
        if st.get("replay_votes") is not None:
            st["replay_choice"] = choice
            votes = st["replay_votes"]
            await broadcast(room, {"type": "replay_vote_update",
                                   "votes_again": len(votes["again"]),
                                   "votes_lobby": len(votes["lobby"]),
                                   "votes_needed": votes_needed_humans(room),
                                   "host_choice": choice})
        elif st.get("end_votes") is not None:
            # Elimination end vote: force its outcome.
            st["host_choice"] = choice
            votes = st["end_votes"]
            await broadcast(room, {"type": "elimination_end_vote_update",
                                   "votes_again": len(votes["again"]),
                                   "votes_lobby": len(votes["lobby"]),
                                   "votes_needed": votes_needed_humans(room),
                                   "host_choice": choice})

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
                                "teams": {pid: t for pid, t in room.teams.items()},
                                "turn_order": list(room.turn_order),
                                "team_scoring": room.team_scoring,
                                "championship": room.championship,
                                "champ_games": room.champ_games})
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
                # v2.2: category goes to both roles; word to citizens only.
                secret = st.get("secret", {})
                impostors = st.get("impostors", set())
                is_imp = player.id in impostors
                # v2.3: turn order = alive list in clue order (the
                # room turn order filtered to the alive set).
                _alive_ids = set(st.get("alive", set()))
                _turn_order = [pid for pid in (room.turn_order or [])
                               if pid in _alive_ids]
                _turn_order += [pid for pid in sorted(_alive_ids)
                                if pid not in _turn_order]
                await send(player, {"type": "secret_word",
                                    "word": secret.get(player.id),
                                    "category": st.get("category", ""),
                                    "role": "imposter" if is_imp else "citizen",
                                    "is_impostor": is_imp,
                                    "impostor_count": len(impostors),
                                    "impostors": len(impostors),
                                    "round": st.get("round", 1),
                                    "alive": sorted(st.get("alive", set())),
                                    "turn_order": _turn_order,
                                    "eliminated": st.get("eliminated", [])})
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
            if st.get("ceremony") and room.last_ceremony is not None:
                # v2.4 item 4: an elimination-ceremony stage is in
                # progress — re-send the latest stage (with live
                # vote counts) so the rejoiner lands on the ceremony
                # page instead of the stale challenge state above.
                # The break that follows re-syncs everyone anyway.
                _cer = dict(room.last_ceremony)
                _elig = st.get("vote_eligible") or set()
                _cer["votes"] = len(st.get("votes", set()))
                _cer_humans = [p for p in room.players
                               if not p.is_bot and p.connected
                               and p.id in _elig]
                _cer["votes_needed"] = max(1, (len(_cer_humans) + 1) // 2)
                await send(player, _cer)

        elif room.phase == "results" and room.current_game:
            # v2.3 item 11: resync for a player rejoining on the
            # results screen — the old code only resynced mid-game
            # ("playing"), so a rejoiner in "results" got no results
            # message and sat on a stale screen. Re-send `results`
            # (scores stripped for Secret, as in _end_of_game_flow)
            # and, once the end-of-game vote has finished, `end_choice`
            # so they can vote on the replay. President finish order
            # / titles are recovered from room.president_finish_order
            # (still stored); Secret's word/outcome lived only in the
            # finished game loop's locals, so it cannot be recovered
            # here and is omitted for the rejoiner.
            results_rows = rankings(room)
            if room.current_game == "secret":
                results_rows = [{k: v for k, v in r.items() if k != "score"}
                                for r in results_rows]
            resync_msg = {"type": "results", "game": room.current_game,
                          "mode": room.mode,
                          "teams": team_standings(room),
                          "rankings": results_rows,
                          "team_scoring": room.team_scoring,
                          "championship": room.championship,
                          "champ_games": room.champ_games}
            if room.current_game == "secret":
                resync_msg["scores_hidden"] = True
            if room.current_game == "president" \
                    and room.president_finish_order:
                _titles = president_titles_for_finish(len(room.players))
                resync_msg["finish_order"] = list(
                    room.president_finish_order)
                resync_msg["titles"] = {
                    pid: _titles[i]
                    for i, pid in enumerate(room.president_finish_order)
                    if i < len(_titles)}
            await send(player, resync_msg)
            st = room.round_state
            if st.get("votes") is not None \
                    and st.get("replay_votes") is None \
                    and st.get("end_votes") is None:
                # End-of-game NEXT vote still in progress: re-send
                # the vote break so the rejoiner can vote NEXT.
                await send(player, {"type": "vote_break",
                                    "index": st.get("vote_index", 1),
                                    "total": 1, "scores": [],
                                    "votes": len(st["votes"]),
                                    "votes_needed": votes_needed_humans(room),
                                    "timeout": VOTE_TIMEOUT_DEFAULT,
                                    "mode": room.mode,
                                    "teams": team_standings(room),
                                    "end_of_game": True})
            if st.get("replay_votes") is not None:
                await send(player, {"type": "end_choice",
                                    "game": room.current_game,
                                    "rankings": rankings(room),
                                    "teams": team_standings(room),
                                    "team_scoring": room.team_scoring,
                                    "championship": room.championship,
                                    "champ_games": room.champ_games,
                                    "champ_history": room.champ_history})
            elif room.current_game == "elimination" \
                    and st.get("end_votes") is not None:
                votes = st["end_votes"]
                await send(player, {"type": "elimination_end_vote",
                                    "votes_again": len(votes["again"]),
                                    "votes_lobby": len(votes["lobby"]),
                                    "votes_needed": votes_needed_humans(room),
                                    "timeout": 60})

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
