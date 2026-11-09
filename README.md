# Party Game Hub — v1.1

A free, simple online party-game hub for 1–8 players (humans + bots),
built for Matthew and his cousins. Simple 2D, text and coloured shapes
only — no fancy graphics, no accounts, no sign-in.

**V1 games:** Reaction Test, Speed Math, Speed Trivia.
Later phases will add Secret Word / Impostor, President / Ditchdiggers,
and UNO — the server is structured for that (see `server.py`,
"Game handlers" section).

## How it works

- **Python server** (`server.py`, FastAPI + WebSockets) holds all game
  logic, so every player in a room sees the same state at the same time.
- **Browser frontend** (`static/index.html`, one page) is served by the
  same server. Players just open a link — nothing to install.
- **Rooms:** Create a server and you get a random 6-letter code
  (e.g. `KQ7X2P`) and a shareable link (`/?code=KQ7X2P`). Join by
  pasting the link or entering the code. Each player picks a nickname
  (unique in the room) and a colour from a 10-colour palette.
- **Lobby:** live player list, host crown 👑, bots marked `BOT`.
  The host manages server settings (room name, max players up to 8),
  game settings, and can **Add BOT**. Click a bot to change its
  nickname, colour, or difficulty (Easy / Medium / Hard), or remove it.
  If the host disconnects, the next human player becomes host.
- **Game picker:** host picks Reaction Test, Speed Math, or Speed
  Trivia from a dropdown, or 🎲 Random. The Start button stays
  disabled until the minimum player count is met (bots count).
- **Minimums (v1):** Reaction Test 1 (solo practice) / 2+ competition;
  Speed Math 1–8; Speed Trivia 1–8. Max 8 players including bots.
- **Reaction Test:** rounds 3 / 5 / 10 (default 5). Red "Wait…" →
  random 1–4 s → green "CLICK NOW!". Clicking early is a **false
  start: that round counts as a fail** (no time recorded). Bots react
  at Easy 380–600 ms, Medium 250–380 ms, Hard 180–280 ms, with
  occasional false starts on Easy. Fastest average wins.
- **Speed Math:** 5 / 10 / 15 questions (default 10); 10 / 15 / 20 s
  per question (default 15 s). Same question to everyone; faster
  correct answers score more (500–1000 pts). Bots: Easy ~55% correct,
  Medium ~75%, Hard ~92%, with difficulty-based delays.
  - Easy: addition and subtraction only, operands and results
    between -30 and 30 (negative numbers included).
  - Medium: addition, subtraction, multiplication up to 12×12, and
    division with exact integer results.
  - Hard: high-school level — basic functions f(x), solving for x,
    max/min of a quadratic, advanced BEDMAS with brackets, and
    adding/subtracting fractions. Fraction answers must be given as a
    decimal to 2 places (the question says so), and are accepted
    within 0.01.
- **Speed Trivia:** categories General / Science / Geography /
  History / Mixed; Easy / Medium / Hard; 5 / 10 / 15 questions;
  10 / 15 / 20 s per question. 72 questions embedded in
  `questions.py` — no external API. Same scoring and bot behaviour
  as Speed Math.
- **After a game:** results screen with rankings, then **Play Again**
  (back to lobby, same players) or **Change Game**.
- **NEXT QUESTION vote (v1.1, Speed Math / Speed Trivia only):** after
  each question (except the last) the scoreboard stays up with a
  **NEXT QUESTION** button. The game advances when at least half of
  the players in the room have voted NEXT, or after 60 seconds,
  whichever comes first. The live vote count is shown (e.g. "2/3
  voted NEXT"). Bots auto-vote after 2–4 s, so a solo player with
  bots is never stuck. Reaction Test keeps its 1.5 s pause between
  rounds and has no vote.
- **Edit your profile in the lobby (v1.1):** every human player can
  change their *own* nickname and colour in the lobby (and between
  games, on the results screen), with the same 10-colour palette.
  Nicknames must stay unique in the room. Bots are still host-only.
- **Disconnects (v1.1 hardened):** a player who disconnects mid-game
  has their score frozen, and the round is held for the full timer
  (it no longer ends early just because the remaining connected
  players have answered), so a reconnecting player can still answer
  or click. The frontend auto-reconnects (up to 20 attempts, 5 s
  heartbeat, visible "Reconnecting…" banner) and reclaims the exact
  same seat — same player id, nickname, colour, host status and
  score — and is re-sent the current question / reaction state on
  rejoining mid-game. In the lobby a seat is held for 30 s.

## Run locally

```bash
cd party-game-hub
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
.venv/bin/uvicorn server:app --host 0.0.0.0 --port 8000
```

Open http://localhost:8000 — create a server, copy the link, and
open it in a second browser window/tab (or another device on the
same Wi-Fi, using your computer's LAN IP, e.g.
`http://192.168.1.10:8000/?code=XXXXXX`) to play together.

## Run the automated test

```bash
.venv/bin/python tests/test_hub.py
```

It starts the server on port 8001, creates a room, joins 2 simulated
humans + 2 bots, checks the lobby, then plays a full round of each of
the 3 games and checks scores/results are produced. It also checks
the trivia bank has ≥ 60 questions.

**v1.1 additions (77 checks total):** a round does not advance after
only one player answers; a disconnect mid-question holds the round
and a rejoin reclaims the same seat and is re-sent the question;
the NEXT vote advances at half the players and at the timeout;
lobby profile editing works and taken nicknames are rejected;
20 generated questions per Math difficulty verify independently
(including all five Hard types), and Hard fraction answers are
accepted within 0.01.

## Deploy later (Replit / Render)

The app is a standard FastAPI app — one process, no database, state
kept in memory (rooms disappear on restart, which is fine for a
party game).

- **Render (free tier):** create a Web Service from this repo,
  build command `pip install -r requirements.txt`, start command
  `uvicorn server:app --host 0.0.0.0 --port $PORT`. Note: free
  Render services sleep after inactivity, so the first join may
  take ~30–60 s to wake up.
- **Replit:** import the repo, set the run command to
  `uvicorn server:app --host 0.0.0.0 --port 8000` (or use the
  Replit `PORT` env var). Replit was not connected during v1, so
  this was not tested there — check the deployment URL serves the
  home page before sharing it.
- **Any VPS / computer left running:** same `uvicorn` command as
  local. For cousins to reach it, the host needs a public URL
  (Render/Replit give you one; a home computer needs port
  forwarding or a tunnel).

## Files

- `server.py` — FastAPI app, WebSocket protocol, rooms, bots, all
  game logic. Game handlers: `run_reaction_game`, `run_quiz_game`.
- `questions.py` — 72 embedded Speed Trivia questions.
- `static/index.html` — the whole frontend (home / lobby / game /
  results), plain HTML/CSS/JS, mobile-friendly.
- `tests/test_hub.py` — automated end-to-end test (see above).
- `requirements.txt`, `README.md` — this file.
