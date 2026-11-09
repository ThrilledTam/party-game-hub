# Party Game Hub — v1.2

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
  voted NEXT"). Reaction Test keeps its 1.5 s pause between
  rounds and has no vote.
  - **v1.2 change: bots do NOT vote and do NOT count toward the
    threshold.** The threshold is now ceil(connected *human* players
    / 2) — bots can no longer decide the break. A solo player with
    bots is never stuck: with 1 human the threshold is 1, so your
    own vote advances, and the 60 s timeout is the fallback.
  - **v1.2: host NEXT override.** On the vote-break screen the host
    also gets an orange **"HOST: Skip to next question →"** button
    that advances immediately, regardless of votes.
- **Edit your profile in the lobby (v1.1):** every human player can
  change their *own* nickname and colour in the lobby (and between
  games, on the results screen), with the same 10-colour palette.
  Nicknames must stay unique in the room. Bots are still host-only.
  The profile box is now compact (v1.2, ~1/3 of its old size), and
  so is the host's bot-customization panel.
- **Colours are unique per room (v1.2):** no two players or bots in
  a room may share a colour. Joining or changing to a taken colour
  is rejected with "That colour is already taken, pick another",
  taken colours are greyed out in every colour picker, and Add BOT
  auto-picks a colour nobody uses.
- **Disconnects (v1.1 hardened, v1.2 fixed):** a player who disconnects
  mid-game has their score frozen, and the round is held for the full
  timer (it no longer ends early just because the remaining connected
  players have answered), so a reconnecting player can still answer
  or click. The frontend auto-reconnects (up to 20 attempts, 5 s
  heartbeat, visible "Reconnecting…" banner) and reclaims the exact
  same seat — same player id, nickname, colour, host status and
  score — and is re-sent the current question / reaction state on
  rejoining mid-game. In the lobby a seat is held for 30 s.
  - **v1.2 root-cause fix:** a player who disconnected *after* a game
    ended (results screen) or mid-game used to never be removed at
    all, so their nickname stayed "already taken" for minutes even
    after their browser closed. Now every disconnect schedules a
    cleanup that removes the player after the grace (waiting for any
    running game to finish first) and frees their nickname and colour.
  - **v1.2 room survival:** a room is no longer deleted the moment
    its host disconnects — a new host is promoted and the room stays.
    A room with no connected humans left is held for 120 s before
    being deleted, so a brief drop does not wipe it.
  - **Known limit:** rooms live only in server memory. If the server
    itself restarts (e.g. a Render free-tier restart), all rooms are
    wiped and cannot be recovered — joining then shows "This room no
    longer exists — it may have been closed or the server restarted"
    with a button to create a new room.
- **Host kick (v1.2):** in the lobby (and on the results screen
  between games) the host sees a **Kick** button next to each other
  human player. The kicked player is told "You were kicked from the
  room by the host" and is sent back to the home screen. Bots are
  still removed with the host's bot Remove button.
- **Game modes (v1.2): FFA and Teams.** The host picks the mode in
  the lobby (default FFA). In **Teams** mode the host chooses 2, 3,
  or 4 teams and assigns players/bots either with **🎲 Randomize
  teams** (balanced, everyone assigned) or one by one with a
  per-player dropdown in the lobby. Anyone with no team is
  **Alone** — their own one-person team. Each team must have at
  least one player/bot: Start is blocked (with an error) if a team
  is empty or there are fewer players than teams. Team score is the
  **average** of its members' individual scores (bots count like
  anyone else); individual scores are still displayed everywhere.
  The results screen shows a team standings table above the
  individual rankings, and the vote-break scoreboard shows team
  averages too. Teams persist through Play Again in the same room;
  switching back to FFA hides the team display. Team badges
  (Team 1 red, Team 2 blue, Team 3 green, Team 4 yellow, Alone grey)
  show next to each player in the lobby and on scoreboards.

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

**v1.2 additions (105 checks total):** a disconnected player is
removed after the grace and their nickname is free again; the host
can kick a player; a room survives its host disconnecting (new host
promoted) and an empty room is held, then deleted, after its grace;
bots do not vote and do not count toward the NEXT threshold; the host
skip advances with zero votes; colours are unique on join, profile
edit, bot edit, and Add BOT; Teams random/manual/Alone assignment,
team-average scoring (100 + 300 → 200), team-count validation, and
Teams results carrying both team standings and individual rankings;
the profile and bot-edit boxes use the compact class (~210 px max).

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
