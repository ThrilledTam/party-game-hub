# Party Game Hub — v2.1

A free, simple online party-game hub for 1–8 players (humans + bots),
built for Matthew and his cousins. Simple 2D, text and coloured shapes
only — no fancy graphics, no accounts, no sign-in.

**Games:** Reaction Test, Speed Math, Speed Trivia, Secret Word /
Impostor, President / Ditchdiggers / Asshole, UNO, Memory, and
Elimination mode (a series of quick challenges). After v2 the hub is
at its initial playable phase for the cousins.

**v2.1 (playtest fixes, 2026-11-09):** President card selection is
now unambiguous (client-side legality, plain-English hints, specific
rejection reasons, visible turn countdown, and a notice when the
server auto-plays on timeout); Memory bots were retuned so Medium
and especially Hard bots survive much longer; Elimination mode is
paced with a NEXT vote break and a challenge intro between
challenges. UNO is unchanged — it playtested great.

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
- **Game picker:** host picks Reaction Test, Speed Math, Speed
  Trivia, Secret Word / Impostor, President, UNO, Memory, or
  Elimination from a dropdown, or 🎲 Random. The Start button stays
  disabled until the minimum player count is met (bots count).
- **Minimums (v2):** Reaction Test 1 (solo practice) / 2+ competition;
  Speed Math 1–8; Speed Trivia 1–8; Memory 2–8; Secret Word /
  Impostor 4–8; President 4–8; UNO 2–8; Elimination 4–8.
  Max 8 players including bots.
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
  10 / 15 / 20 s per question. 144 questions embedded in
  `questions.py` (36 per category) — no external API. Same scoring
  and bot behaviour as Speed Math.
- **Secret Word / Impostor (v2, min 4):** one impostor with 4–5
  players, two with 6–8, chosen at random and kept secret. Every
  non-impostor sees the same secret word privately; impostors see
  "You are the IMPOSTOR — figure out the word from the clues, don't
  get caught." The word comes from a bank of 72 common nouns
  (animals, food, places, objects, jobs) embedded in `server.py`,
  each with 3–4 one-word clues the bots use. Play is one clue round
  — each player in turn submits ONE word as their clue (30 s per
  player; a timeout becomes a vague default clue "thing") — then a
  vote round where everyone votes for who they think is the
  impostor (30 s; bots vote for the player whose clue was vaguest,
  or at random among non-themselves; impostor bots submit vague
  words like "thing" / "big" / "useful"). The player with the most
  votes is revealed: if they are an impostor, the word-team wins;
  if not, the impostor(s) win. **A tie in votes eliminates nobody
  and the impostors win.** Scoring: every winner +1000; on an
  impostor win each impostor gets +1500 instead (they beat the
  majority). The results screen names the word, the impostors, and
  the winners. In Teams mode individuals are scored as usual and
  team averages follow.
- **President / Ditchdiggers / Asshole (v2, min 4):** a standard
  52-card deck, all cards dealt as evenly as possible. The player
  with the 3 of clubs leads the first trick. Ranks run 3 (low) to 2
  (high); suits don't matter except that 3♣ leads. On your turn
  play a set of 1–4 cards of the SAME rank that beats the current
  trick (same set size, higher rank), or Pass. When everyone else
  passes, the last player to play leads a new trick with any set.
  First player to shed all cards is President, then Vice-President,
  then middle players, second-to-last is Vice-Asshole, last is
  Asshole; if only one player still holds cards they are the
  Asshole and the game ends. Scoring by finish order: President
  1000, Vice-President 750, middle players 500, Vice-Asshole 250,
  Asshole 0 — scaled linearly to the player count (4 players:
  1000 / 667 / 333 / 0; 5: 1000 / 750 / 500 / 250 / 0; 8: 1000 /
  857 / 714 / 571 / 429 / 286 / 143 / 0). Bots play the lowest set
  that beats the trick, otherwise pass, and lead with their lowest
  set. Plays are validated server-side; illegal plays are rejected
  with an error, never trusted from the client. Turn timeout is
  30 s (host setting): a bot or idle player auto-plays their lowest
  legal set when leading, or passes.
  - **v2.1 selection UX:** the key rule is that a play must match
    the trick's SIZE *and* beat its rank — one higher card does
    NOT beat a pair. The UI now says so at all times ("Beat a pair
    of 8s — pick 2 cards of the same rank, 9 or higher"), cards
    that can legally be played are highlighted green and the rest
    dimmed, the Play button is disabled while nothing (or nothing
    legal) is selected, and a rejected play keeps your selection
    and tells you exactly why ("You picked 1 card but the trick
    needs 2"). A visible turn countdown warns in the last 10 s,
    and when the server auto-plays on timeout it announces it
    ("Matthew ran out of time — lowest card played
    automatically"), so it never looks like a random card.
- **UNO (v2, min 2):** a standard 108-card deck — 4 colours ×
  numbers 0–9 (0 once, 1–9 twice), Skip / Reverse / Draw 2 (twice
  per colour), Wild and Wild Draw 4 (4 each). 7 cards each; the
  top card starts the discard pile (re-drawn if it's a Wild Draw
  4). Match by colour, number, or symbol; Wilds are playable
  anytime and the Wild player picks the next colour. Draw 2: next
  player draws 2 and loses their turn. Skip: next player loses
  their turn. Reverse: reverses direction (with 2 players it acts
  as a Skip). Wild Draw 4: next player draws 4 and loses their
  turn. No stacking, no Wild Draw 4 challenge. If the draw pile
  runs out, the discard pile is reshuffled (keeping the top card).
  On your turn play a matching card or draw 1 (a drawn card may be
  played immediately if it matches, otherwise the turn passes).
  First player to empty their hand wins. Scoring: winner 1000,
  everyone else 0 — the results screen lists everyone by cards
  remaining, fewest first. Bots play a matching card if they have
  one (preferring number cards and saving Wilds for last),
  otherwise draw; a bot's Wild colour is its most common colour
  in hand. Turn timeout is 30 s (host setting): an idle player
  auto-draws.
- **Memory (v2, min 2):** each surviving player gets a UNIQUE
  4-digit number (no two players share a number in the same
  round). The host sets the memorize time in the game settings
  (default 10 s, range 5–30 s). Memorize phase: the number is shown
  for that long, then hidden. Recall phase: each player types the
  number from memory — **15 s to answer** (fixed, not the
  per-question timer). Wrong answer or timeout = ELIMINATED from
  the game. Right answer = survive to the next round, where the
  number is one digit longer: 5 digits, then 6, 7, 8, and so on
  with no cap. Rounds continue until ONE player remains — they are
  the winner. **Tie rule:** if all remaining players are
  eliminated in the same round (nobody left, no clear winner),
  those tied players redo the SAME digit-length round with NEW
  respective numbers, repeating until one of them gets it right.
  Scoring: winner 1000; eliminated players score **100 per round
  they survived** — a player eliminated in round 3 survived rounds
  1 and 2, so they score 200. Bots recall with a difficulty-based
  per-digit error chance that *grows* with the number's length, so
  bots breeze through the early rounds and fail gradually as the
  digits grow (v2.1 retune — the old flat rates let a Hard bot lose
  within 3 rounds ~26% of the time). Simulated mean last digit
  survived: Easy ~4.8, Medium ~6.3, Hard ~8.3. At 4 digits Easy is
  ~83% correct, Medium ~94%, Hard ~98%.
- **Elimination mode (v2, min 4):** a series of quick challenges.
  Each challenge is one game picked at random from the pool of
  games that produce a clear per-challenge ranking: **Reaction
  Test, Speed Math, Speed Trivia, Memory**. Secret Word, President
  and UNO are NOT in the pool — they don't produce a per-challenge
  last-place ranking and they take too long for a challenge round.
  A game is only in the pool if the current remaining player count
  meets its minimum (Memory needs 2+, the others 1+), so the pool
  is filtered generically and shrinks as players drop out. Each
  challenge is played by the REMAINING players only (eliminated
  players spectate) and kept short: Reaction 3 rounds, Math/Trivia
  3 questions at 10 s, Memory as normal but capped at 5 rounds
  (digits 4–8; if several survivors remain, they are ranked by
  total recall time, slowest last). After each challenge the
  LAST-PLACE player is ELIMINATED from the series ("X is
  eliminated!"). Repeat until exactly 3 players remain. **Finale:**
  when 3 remain, one final challenge is picked at random; its
  ranking eliminates the BOTTOM 2 players and crowns the top
  player as the victor of the whole series. The results screen
  lists every player in finishing order — victor 1st, finale 2nd
  and 3rd, then earlier eliminated players in reverse elimination
  order (most recently eliminated first). Scoring: victor 1000,
  2nd 500, 3rd 250, earlier eliminated 100 per challenge they
  survived. **Pacing (v2.1):** after EVERY challenge — including
  the finale, before the final results — there is a break screen
  showing the challenge ranking, who was eliminated, how many
  players remain, and the finishing order so far, with a NEXT
  vote: at least half of the connected humans *still alive* vote
  NEXT (bots don't vote, eliminated spectators don't count), or
  the 60 s timeout, or the host's Skip button. Nothing advances
  until the break ends. Before each challenge (including the first
  and the finale) there is a 4-second intro naming the game and
  who is still in, so nobody is ambushed by a new game starting.
  **End votes:** two community votes on the results
  screen, "Play Again" and "Return to Lobby". Humans only (bots
  don't vote); at least half of connected humans voting Play Again
  starts a NEW elimination series with the same players; at least
  half voting Return to Lobby (or a 60 s timeout) goes back to the
  lobby. Live counts for both votes are shown. **The host has NO
  override here — the community decides.** Elimination runs in
  FFA only: if Teams mode is on, the series still scores
  individuals and the team display is hidden. Bots count as
  players throughout and can be eliminated or win.
- **After a game:** results screen with rankings, then **Play Again**
  (back to lobby, same players) or **Change Game**.
- **NEXT QUESTION vote (v1.1, Speed Math / Speed Trivia; v2 adds
  Reaction Test):** after each question (except the last) the
  scoreboard stays up with a **NEXT QUESTION** button. The game
  advances when at least half of the players in the room have
  voted NEXT, or after 60 seconds, whichever comes first. The live
  vote count is shown (e.g. "2/3 voted NEXT"). **Reaction Test
  (v2):** the same vote break now appears between rounds (after
  each round except the last), showing that round's times; the
  1.5 s pause only remains as a fallback if the vote is somehow
  skipped.
  - **v1.2 change: bots do NOT vote and do NOT count toward the
    threshold.** The threshold is now ceil(connected *human* players
    / 2) — bots can no longer decide the break. A solo player with
    bots is never stuck: with 1 human the threshold is 1, so your
    own vote advances, and the 60 s timeout is the fallback.
  - **v1.2: host NEXT override.** On the vote-break screen the host
    also gets an orange **"HOST: Skip to next question →"** button
    that advances immediately, regardless of votes. (No host
    override on the Elimination end votes — the community decides
    there.)
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
the 8 games and checks scores/results are produced. It also checks
the trivia bank has 144 questions with no duplicates.

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

**v2 additions:** unit tests for the Secret Word vote tally (impostor
caught / innocent voted out / tie = impostors win) and the 60+ word
bank, President legality / lowest-legal-set / 52-card deck / scoring
scale, UNO 108-card deck / matching / bot card choice / wild colour,
and Memory bot recall rates (Hard beats Easy); full-game integration
tests for Secret Word (2 humans + 2 bots, vote out a bot suspect),
President (2 humans + 2 bots, lowest-set bot-style play, finish order
and 1000/0 scores), UNO (2 humans + 1 bot, winner empties their hand
for 1000), Memory (3 humans: one correct survives, two wrong score 0;
plus a 2-player tie-redo where both are wrong in round 1 and the redo
is the same 4-digit length), the Reaction vote break between rounds
(vote round 1, host skip round 2), and an Elimination series end-to-end
with 2 humans + 2 bots (challenges only from the quick-game pool,
one elimination per challenge, finale crowns a victor, finishing
order lists all 4 players, victor 1000 / 2nd 500 / 3rd 250, Play Again
starts a second series, Return to Lobby ends back in the lobby).

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
  game logic. Game handlers: `run_reaction_game`, `run_quiz_game`,
  `run_secret_game`, `run_president_game`, `run_uno_game`,
  `run_memory_game`, `run_elimination_game` (+ `run_challenge`,
  `elimination_end_vote`).
- `questions.py` — 144 embedded Speed Trivia questions (36 per
  category, 48 per difficulty).
- `static/index.html` — the whole frontend (home / lobby / game /
  results), plain HTML/CSS/JS, mobile-friendly.
- `tests/test_hub.py` — automated end-to-end test (see above).
- `requirements.txt`, `README.md` — this file.
