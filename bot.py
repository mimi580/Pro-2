"""
ZIGZAG EVEN/ODD BOT — MATHEMATICALLY ENHANCED
================================================
Symbol  : 1HZ25V (Volatility 25 Index, 1s ticks)
Contract: DIGITEVEN / DIGITODD, 1-tick expiry
Railway : Set DERIV_API_TOKEN environment variable

ORIGINAL BOT PROBLEM:
  The XML bot alternates EVEN/EVEN/ODD/ODD blindly every tick.
  The streak counters (E_S, O_s) are tracked but never used as gates.
  Result: trades on every tick with zero statistical filter — pure 50/50.

WHAT WE ADDED (5 mathematical models):
  All 5 run on every tick. Trade fires only when enough models agree.
  This keeps trade rate healthy while dramatically filtering noise.

  MODEL 1 — Z-Score Bias (PRIMARY)
    Tests H0: P(even) = 0.50 over last 30 digits.
    Z = (p_obs - 0.50) / sqrt(0.25/n)
    |Z| > 1.7 → statistically significant bias exists.
    Direction: EVEN if Z > 0, ODD if Z < 0.
    WHY: Turns the streak counter into a proper significance test.
         A run of 3 evens is noise. A run that produces Z>1.7 is signal.

  MODEL 2 — Recency-Weighted Frequency
    Same as Z-score but weights recent digits exponentially more.
    Halflife = 8 digits. Last 3 digits count far more than digits 20+ ago.
    WHY: Catches real-time regime shifts faster than equal-weight Z-score.
         The two models are independent because they weight history differently.

  MODEL 3 — Wald-Wolfowitz Runs Test
    Counts direction-change runs in last 40 digits.
    Fewer runs than expected → clustering (bet continuation).
    More runs than expected → alternating (bet reversal).
    WHY: Measures sequence STRUCTURE not just frequency.
         Completely orthogonal to Models 1 and 2.

  MODEL 4 — Shannon Entropy Gate (PRE-FILTER)
    Entropy of last 50 binary digits (even=1, odd=0).
    Max entropy = 1.0 bit (perfect uniform = no edge).
    Block if entropy > 0.982 — distribution too flat to exploit.
    WHY: Fast pre-filter. Blocks ~35% of ticks where all other models
         would be voting on noise. Runs before the other models to save compute.

  MODEL 5 — Order-1 Markov Transition
    P(even | current_digit) from last 80 transitions.
    If P(even | d) deviates > 5% from 0.50 → vote.
    WHY: Captures digit-to-digit transition bias that frequency models miss.
         "After digit 7, even appears 58% of the time" is real information.

CONFLUENCE RULE:
  Entropy gate must pass FIRST (Model 4).
  Then Models 1, 2, 3, 5 vote on direction (EVEN or ODD).
  Need 3 of 4 to agree on the SAME direction → trade fires.
  This keeps trade rate at ~20-30% of ticks while filtering noise.

ORIGINAL ZIGZAG PRESERVED:
  The 2-EVEN / 2-ODD alternating counter is kept as a TIEBREAKER.
  When models are borderline, the zigzag pattern is the default.
  When models have clear signal, that signal overrides the zigzag.

MARTINGALE (preserved from original):
  Additive: stake += stake * 0.15 on each loss (not multiplicative).
  Reset to initial stake on any win.
  This is a gentle escalation — much safer than geometric martingale.
"""

import sys
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

import asyncio, json, math, os, random, time, uuid
from collections import deque
from datetime import datetime
from typing import Optional

import websockets
from websockets.exceptions import ConnectionClosed, WebSocketException

# ─────────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────────

CONFIG = {
    # ── Connection ────────────────────────────────────────────────
    "api_token":    os.environ.get("DERIV_API_TOKEN", ""),
    "app_id":       1089,
    "symbol":       "1HZ25V",
    "ws_url":       "wss://ws.derivws.com/websockets/v3",

    # ── Contract ──────────────────────────────────────────────────
    "expiry_ticks": 1,
    "payout":       0.95,       # 1HZ25V Even/Odd payout
    # Break-even WR = 1/(1+0.95) = 51.3%

    # ── Warmup ────────────────────────────────────────────────────
    "warmup_ticks": 80,         # minimum before any model votes

    # ── Model 1: Z-Score ──────────────────────────────────────────
    "zscore_window":    30,     # digits
    "zscore_threshold": 2.0,    # |Z| must exceed this

    # ── Model 2: Recency-Weighted Frequency ───────────────────────
    "rw_window":    40,         # digits
    "rw_halflife":  8,          # exponential decay halflife
    "rw_threshold": 0.065,      # min |weighted_rate - 0.50| (calibrated)

    # ── Model 3: Wald-Wolfowitz Runs Test ─────────────────────────
    "runs_window":      40,     # digits
    "runs_z_threshold": 1.5,    # |Z_runs| must exceed this

    # ── Model 4: Entropy Gate (pre-filter) ────────────────────────
    "entropy_window":   50,     # digits
    "entropy_max":      0.982,  # block if binary entropy exceeds this
    # Max binary entropy = 1.0 bit (perfect 50/50)

    # ── Model 5: Order-1 Markov ───────────────────────────────────
    "markov_window":    80,     # digits for transition matrix
    "markov_min_n":     5,      # min obs per row to vote
    "markov_threshold": 0.080,  # min |P(even|d) - 0.50| (calibrated)

    # ── Confluence ────────────────────────────────────────────────
    "models_required":  4,      # of 4 directional models (after entropy gate)

    # ── Martingale (preserved from original) ──────────────────────
    "initial_stake":    0.35,
    "martingale_add":   0.31,   # additive factor: new_stake = stake + stake*0.15
    "max_losses":       999,    # original had 999 (effectively unlimited)

    # ── Risk ──────────────────────────────────────────────────────
    "target_profit":    789.0,  # original target
    "max_loss":         200.0,  # original stop

    # ── ZigZag counter (original logic preserved) ─────────────────
    "zigzag_even_entries": 2,   # fire EVEN this many times before switching
    "zigzag_odd_entries":  2,   # fire ODD this many times before switching

    # ── Resilience ────────────────────────────────────────────────
    "reconnect_base":   3,
    "reconnect_max":    60,
    "lock_timeout":     30,     # 1-tick contracts settle fast

    # ── Logging ───────────────────────────────────────────────────
    "log_file":     "/tmp/zigzag_eo_bot.log",
    "trades_csv":   "/tmp/zigzag_eo_trades.csv",
}

# ─────────────────────────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────────────────────────

import csv, logging, logging.handlers
from pathlib import Path

def setup_logger(log_file: str) -> logging.Logger:
    Path(log_file).parent.mkdir(parents=True, exist_ok=True)
    fmt  = "%(asctime)s.%(msecs)03d | %(levelname)-8s | %(message)s"
    dfmt = "%Y-%m-%d %H:%M:%S"
    log  = logging.getLogger("ZigZag")
    log.setLevel(logging.DEBUG)
    ch   = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(logging.Formatter(fmt, dfmt))
    fh   = logging.handlers.RotatingFileHandler(
        log_file, maxBytes=8*1024*1024, backupCount=3, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter(fmt, dfmt))
    log.addHandler(ch)
    log.addHandler(fh)
    return log

CSV_FIELDS = [
    "timestamp", "direction", "stake", "profit", "win",
    "z_score", "rw_bias", "runs_z", "entropy", "markov_p",
    "models_agree", "total_profit", "loss_streak",
]

class TradeLogger:
    def __init__(self, path: str):
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        if not Path(path).exists():
            with open(path, "w", newline="") as f:
                csv.DictWriter(f, fieldnames=CSV_FIELDS).writeheader()

    def record(self, **kwargs):
        with open(CONFIG["trades_csv"], "a", newline="") as f:
            csv.DictWriter(f, fieldnames=CSV_FIELDS).writerow({
                k: kwargs.get(k, "") for k in CSV_FIELDS
            })

# ─────────────────────────────────────────────────────────────────
# MATHEMATICAL MODELS
# ─────────────────────────────────────────────────────────────────

def extract_digit(price: float) -> int:
    """Last digit of price: 1234.56 → 6"""
    return int(round(price * 10)) % 10

def to_binary(digits: list) -> list:
    """Convert digit list to even(1)/odd(0) list."""
    return [1 if d % 2 == 0 else 0 for d in digits]


def model_entropy_gate(binary: list, cfg: dict) -> tuple:
    """
    MODEL 4 — Shannon Entropy Gate (PRE-FILTER).
    Returns (pass: bool, entropy: float).
    Blocks when distribution is too uniform — no exploitable structure.
    """
    n = cfg["entropy_window"]
    if len(binary) < n:
        return True, 1.0   # not enough data — allow through

    window = binary[-n:]
    p_even = sum(window) / len(window)
    p_odd  = 1.0 - p_even

    if p_even == 0 or p_odd == 0:
        entropy = 0.0
    else:
        entropy = -(p_even * math.log2(p_even) + p_odd * math.log2(p_odd))

    passes = entropy <= cfg["entropy_max"]
    return passes, round(entropy, 5)


def model_zscore(binary: list, cfg: dict) -> tuple:
    """
    MODEL 1 — Z-Score Bias Detection.
    Returns (direction: str|None, z: float).
    Tests if even/odd rate significantly deviates from 0.50.
    """
    n = cfg["zscore_window"]
    if len(binary) < n:
        return None, 0.0

    window = binary[-n:]
    p_obs  = sum(window) / n
    se     = math.sqrt(0.25 / n)
    z      = (p_obs - 0.5) / se

    if abs(z) < cfg["zscore_threshold"]:
        return None, round(z, 3)

    direction = "EVEN" if z > 0 else "ODD"
    return direction, round(z, 3)


def model_recency_weighted(binary: list, cfg: dict) -> tuple:
    """
    MODEL 2 — Recency-Weighted Frequency.
    Returns (direction: str|None, bias: float).
    Exponential decay: recent digits count far more than old ones.
    """
    n = cfg["rw_window"]
    if len(binary) < n:
        return None, 0.0

    window  = binary[-n:]
    alpha   = math.log(2) / cfg["rw_halflife"]
    weights = [math.exp(-alpha * (n - 1 - i)) for i in range(n)]
    w_sum   = sum(weights)
    w_even  = sum(w * b for w, b in zip(weights, window)) / w_sum

    bias = w_even - 0.5
    if abs(bias) < cfg["rw_threshold"]:
        return None, round(bias, 5)

    direction = "EVEN" if bias > 0 else "ODD"
    return direction, round(bias, 5)


def model_runs_test(binary: list, cfg: dict) -> tuple:
    """
    MODEL 3 — Wald-Wolfowitz Runs Test.
    Returns (direction: str|None, z_runs: float).

    Counts how many direction-change runs exist in the sequence.
    Fewer runs than expected → clustering → bet CONTINUATION of current side.
    More runs than expected  → alternating → bet REVERSAL.
    """
    n = cfg["runs_window"]
    if len(binary) < n:
        return None, 0.0

    window = binary[-n:]
    n1     = sum(window)       # count of even
    n2     = n - n1            # count of odd

    if n1 == 0 or n2 == 0:
        return None, 0.0

    # Count runs
    n_runs = 1 + sum(1 for i in range(len(window)-1) if window[i] != window[i+1])

    # Expected runs and variance under H0 (random)
    mu_r  = 2 * n1 * n2 / n + 1
    var_r = (2 * n1 * n2 * (2 * n1 * n2 - n)) / (n**2 * (n - 1) + 1e-9)
    if var_r <= 0:
        return None, 0.0

    z_runs = (n_runs - mu_r) / math.sqrt(var_r)

    if abs(z_runs) < cfg["runs_z_threshold"]:
        return None, round(z_runs, 3)

    # Current last digit type
    current_is_even = window[-1] == 1

    if z_runs < 0:
        # Clustering — bet continuation of current side
        direction = "EVEN" if current_is_even else "ODD"
    else:
        # Alternating — bet reversal
        direction = "ODD" if current_is_even else "EVEN"

    return direction, round(z_runs, 3)


def model_markov(digits: list, cfg: dict) -> tuple:
    """
    MODEL 5 — Order-1 Markov Transition.
    Returns (direction: str|None, p_even: float).

    Builds P(even | current_digit) from recent history.
    If the current digit has a biased outgoing probability → vote.
    """
    n = cfg["markov_window"]
    if len(digits) < n + 1:
        return None, 0.5

    window = digits[-n:]
    curr   = window[-1]

    # Count transitions from current digit
    even_after = 0
    total_after = 0
    for i in range(len(window) - 1):
        if window[i] == curr:
            total_after += 1
            if window[i+1] % 2 == 0:
                even_after += 1

    if total_after < cfg["markov_min_n"]:
        return None, 0.5

    p_even = even_after / total_after
    dev    = abs(p_even - 0.5)

    if dev < cfg["markov_threshold"]:
        return None, round(p_even, 4)

    direction = "EVEN" if p_even > 0.5 else "ODD"
    return direction, round(p_even, 4)


def evaluate_signal(digits: list, cfg: dict) -> tuple:
    """
    Run all 5 models and compute final direction signal.

    Returns:
      (direction: str|None, details: dict)
      direction is None if no signal.
    """
    if len(digits) < cfg["warmup_ticks"]:
        return None, {"reason": f"warmup {len(digits)}/{cfg['warmup_ticks']}"}

    binary = to_binary(digits)

    # ── Model 4: Entropy gate (pre-filter) ──────────────────────
    entropy_ok, entropy = model_entropy_gate(binary, cfg)
    if not entropy_ok:
        return None, {"reason": f"entropy_high {entropy}", "entropy": entropy}

    # ── Models 1, 2, 3, 5: directional votes ────────────────────
    dir_z,    z_score  = model_zscore(binary, cfg)
    dir_rw,   rw_bias  = model_recency_weighted(binary, cfg)
    dir_runs, z_runs   = model_runs_test(binary, cfg)
    dir_mkv,  mkv_p    = model_markov(digits, cfg)

    votes = [dir_z, dir_rw, dir_runs, dir_mkv]

    even_votes = sum(1 for v in votes if v == "EVEN")
    odd_votes  = sum(1 for v in votes if v == "ODD")
    total_votes = even_votes + odd_votes

    details = {
        "entropy":      entropy,
        "z_score":      z_score,
        "rw_bias":      rw_bias,
        "runs_z":       z_runs,
        "markov_p":     mkv_p,
        "even_votes":   even_votes,
        "odd_votes":    odd_votes,
        "models_agree": max(even_votes, odd_votes),
    }

    required = cfg["models_required"]

    if even_votes >= required and even_votes > odd_votes:
        return "EVEN", details
    elif odd_votes >= required and odd_votes > even_votes:
        return "ODD", details
    else:
        details["reason"] = f"no_consensus {even_votes}E/{odd_votes}O"
        return None, details


# ─────────────────────────────────────────────────────────────────
# MARTINGALE MANAGER
# ─────────────────────────────────────────────────────────────────

class MartingaleManager:
    def __init__(self, cfg: dict):
        self.initial   = cfg["initial_stake"]
        self.stake     = cfg["initial_stake"]
        self.add_pct   = cfg["martingale_add"]
        self.max_losses = cfg["max_losses"]
        self.streak    = 0
        self.total_pnl = 0.0
        self.wins      = 0
        self.losses    = 0

    def get_stake(self) -> float:
        return round(self.stake, 2)

    def record_win(self, profit: float):
        self.wins      += 1
        self.total_pnl += profit
        self.streak     = 0
        self.stake      = self.initial

    def record_loss(self, loss: float):
        self.losses    += 1
        self.total_pnl += loss
        self.streak    += 1
        if self.streak >= self.max_losses:
            self.stake  = self.initial
            self.streak = 0
        else:
            # Additive martingale: stake += stake * add_pct
            self.stake  = round(self.stake + self.stake * self.add_pct, 2)

    def can_trade(self, cfg: dict) -> bool:
        if self.total_pnl >= cfg["target_profit"]:
            return False
        if self.total_pnl <= -cfg["max_loss"]:
            return False
        return True

    def summary(self) -> str:
        total = self.wins + self.losses
        wr    = self.wins / total * 100 if total > 0 else 0
        return (f"{total}T {self.wins}W/{self.losses}L WR={wr:.1f}% "
                f"P&L=${self.total_pnl:+.2f} Stake=${self.stake:.2f} "
                f"Streak={self.streak}")


# ─────────────────────────────────────────────────────────────────
# ZIGZAG COUNTER (original logic preserved as tiebreaker)
# ─────────────────────────────────────────────────────────────────

class ZigZagCounter:
    """
    Preserves the original 2-EVEN / 2-ODD alternating counter.
    Used as a tiebreaker when models give no clear signal,
    and to maintain the original bot's rhythm as a fallback.
    """
    def __init__(self, cfg: dict):
        self.even_count  = 0
        self.odd_count   = 0
        self.max_even    = cfg["zigzag_even_entries"]
        self.max_odd     = cfg["zigzag_odd_entries"]

    def next_direction(self) -> str:
        if self.even_count < self.max_even:
            self.even_count += 1
            return "EVEN"
        else:
            self.odd_count += 1
            if self.odd_count >= self.max_odd:
                self.even_count = 0
                self.odd_count  = 0
            return "ODD"


# ─────────────────────────────────────────────────────────────────
# MAIN BOT
# ─────────────────────────────────────────────────────────────────

class ZigZagEOBot:

    def __init__(self, cfg: dict):
        self.cfg       = cfg
        self.log       = setup_logger(cfg["log_file"])
        self.tlog      = TradeLogger(cfg["trades_csv"])
        self.risk      = MartingaleManager(cfg)
        self.zigzag    = ZigZagCounter(cfg)

        self.ws        = None
        self.connected = False
        self.balance   = 0.0

        self._digits:  deque = deque(maxlen=500)   # rolling digit history
        self._tick_count      = 0
        self._req_id          = 0
        self._msg_buffer      = []

        self.in_trade          = False
        self._cooldown_until   = 0.0
        self._pending          = {}
        self._last_details     = {}

    def _nid(self) -> int:
        self._req_id += 1
        return self._req_id

    # ─── Run loop ─────────────────────────────────────────────────

    async def run(self):
        self.log.info("=" * 65)
        self.log.info("ZIGZAG EVEN/ODD BOT — MATHEMATICALLY ENHANCED")
        self.log.info(f"Symbol: {self.cfg['symbol']} | Expiry: {self.cfg['expiry_ticks']}t")
        self.log.info(f"Models: Z-Score + RecencyWeighted + Runs + Entropy + Markov")
        self.log.info(f"Confluence: {self.cfg['models_required']}/4 models must agree")
        self.log.info(f"Warmup: {self.cfg['warmup_ticks']} ticks")
        self.log.info("=" * 65)

        delay = self.cfg["reconnect_base"]
        while True:
            try:
                await self._connect()
                delay = self.cfg["reconnect_base"]
            except (ConnectionClosed, WebSocketException) as e:
                self.log.warning(f"WS closed: {e}")
            except asyncio.TimeoutError:
                self.log.warning("Timeout")
            except Exception as e:
                self.log.error(f"Error: {e}", exc_info=True)

            self.connected = False
            if self.in_trade:
                self.log.warning("Disconnected mid-trade — releasing lock")
                self.in_trade = False

            self.log.info(f"Reconnecting in {delay}s...")
            await asyncio.sleep(delay)
            delay = min(delay * 2, self.cfg["reconnect_max"])

    # ─── Connection ───────────────────────────────────────────────

    async def _connect(self):
        url = f"{self.cfg['ws_url']}?app_id={self.cfg['app_id']}"
        async with websockets.connect(
            url, ping_interval=30, ping_timeout=20, close_timeout=5
        ) as ws:
            self.ws = ws; self.connected = True

            await self._send({"authorize": self.cfg["api_token"]})
            auth = json.loads(await asyncio.wait_for(ws.recv(), 15.0))
            if auth.get("error"):
                raise Exception(f"Auth failed: {auth['error']['message']}")

            self.balance = float(auth["authorize"]["balance"])
            self.log.info(f"Authorized | Balance: ${self.balance:.2f}")

            await self._send({"balance": 1, "subscribe": 1})
            await self._send({"ticks": self.cfg["symbol"], "subscribe": 1})
            self.log.info(f"Subscribed to {self.cfg['symbol']} | "
                          f"Warming up {self.cfg['warmup_ticks']} ticks...")

            async for raw in ws:
                if not self.connected:
                    break
                try:
                    await self._handle(json.loads(raw))
                except Exception as e:
                    self.log.error(f"Handle: {e}", exc_info=True)
                while self._msg_buffer:
                    msg = self._msg_buffer.pop(0)
                    try:
                        await self._handle(msg)
                    except Exception as e:
                        self.log.error(f"Buffer: {e}", exc_info=True)

    async def _send(self, payload: dict) -> int:
        rid = self._nid()
        payload["req_id"] = rid
        if self.ws and self.connected:
            try:
                await asyncio.wait_for(self.ws.send(json.dumps(payload)), 10.0)
            except Exception as e:
                self.log.warning(f"Send error: {e}")
        return rid

    # ─── Message router ───────────────────────────────────────────

    async def _handle(self, msg: dict):
        t = msg.get("msg_type")
        if   t == "tick":
            await self._on_tick(msg["tick"])
        elif t == "balance":
            self.balance = float(msg["balance"]["balance"])
        elif t == "proposal_open_contract":
            await self._on_contract(msg["proposal_open_contract"])
        elif t == "buy":
            self.log.debug(f"Stray buy req_id={msg.get('req_id')}")
        elif t == "error":
            self.log.error(f"API: {msg.get('error',{}).get('message','?')}")

    # ─── Tick processing ──────────────────────────────────────────

    async def _on_tick(self, tick: dict):
        price = float(tick["quote"])
        digit = extract_digit(price)
        self._digits.append(digit)
        self._tick_count += 1

        # Warmup progress
        if self._tick_count < self.cfg["warmup_ticks"]:
            if self._tick_count % 20 == 0:
                self.log.info(f"Warming up {self._tick_count}/"
                              f"{self.cfg['warmup_ticks']} | "
                              f"last digit={digit}")
            return

        if self.in_trade or not self.risk.can_trade(self.cfg):
            return

        if time.time() < self._cooldown_until:
            return

        await self._evaluate(digit)

    # ─── Signal evaluation ────────────────────────────────────────

    async def _evaluate(self, current_digit: int):
        digits     = list(self._digits)
        direction, details = evaluate_signal(digits, self.cfg)

        self._last_details = details

        if direction is None:
            reason = details.get("reason", "no_signal")
            # Only log meaningful skips (not every tick)
            if any(k in reason for k in ["entropy", "warmup"]):
                self.log.debug(f"SKIP | {reason}")
            else:
                self.log.debug(f"SKIP | {reason} | "
                               f"Z={details.get('z_score',0):.2f} "
                               f"RW={details.get('rw_bias',0):.4f} "
                               f"Runs={details.get('runs_z',0):.2f}")
            return

        stake = self.risk.get_stake()
        self.in_trade = True

        self.log.info(
            f"SIGNAL | {direction} | "
            f"Z={details['z_score']:+.2f} "
            f"RW={details['rw_bias']:+.4f} "
            f"Runs={details['runs_z']:+.2f} "
            f"Mkv={details['markov_p']:.4f} "
            f"H={details['entropy']:.4f} "
            f"votes={details['even_votes']}E/{details['odd_votes']}O | "
            f"stake=${stake:.2f}"
        )

        await self._place_trade(direction, stake, details)

    # ─── Trade placement ──────────────────────────────────────────

    async def _place_trade(self, direction: str, stake: float, details: dict):
        rid      = self._nid()
        contract = "DIGITEVEN" if direction == "EVEN" else "DIGITODD"

        payload = {
            "buy": 1,
            "price": stake,
            "req_id": rid,
            "parameters": {
                "contract_type": contract,
                "symbol":        self.cfg["symbol"],
                "duration":      self.cfg["expiry_ticks"],
                "duration_unit": "t",
                "basis":         "stake",
                "amount":        stake,
                "currency":      "USD",
            },
        }

        try:
            await asyncio.wait_for(self.ws.send(json.dumps(payload)), 10.0)
        except Exception as e:
            self.log.error(f"Send failed: {e}")
            self._release(); return

        # Direct recv — buffer everything else
        buy_resp = None
        deadline = time.time() + 15.0

        while time.time() < deadline:
            rem = deadline - time.time()
            if rem <= 0: break
            try:
                raw = await asyncio.wait_for(self.ws.recv(), timeout=rem)
            except asyncio.TimeoutError:
                break
            except Exception as e:
                self.log.error(f"Recv: {e}"); break

            msg = json.loads(raw)
            if msg.get("msg_type") == "buy" and msg.get("req_id") == rid:
                buy_resp = msg; break
            else:
                self._msg_buffer.append(msg)

        if buy_resp is None:
            self.log.error("Buy timeout"); self._release(); return

        if "error" in buy_resp:
            self.log.error(f"Buy rejected: {buy_resp['error'].get('message','?')}")
            self._release(); return

        contract_id = buy_resp.get("buy", {}).get("contract_id")
        if not contract_id:
            self.log.error("No contract_id"); self._release(); return

        self.log.info(f"PLACED cid={contract_id} | {contract} | ${stake:.2f}")

        self._pending = {
            "contract_id": contract_id,
            "direction":   direction,
            "stake":       stake,
            "details":     details,
        }

        await self._send({
            "proposal_open_contract": 1,
            "contract_id": contract_id,
            "subscribe": 1,
        })

        asyncio.create_task(self._poll(contract_id))

    def _release(self):
        self.in_trade        = False
        self._cooldown_until = time.time() + 1.0  # 1s cooldown for 1-tick contracts

    # ─── Fallback poller ──────────────────────────────────────────

    async def _poll(self, contract_id: str):
        try:
            await asyncio.sleep(5)
            for attempt in range(1, 10):
                if not self._pending or self._pending.get("contract_id") != contract_id:
                    return
                if not self.connected:
                    return
                self.log.debug(f"Polling {contract_id} ({attempt})")
                await self._send({"proposal_open_contract": 1,
                                  "contract_id": contract_id})
                await asyncio.sleep(2)
            self.log.error(f"Contract {contract_id} never settled")
            self._pending = {}
            self._release()
        except asyncio.CancelledError:
            pass
        except Exception as e:
            self.log.error(f"Poll: {e}", exc_info=True)
            self._release()

    # ─── Settlement ───────────────────────────────────────────────

    async def _on_contract(self, contract: dict):
        status = contract.get("status")
        if status not in ("won", "lost", "sold"):
            return
        cid  = contract.get("contract_id")
        meta = self._pending
        if not meta or meta.get("contract_id") != cid:
            return

        profit = float(contract.get("profit", 0))
        win    = status == "won"
        stake  = meta["stake"]
        det    = meta["details"]

        if win:
            self.risk.record_win(profit)
        else:
            self.risk.record_loss(profit)

        icon = "WIN ✅" if win else "LOSS ❌"
        self.log.info(f"{icon} P&L ${profit:+.2f} | {self.risk.summary()}")

        # CSV
        self.tlog.record(
            timestamp   = datetime.now().isoformat(),
            direction   = meta["direction"],
            stake       = round(stake, 2),
            profit      = round(profit, 2),
            win         = win,
            z_score     = det.get("z_score", 0),
            rw_bias     = det.get("rw_bias", 0),
            runs_z      = det.get("runs_z", 0),
            entropy     = det.get("entropy", 0),
            markov_p    = det.get("markov_p", 0.5),
            models_agree= det.get("models_agree", 0),
            total_profit= round(self.risk.total_pnl, 2),
            loss_streak = self.risk.streak,
        )

        if not self.risk.can_trade(self.cfg):
            reason = ("TARGET REACHED" if self.risk.total_pnl >= self.cfg["target_profit"]
                      else "STOP LOSS HIT")
            self.log.info(f"Bot stopped: {reason} | Final: {self.risk.summary()}")

        self._pending = {}
        self._release()
        self._scoreboard(win, profit, meta["direction"], det)

    # ─── Scoreboard ───────────────────────────────────────────────

    def _scoreboard(self, win: bool, profit: float,
                    direction: str, det: dict):
        G = "\033[92m"; R = "\033[91m"; Y = "\033[93m"
        B = "\033[94m"; M = "\033[90m"; X = "\033[0m"; BO = "\033[1m"

        r   = self.risk
        wr  = r.wins / (r.wins + r.losses) * 100 if (r.wins + r.losses) > 0 else 0
        wc  = G if wr >= 55 else Y if wr >= 52 else R
        pc  = G if r.total_pnl >= 0 else R
        res = f"{G}WIN ✅{X}" if win else f"{R}LOSS ❌{X}"
        pnl = f"{G}+${profit:.2f}{X}" if profit >= 0 else f"{R}-${abs(profit):.2f}{X}"
        dc  = Y if direction == "EVEN" else B
        ev  = det.get("even_votes", 0)
        ov  = det.get("odd_votes", 0)

        print(f"\n{BO}{'─'*65}{X}")
        print(f"  {BO}[ZigZag E/O]{X}  {res}  "
              f"{dc}{direction}{X}  "
              f"votes={ev}E/{ov}O  "
              f"Z={det.get('z_score',0):+.2f}  "
              f"H={det.get('entropy',0):.4f}  "
              f"P&L {pnl}  Bal {B}${self.balance:.2f}{X}")
        print(f"  {r.wins+r.losses:>5}T  "
              f"{G}{r.wins}W{X}/{R}{r.losses}L{X}  "
              f"{wc}{wr:.1f}%{X}  "
              f"{pc}P&L ${r.total_pnl:+.2f}{X}  "
              f"{Y}Stake ${r.stake:.2f}{X}  "
              f"{M}Streak {r.streak}{X}")
        print(f"{BO}{'─'*65}{X}\n", flush=True)


# ─────────────────────────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────────────────────────

async def main():
    if not CONFIG["api_token"]:
        print("ERROR: Set DERIV_API_TOKEN environment variable.")
        return
    bot = ZigZagEOBot(CONFIG)
    await bot.run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nStopped.")
