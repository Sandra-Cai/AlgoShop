"""
AlgoShop Order Book + Market Entry Timing Engine v2.0
=====================================================
Applies real algorithmic trading concepts to consumer purchases:

1. LIMIT ORDER BOOK (LOB)
   - Shows price distribution: how many buyers paid what price for a product
   - Bid side: prices people are willing to pay (demand)
   - Ask side: prices retailers/resellers are offering (supply)
   - Spread analysis: gap between best bid and best ask
   - Volume-Weighted Average Price (VWAP): the "fair" price
   - Microprice (volume-weighted mid-quote)
   - Order flow imbalance (OFI)

2. MARKET ENTRY TIMING (MET)
   - Analyzes price momentum, volatility, and seasonality
   - Outputs a signal: BUY NOW / WAIT / SET ALERT
   - Computes optimal entry price and expected wait time
   - Uses:
       - Exponential moving averages (EMA) + crossovers
       - Bollinger Bands (2σ envelope)
       - Relative Strength Index (RSI, 14-period)
       - MACD (12/26/9 convergence/divergence)
       - Rate of Change (ROC) momentum
       - Realized volatility
       - Seasonal sinusoidal decomposition for retail cycles
       - Order-book pressure / microstructure signals

References:
  - VWAP: Volume Weighted Average Price (standard equity-trading benchmark)
  - Bollinger Bands: John Bollinger (1980s) — price envelope analysis
  - RSI: J. Welles Wilder Jr. (New Concepts in Technical Trading, 1978)
  - MACD: Gerald Appel (1970s) — trend-following momentum
  - EMA: Exponential Moving Average — recency-weighted trend detection
  - LOB microstructure: Gould et al., 2013 (Quantitative Finance)
  - OFI: Cont, Kukanov, and Stoikov, 2014
"""

import math
import time
import logging
from collections import defaultdict

log = logging.getLogger("order_book")

# ─── Constants ────────────────────────────────────────────────────────────────

NUM_PRICE_LEVELS = 10       # levels each side of the order book
BOOK_DEPTH = 50             # simulated orders per product
HISTORY_DAYS = 90           # days of simulated price history
EMA_SHORT = 7               # 7-day EMA
EMA_FAST = 3                # 3-day EMA
EMA_MACD_FAST = 12          # MACD fast EMA
EMA_MACD_SLOW = 26          # MACD slow EMA
MACD_SIGNAL = 9             # MACD signal line period
RSI_PERIOD = 14             # standard RSI period
BOLLINGER_PERIOD = 20       # 20-day Bollinger Band
BOLLINGER_STD = 2.0         # 2 standard deviations

# Platform catalogs (deterministic order — picked by seeded rand)
BID_PLATFORMS = ["Poshmark", "eBay", "Mercari", "Depop", "ThredUp", "StockX",
                 "GOAT", "Grailed", "Vestiaire", "The RealReal"]
ASK_PLATFORMS = ["Amazon", "Walmart", "Nike.com", "Nordstrom", "Best Buy",
                 "Target", "Gucci.com", "Apple.com", "REI", "Dick's", "Sephora",
                 "Costco"]


def _seeded_random(seed_val):
    """Deterministic linear-congruential random for consistent output per product."""
    state = seed_val & 0x7fffffff
    if state == 0:
        state = 1
    def rand():
        nonlocal state
        state = (state * 1103515245 + 12345) & 0x7fffffff
        return (state >> 16) / 32768.0
    return rand


# ─── Limit Order Book ────────────────────────────────────────────────────────

class OrderBook:
    """
    Limit Order Book for a consumer product.

    In equities, the LOB shows all outstanding buy (bid) and sell (ask) orders.
    We adapt this to shopping:

    - BID SIDE: What buyers have recently paid across platforms
      (represents demand — "I'd buy at this price")
    - ASK SIDE: What retailers/resellers are currently listing
      (represents supply — "Available at this price")
    - SPREAD: Gap between best bid and best ask
      (tight spread = competitive market = good for buyer)
    - VWAP: Volume-weighted average price (fair value)
    - MICROPRICE: Volume-weighted mid (leans toward the heavier side)
    - OFI: Order Flow Imbalance — leading indicator of short-term price direction
    """

    def __init__(self, product_id: str, product_data: dict):
        self.product_id = product_id
        self.name = product_data.get("name", product_id)
        self.msrp = product_data.get("msrp", 100)
        self.price_range = product_data.get(
            "typical_price_range",
            [self.msrp * 0.7, self.msrp * 1.1]
        )
        self.category = product_data.get("category", "")

        self.rand = _seeded_random(hash(product_id) & 0xffffffff)

        # Generate order book
        self.bids: list[dict] = []  # buyer prices (sorted high→low)
        self.asks: list[dict] = []  # seller prices (sorted low→high)
        self._generate_book()

    # ─── Book generation ────────────────────────────────────────────────

    def _pick(self, options: list[str]) -> str:
        """Deterministic pick from a list using the seeded generator."""
        if not options:
            return ""
        idx = int(self.rand() * len(options)) % len(options)
        return options[idx]

    def _generate_book(self):
        """Generate realistic bid/ask levels based on product pricing."""
        lo, hi = self.price_range
        mid = (lo + hi) / 2
        spread_pct = 0.03 + self.rand() * 0.05  # 3-8% spread

        # Bid side: buyer prices cluster below mid
        bid_center = mid * (1 - spread_pct / 2)
        bid_levels = []
        for i in range(NUM_PRICE_LEVELS):
            price = bid_center - (i * (hi - lo) * 0.04)
            price = max(lo * 0.85, price * (0.98 + self.rand() * 0.04))
            quantity = int(5 + self.rand() * 40 + (NUM_PRICE_LEVELS - i) * 8)
            bid_levels.append({
                "price": round(price, 2),
                "quantity": quantity,
                "orders": int(2 + self.rand() * 12),
                "platform": self._pick(BID_PLATFORMS),
                "side": "bid",
            })

        # Ask side: seller prices cluster above mid
        ask_center = mid * (1 + spread_pct / 2)
        ask_levels = []
        for i in range(NUM_PRICE_LEVELS):
            price = ask_center + (i * (hi - lo) * 0.04)
            price = min(hi * 1.15, price * (0.98 + self.rand() * 0.04))
            quantity = int(3 + self.rand() * 25 + (NUM_PRICE_LEVELS - i) * 5)
            ask_levels.append({
                "price": round(price, 2),
                "quantity": quantity,
                "orders": int(1 + self.rand() * 8),
                "platform": self._pick(ASK_PLATFORMS),
                "side": "ask",
            })

        self.bids = sorted(bid_levels, key=lambda x: -x["price"])
        self.asks = sorted(ask_levels, key=lambda x: x["price"])

    # ─── Basic stats ────────────────────────────────────────────────────

    def best_bid(self) -> float:
        return self.bids[0]["price"] if self.bids else 0.0

    def best_ask(self) -> float:
        return self.asks[0]["price"] if self.asks else 0.0

    def best_bid_qty(self) -> int:
        return self.bids[0]["quantity"] if self.bids else 0

    def best_ask_qty(self) -> int:
        return self.asks[0]["quantity"] if self.asks else 0

    def spread(self) -> float:
        return max(0.0, self.best_ask() - self.best_bid())

    def spread_pct(self) -> float:
        mid = self.mid_price()
        return (self.spread() / mid * 100) if mid > 0 else 0.0

    def mid_price(self) -> float:
        return (self.best_bid() + self.best_ask()) / 2

    def microprice(self) -> float:
        """
        Microprice = volume-weighted mid quote.
        microprice = (bid_qty * ask_px + ask_qty * bid_px) / (bid_qty + ask_qty)
        (Stoikov, 2018). Leans toward the heavier side — better predictor of next trade.
        """
        bq, aq = self.best_bid_qty(), self.best_ask_qty()
        if bq + aq == 0:
            return self.mid_price()
        return (bq * self.best_ask() + aq * self.best_bid()) / (bq + aq)

    def vwap(self) -> float:
        """Volume-Weighted Average Price across all levels."""
        total_value = 0.0
        total_qty = 0
        for level in self.bids + self.asks:
            total_value += level["price"] * level["quantity"]
            total_qty += level["quantity"]
        return round(total_value / total_qty, 2) if total_qty > 0 else float(self.msrp)

    def total_bid_volume(self) -> int:
        return sum(l["quantity"] for l in self.bids)

    def total_ask_volume(self) -> int:
        return sum(l["quantity"] for l in self.asks)

    def bid_ask_ratio(self) -> float:
        """Bid/Ask volume ratio. > 1 means more demand than supply."""
        ask_vol = self.total_ask_volume()
        return self.total_bid_volume() / ask_vol if ask_vol > 0 else 1.0

    def order_flow_imbalance(self) -> float:
        """
        OFI ∈ [-1, 1]. +1 = all demand, -1 = all supply.
        OFI = (bid_vol - ask_vol) / (bid_vol + ask_vol)
        """
        b, a = self.total_bid_volume(), self.total_ask_volume()
        tot = b + a
        return (b - a) / tot if tot > 0 else 0.0

    def imbalance(self) -> str:
        """Market imbalance direction label."""
        ratio = self.bid_ask_ratio()
        if ratio > 1.3:
            return "BUYER PRESSURE"
        elif ratio < 0.7:
            return "SELLER PRESSURE"
        return "BALANCED"

    def kyle_lambda(self) -> float:
        """
        Approximate Kyle's lambda (price impact per unit volume).
        Proxied here by spread / sqrt(avg depth) — a rough liquidity cost proxy.
        Higher λ = less liquid = larger price slippage per unit bought.
        """
        avg_depth = (self.total_bid_volume() + self.total_ask_volume()) / 2
        if avg_depth <= 0:
            return 0.0
        return round(self.spread() / math.sqrt(avg_depth), 4)

    def depth_chart_data(self) -> dict:
        """Data for rendering a depth chart (cumulative volume at each price)."""
        bid_cum = []
        cum = 0
        for level in self.bids:
            cum += level["quantity"]
            bid_cum.append({"price": level["price"], "cumulative": cum})

        ask_cum = []
        cum = 0
        for level in self.asks:
            cum += level["quantity"]
            ask_cum.append({"price": level["price"], "cumulative": cum})

        return {"bids": bid_cum, "asks": ask_cum}

    def to_dict(self) -> dict:
        return {
            "product_id": self.product_id,
            "name": self.name,
            "msrp": self.msrp,
            "best_bid": self.best_bid(),
            "best_ask": self.best_ask(),
            "mid_price": round(self.mid_price(), 2),
            "microprice": round(self.microprice(), 2),
            "spread": round(self.spread(), 2),
            "spread_pct": round(self.spread_pct(), 2),
            "vwap": self.vwap(),
            "bid_volume": self.total_bid_volume(),
            "ask_volume": self.total_ask_volume(),
            "bid_ask_ratio": round(self.bid_ask_ratio(), 2),
            "order_flow_imbalance": round(self.order_flow_imbalance(), 3),
            "kyle_lambda": self.kyle_lambda(),
            "imbalance": self.imbalance(),
            "bids": self.bids[:NUM_PRICE_LEVELS],
            "asks": self.asks[:NUM_PRICE_LEVELS],
            "depth_chart": self.depth_chart_data(),
        }


# ─── Price History Generator ─────────────────────────────────────────────────

def generate_price_history(product_id: str, product_data: dict,
                           days: int = HISTORY_DAYS) -> list[dict]:
    """
    Generate realistic daily price history for a product.

    Combines:
      - Random walk with mean-reversion
      - Sinusoidal seasonal component (retail cycles)
      - Discrete sale events (Prime Day, BFCM, end-of-season)
    """
    rand = _seeded_random(hash(product_id + "_history") & 0xffffffff)
    lo, hi = product_data.get("typical_price_range", [80, 120])
    msrp = product_data.get("msrp", 100)
    mid = (lo + hi) / 2
    price_range = max(1e-6, hi - lo)

    history = []
    price = mid + (rand() - 0.5) * price_range * 0.3

    # Sale event days — realistic retail calendar (Prime Day, BFCM, end-of-season)
    sale_days: set[int] = set()
    for base in [30, 60, 80]:
        for offset in range(-2, 3):
            sale_days.add(base + offset)

    # Per-product seasonal phase shift & amplitude
    phase = rand() * 2 * math.pi
    seasonal_amp = price_range * (0.04 + rand() * 0.04)  # 4-8% of range

    for day in range(days):
        # Sinusoidal seasonal component (retail demand cycle ~45-60 days)
        seasonal_wave = seasonal_amp * math.sin(2 * math.pi * day / 52 + phase)

        # Random walk drift
        drift = (rand() - 0.5) * price_range * 0.02

        # Mean reversion to center
        mean_revert = (mid - price) * 0.03

        # Discrete sale drop
        sale_drop = -price_range * 0.08 if day in sale_days else 0.0

        price = price + drift + mean_revert + sale_drop + seasonal_wave * 0.1
        price = max(lo * 0.85, min(hi * 1.12, price))

        # Volume (higher on sale days + mild demand seasonality)
        base_vol = 50 + rand() * 200
        seasonal_vol_boost = max(0, seasonal_wave * 10)
        sale_vol_boost = 300 if day in sale_days else 0
        volume = int(base_vol + seasonal_vol_boost + sale_vol_boost)

        history.append({
            "day": day,
            "price": round(price, 2),
            "volume": volume,
            "is_sale": day in sale_days,
        })

    return history


# ─── Market Entry Timing Engine ──────────────────────────────────────────────

class MarketEntryTiming:
    """
    Algorithmic market entry timing for shopping.

    Indicators:
      1. EMA Crossover (3 vs 7 day) — short-term trend direction
      2. MACD (12/26/9) — trend & momentum
      3. Bollinger Bands (20d, 2σ) — overbought/oversold detection
      4. RSI (14d) — momentum oscillator
      5. Price vs VWAP — fair-value comparison
      6. ROC (7d) — momentum
      7. Realized volatility — regime detection
      8. Seasonal proximity — is a sale event coming?
      9. Order-book pressure & OFI — microstructure signal

    Signal:
      - BUY_NOW: strong buy (below fair value + downtrend + oversold + no imminent sale)
      - WAIT: price likely to drop further (overbought or pre-sale)
      - SET_ALERT: neutral — set a price target
    """

    def __init__(self, product_id: str, product_data: dict):
        self.product_id = product_id
        self.name = product_data.get("name", product_id)
        self.msrp = product_data.get("msrp", 100)
        self.price_range = product_data.get(
            "typical_price_range",
            [self.msrp * 0.7, self.msrp * 1.1]
        )
        self.category = product_data.get("category", "")
        self.history = generate_price_history(product_id, product_data)
        self._order_book = OrderBook(product_id, product_data)

    # ─── Technical indicators ──────────────────────────────────────────

    @staticmethod
    def _ema(prices: list[float], period: int) -> list[float]:
        """Exponential Moving Average."""
        if not prices:
            return []
        k = 2 / (period + 1)
        ema = [prices[0]]
        for i in range(1, len(prices)):
            ema.append(prices[i] * k + ema[-1] * (1 - k))
        return ema

    def _bollinger_bands(self, prices: list[float], period: int = BOLLINGER_PERIOD,
                         num_std: float = BOLLINGER_STD) -> tuple[list, list, list]:
        """Bollinger Bands: middle (SMA), upper, lower."""
        middle, upper, lower = [], [], []
        for i in range(len(prices)):
            window = prices[max(0, i - period + 1):i + 1]
            avg = sum(window) / len(window)
            std = (sum((x - avg) ** 2 for x in window) / len(window)) ** 0.5
            middle.append(avg)
            upper.append(avg + num_std * std)
            lower.append(avg - num_std * std)
        return middle, upper, lower

    def _rsi(self, prices: list[float], period: int = RSI_PERIOD) -> float:
        """
        Relative Strength Index (Wilder, 1978).
        RSI > 70 = overbought (price high), RSI < 30 = oversold (price low).
        For a shopper, low RSI means good time to buy.
        """
        if len(prices) < period + 1:
            return 50.0
        gains, losses = [], []
        for i in range(1, len(prices)):
            diff = prices[i] - prices[i - 1]
            gains.append(max(0.0, diff))
            losses.append(max(0.0, -diff))
        # Wilder's smoothing (running average)
        avg_gain = sum(gains[:period]) / period
        avg_loss = sum(losses[:period]) / period
        for i in range(period, len(gains)):
            avg_gain = (avg_gain * (period - 1) + gains[i]) / period
            avg_loss = (avg_loss * (period - 1) + losses[i]) / period
        if avg_loss == 0:
            return 100.0
        rs = avg_gain / avg_loss
        return 100.0 - (100.0 / (1.0 + rs))

    def _macd(self, prices: list[float]) -> dict:
        """
        MACD = EMA(12) - EMA(26). Signal line = EMA(MACD, 9).
        Histogram = MACD - Signal.
        - Histogram rising/positive = bullish momentum
        - Crossover of MACD above signal = buy-side trigger
        """
        if len(prices) < EMA_MACD_SLOW + MACD_SIGNAL:
            return {"macd": 0.0, "signal": 0.0, "histogram": 0.0, "trend": "flat"}
        ema_fast = self._ema(prices, EMA_MACD_FAST)
        ema_slow = self._ema(prices, EMA_MACD_SLOW)
        macd_line = [f - s for f, s in zip(ema_fast, ema_slow)]
        signal_line = self._ema(macd_line, MACD_SIGNAL)
        macd_val = macd_line[-1]
        signal_val = signal_line[-1]
        hist = macd_val - signal_val
        prev_hist = macd_line[-2] - signal_line[-2] if len(macd_line) >= 2 else 0.0
        if hist > 0 and hist > prev_hist:
            trend = "bullish_strengthening"
        elif hist > 0:
            trend = "bullish"
        elif hist < 0 and hist < prev_hist:
            trend = "bearish_strengthening"
        elif hist < 0:
            trend = "bearish"
        else:
            trend = "flat"
        return {
            "macd": round(macd_val, 4),
            "signal": round(signal_val, 4),
            "histogram": round(hist, 4),
            "trend": trend,
        }

    @staticmethod
    def _roc(prices: list[float], period: int = 7) -> float:
        """Rate of Change (momentum) over N periods, as percent."""
        if len(prices) < period + 1 or prices[-period - 1] == 0:
            return 0.0
        return (prices[-1] - prices[-period - 1]) / prices[-period - 1] * 100

    @staticmethod
    def _volatility(prices: list[float], period: int = 20) -> float:
        """Historical volatility (std dev of log returns), percent."""
        if len(prices) < period + 1:
            return 0.0
        returns = []
        for i in range(max(1, len(prices) - period), len(prices)):
            if prices[i - 1] > 0:
                returns.append(math.log(prices[i] / prices[i - 1]))
        if not returns:
            return 0.0
        avg = sum(returns) / len(returns)
        var = sum((r - avg) ** 2 for r in returns) / len(returns)
        return (var ** 0.5) * 100  # percent

    def _days_to_next_sale(self) -> int:
        """Estimate days until next major sale event."""
        # Use product-specific deterministic offset (not wall clock)
        rand = _seeded_random(hash(self.product_id + "sale") & 0xffffffff)
        # A 30-day cadence with jitter is a reasonable retail calendar proxy
        base_cycle = 30
        offset = int(rand() * base_cycle)
        return max(1, base_cycle - offset)

    # ─── Signal computation ─────────────────────────────────────────────

    def compute_signal(self) -> dict:
        """
        Compute the market entry timing signal.
        Returns a comprehensive analysis dict with a BUY_NOW / WAIT / SET_ALERT signal.
        """
        prices = [h["price"] for h in self.history]
        volumes = [h["volume"] for h in self.history]

        if len(prices) < 2:
            return {
                "product_id": self.product_id,
                "name": self.name,
                "signal": "SET_ALERT",
                "confidence": 50,
                "reason": "Insufficient data",
            }

        current_price = prices[-1]

        # === Indicator 1: EMA crossover ===
        ema_fast = self._ema(prices, EMA_FAST)
        ema_slow = self._ema(prices, EMA_SHORT)
        # For shopping: fast < slow = price trending down = GOOD to buy (bullish-for-buyer)
        ema_signal = "bullish" if ema_fast[-1] < ema_slow[-1] else "bearish"

        # === Indicator 2: Bollinger Bands ===
        bb_mid, bb_upper, bb_lower = self._bollinger_bands(prices)
        if current_price < bb_lower[-1]:
            bb_position = "below_lower"
        elif current_price > bb_upper[-1]:
            bb_position = "above_upper"
        else:
            bb_position = "in_band"
        bb_band_width = bb_upper[-1] - bb_lower[-1]
        bb_pct = ((current_price - bb_lower[-1]) / bb_band_width) if bb_band_width > 1e-9 else 0.5

        # === Indicator 3: Price vs VWAP ===
        vwap = self._order_book.vwap()
        vwap_delta = (current_price - vwap) / vwap * 100 if vwap > 0 else 0
        if vwap_delta < -2:
            price_vs_vwap = "below"
        elif vwap_delta > 2:
            price_vs_vwap = "above"
        else:
            price_vs_vwap = "at_fair"

        # === Indicator 4: Momentum (ROC) ===
        roc = self._roc(prices)
        if roc < -2:
            momentum = "falling"
        elif roc > 2:
            momentum = "rising"
        else:
            momentum = "flat"

        # === Indicator 5: Volatility ===
        vol = self._volatility(prices)
        if vol > 3:
            vol_label = "high"
        elif vol < 1:
            vol_label = "low"
        else:
            vol_label = "normal"

        # === Indicator 6: Sale proximity ===
        days_to_sale = self._days_to_next_sale()
        if days_to_sale <= 7:
            sale_proximity = "imminent"
        elif days_to_sale <= 21:
            sale_proximity = "near"
        else:
            sale_proximity = "far"

        # === Indicator 7: Order book signals ===
        imbalance = self._order_book.imbalance()
        spread_pct = self._order_book.spread_pct()
        ofi = self._order_book.order_flow_imbalance()

        # === Indicator 8: RSI ===
        rsi = self._rsi(prices)
        if rsi < 30:
            rsi_label = "oversold"      # good to buy
        elif rsi > 70:
            rsi_label = "overbought"    # wait
        else:
            rsi_label = "neutral"

        # === Indicator 9: MACD ===
        macd = self._macd(prices)
        macd_hist = macd["histogram"]

        # ─── Composite score ─────────────────────────────────────────────
        # +score = buy now, −score = wait
        score = 0.0
        reasons = []

        # EMA crossover
        if ema_signal == "bullish":  # price trending down
            score += 20
            reasons.append("Price trending downward (fast EMA < slow EMA)")
        else:
            score -= 15
            reasons.append("Price trending upward — may rise further")

        # Bollinger
        if bb_position == "below_lower":
            score += 30
            reasons.append("Below Bollinger lower band — oversold")
        elif bb_position == "above_upper":
            score -= 25
            reasons.append("Above Bollinger upper band — overbought")
        else:
            score += 5 * (1 - bb_pct)

        # RSI
        if rsi_label == "oversold":
            score += 20
            reasons.append(f"RSI {rsi:.0f} — oversold (good buy window)")
        elif rsi_label == "overbought":
            score -= 20
            reasons.append(f"RSI {rsi:.0f} — overbought")

        # MACD — histogram negative & strengthening = prices falling & accelerating down → good buy
        if macd["trend"] == "bearish_strengthening":
            score += 12
            reasons.append("MACD negative & strengthening — downtrend accelerating")
        elif macd["trend"] == "bullish_strengthening":
            score -= 12
            reasons.append("MACD positive & strengthening — uptrend accelerating")

        # VWAP
        if price_vs_vwap == "below":
            score += 20
            reasons.append(f"Below VWAP (${vwap:.0f}) — priced under fair value")
        elif price_vs_vwap == "above":
            score -= 15
            reasons.append(f"Above VWAP (${vwap:.0f}) — above fair value")

        # Momentum
        if momentum == "falling":
            score += 15
            reasons.append("Negative momentum — price still dropping")
        elif momentum == "rising":
            score -= 10

        # Sale proximity (dominant if imminent)
        if sale_proximity == "imminent":
            score -= 30
            reasons.append(f"Major sale event in ~{days_to_sale} days — WAIT")
        elif sale_proximity == "near":
            score -= 10
            reasons.append(f"Sale event possible in ~{days_to_sale} days")

        # OB pressure
        if imbalance == "SELLER PRESSURE":
            score += 10
            reasons.append("Seller pressure — supply exceeds demand")
        elif imbalance == "BUYER PRESSURE":
            score -= 10
            reasons.append("Buyer pressure — demand exceeds supply")

        # OFI microstructure
        if ofi < -0.3:
            score += 6
            reasons.append(f"OFI {ofi:+.2f} — sell-side flow dominates")
        elif ofi > 0.3:
            score -= 6
            reasons.append(f"OFI {ofi:+.2f} — buy-side flow dominates")

        # Wide spread = more room to negotiate / find resale deals
        if spread_pct > 8:
            score += 5
            reasons.append(f"Wide spread ({spread_pct:.1f}%) — check resale markets")

        # High vol regime = bigger opportunities (penalize patience slightly in high-vol)
        if vol_label == "high":
            reasons.append(f"High volatility regime ({vol:.1f}%) — price swings favor timed entries")

        # ─── Determine signal ─────────────────────────────────────────────
        if score >= 25:
            signal = "BUY_NOW"
            confidence = min(95, 60 + score)
        elif score <= -15:
            signal = "WAIT"
            confidence = min(95, 60 + abs(score))
        else:
            signal = "SET_ALERT"
            confidence = 50 + abs(score)

        confidence = max(30, min(95, confidence))

        # ─── Target price ─────────────────────────────────────────────────
        target_price = min(
            bb_lower[-1],
            vwap * 0.97,
            current_price * 0.92,
        )
        target_price = max(self.price_range[0] * 0.9, target_price)

        # ─── Expected wait time ───────────────────────────────────────────
        if signal == "BUY_NOW":
            wait_days = 0
        elif sale_proximity == "imminent":
            wait_days = days_to_sale
        else:
            wait_days = int(7 + abs(score) * 0.3)

        # ─── Price position in range ──────────────────────────────────────
        lo, hi = self.price_range
        range_pct = ((current_price - lo) / (hi - lo)) * 100 if hi > lo else 50

        avg_price = sum(prices) / len(prices)

        return {
            "product_id": self.product_id,
            "name": self.name,
            "signal": signal,
            "confidence": round(confidence),
            "current_price": round(current_price, 2),
            "target_price": round(target_price, 2),
            "savings_if_wait": round(current_price - target_price, 2),
            "savings_pct_if_wait": round((current_price - target_price) / current_price * 100, 1)
                                   if current_price > 0 else 0,
            "wait_days": wait_days,
            "vwap": round(vwap, 2),
            "range_position_pct": round(range_pct, 1),
            "price_range": [round(lo, 2), round(hi, 2)],
            "msrp": self.msrp,
            "reasons": reasons[:5],
            "score": round(score, 1),
            "indicators": {
                "ema_crossover": ema_signal,
                "ema_fast": round(ema_fast[-1], 2),
                "ema_slow": round(ema_slow[-1], 2),
                "bollinger_position": bb_position,
                "bollinger_pct": round(bb_pct * 100, 1),
                "bollinger_upper": round(bb_upper[-1], 2),
                "bollinger_lower": round(bb_lower[-1], 2),
                "bollinger_mid": round(bb_mid[-1], 2),
                "rsi": round(rsi, 1),
                "rsi_label": rsi_label,
                "macd": macd["macd"],
                "macd_signal": macd["signal"],
                "macd_histogram": macd["histogram"],
                "macd_trend": macd["trend"],
                "price_vs_vwap": price_vs_vwap,
                "vwap_delta_pct": round(vwap_delta, 2),
                "momentum": momentum,
                "roc_7d": round(roc, 2),
                "volatility": round(vol, 2),
                "volatility_label": vol_label,
                "sale_proximity": sale_proximity,
                "days_to_sale": days_to_sale,
                "order_book_imbalance": imbalance,
                "order_flow_imbalance": round(ofi, 3),
                "spread_pct": round(spread_pct, 2),
            },
            "price_history_summary": {
                "days": len(prices),
                "min": round(min(prices), 2),
                "max": round(max(prices), 2),
                "avg": round(avg_price, 2),
                "current_vs_avg": round((current_price - avg_price) / avg_price * 100, 2)
                                   if avg_price > 0 else 0,
            },
        }


# ─── Order Book Manager ─────────────────────────────────────────────────────

class OrderBookManager:
    """Manages order books and market entry timing for all tracked products."""

    def __init__(self, products: dict):
        self.products = products
        self.books: dict[str, OrderBook] = {}
        self.timing: dict[str, MarketEntryTiming] = {}
        # Cache signal outputs (they're pure functions of product data)
        self._signal_cache: dict[str, dict] = {}

        for pid, pdata in products.items():
            try:
                self.books[pid] = OrderBook(pid, pdata)
                self.timing[pid] = MarketEntryTiming(pid, pdata)
            except Exception as e:
                log.error(f"Failed to init book/timing for {pid}: {e}")

        log.info(f"OrderBookManager initialized: {len(self.books)} products")

    def get_order_book(self, product_id: str) -> dict:
        if product_id not in self.books:
            return {"error": f"Product {product_id} not found"}
        try:
            return self.books[product_id].to_dict()
        except Exception as e:
            log.error(f"order book error for {product_id}: {e}")
            return {"error": str(e), "product_id": product_id}

    def get_timing_signal(self, product_id: str) -> dict:
        if product_id not in self.timing:
            return {"error": f"Product {product_id} not found"}
        if product_id in self._signal_cache:
            return self._signal_cache[product_id]
        try:
            sig = self.timing[product_id].compute_signal()
            self._signal_cache[product_id] = sig
            return sig
        except Exception as e:
            log.error(f"timing signal error for {product_id}: {e}")
            return {"error": str(e), "product_id": product_id,
                    "signal": "SET_ALERT", "confidence": 30}

    def get_all_signals(self) -> list[dict]:
        """Get timing signals for all products, sorted by confidence."""
        signals = [self.get_timing_signal(pid) for pid in self.products]
        signals = [s for s in signals if "confidence" in s]
        return sorted(signals, key=lambda x: x.get("confidence", 0), reverse=True)

    def get_market_overview(self) -> dict:
        """Summary dashboard data."""
        signals = self.get_all_signals()
        buy_now = [s for s in signals if s.get("signal") == "BUY_NOW"]
        wait = [s for s in signals if s.get("signal") == "WAIT"]
        alert = [s for s in signals if s.get("signal") == "SET_ALERT"]

        total = len(self.books) or 1
        avg_spread = sum(self.books[pid].spread_pct() for pid in self.books) / total
        avg_ofi = sum(self.books[pid].order_flow_imbalance() for pid in self.books) / total

        # Market regime heuristic
        if avg_ofi > 0.15:
            regime = "DEMAND-HEAVY"
        elif avg_ofi < -0.15:
            regime = "SUPPLY-HEAVY"
        else:
            regime = "BALANCED"

        return {
            "total_products": len(self.products),
            "buy_now_count": len(buy_now),
            "wait_count": len(wait),
            "alert_count": len(alert),
            "avg_spread_pct": round(avg_spread, 2),
            "avg_order_flow_imbalance": round(avg_ofi, 3),
            "market_regime": regime,
            "top_buys": [
                {
                    "product_id": s["product_id"],
                    "name": s["name"],
                    "confidence": s["confidence"],
                    "current": s["current_price"],
                    "target": s["target_price"],
                    "signal": s["signal"],
                }
                for s in buy_now[:5]
            ],
            "upcoming_drops": [
                {
                    "product_id": s["product_id"],
                    "name": s["name"],
                    "wait_days": s["wait_days"],
                    "target": s["target_price"],
                    "savings": s["savings_if_wait"],
                }
                for s in wait[:5]
            ],
            "timestamp": time.time(),
        }

    def get_price_history(self, product_id: str) -> list[dict]:
        """Get price history for a product."""
        if product_id not in self.products:
            return []
        return generate_price_history(product_id, self.products[product_id])

    def invalidate_cache(self, product_id: str | None = None):
        """Drop cached signals (useful after product data updates)."""
        if product_id is None:
            self._signal_cache.clear()
        else:
            self._signal_cache.pop(product_id, None)
