# AlgoShop

**Agentic commerce powered by algorithmic trading strategies.**

*Phia Hack 2026 · April 18–19 · Built in 24 hours*

---

## Why This Problem

I've spent the last three years inside trading pits — placing at the [Duke Fintech Trading Competition 2026](https://www.linkedin.com/in/yijia-sandra-cai/) as a Top Trader and winning the Phoenix Trading Competition in 2023. In quant finance, we obsess over microstructure: the spread between bid and ask, VWAP deviations, RSI extremes, mean reversion after an overreaction. We treat every basis point like it matters, because over thousands of trades, it does.

Then I go shopping and watch friends pay $180 for a Nike hoodie that will be $119 in eleven days, or buy a hotel room at list price when the rate curve is clearly in contango. **Consumer shopping is the most inefficient market on Earth** — asymmetric information, emotional buyers, no order book, no execution discipline. Meanwhile, Phia is building the autonomous agent layer that could finally fix this: if the agent can "just handle it," why shouldn't it handle it *like a quant desk*?

AlgoShop is that thesis shipped as a product.

## The Solution

AlgoShop is a mobile-first agentic commerce platform that turns every SKU into a tradeable instrument. Users get:

- A **live algo trading terminal** for consumer goods — candlestick charts, order book depth, VWAP, RSI/MACD/EMA/Bollinger overlays.
- **Eight quantitative shopping strategies** (VWAP, Bollinger Bands, EMA Crossover, RSI, MACD, Momentum, Mean Reversion, Order Flow) mapped to real shopping outcomes with 5–25% documented savings per strategy.
- **Limit orders on products**: set a price, walk away, auto-execute when the market hits your bid. Measured save rate across the demo catalog is **44%** vs retail.
- An **LLM agent with a tool-use chain** that searches 40,000+ sites (Phia-aligned), pulls quant signals, compares retail vs resale, and returns formatted recommendations with one-tap action buttons.
- **Social shopping**: friends' strategies, order books, and live savings feed — turning execution alpha into a shareable flex.
- **InterestRank personalization**: a PageRank-style authority score over the product graph, personalized by email subscriptions, browsing, social, and purchase signals, with user-adjustable weights.

This hits all three of Phia's interest areas — Autonomous Agents, Personalized Shopping, Social Shopping — through a single coherent lens: **execution quality**.

## Technical Stack

**Frontend** — Single-page mobile-first web app, ~8,200 lines of hand-written HTML/CSS/JS. Seven tabs (Shop, Stores, Book, Agent, Strategies, Trends, Friends), full dark/light theming, live deal ticker, savings tracker, and a Diamond Saver tier system (Explorer → Saver → Smart → Deal Pro → Diamond). No framework bloat — every animation, chart, and order-book update is raw DOM + Canvas for 60fps on mid-tier phones.

**Backend** — FastAPI (Python), organized as five modules:

- `agent_server.py` (~2,300 lines) — 26-product demo catalog, real-time price simulation loop, trading endpoints (place/cancel/fill), and the LLM agent runtime with a tool-use chain (search → quant signals → price compare → action).
- `data_engine.py` — v3.0 product data engine, Phia-aligned, normalizes listings across retail, resale, and affiliate sources.
- `interest_rank.py` — v4.0 **InterestRank** algorithm: PageRank + HITS + Eigenvector Centrality over an adjacency matrix of the product graph. Personalized by user signals with live-tunable weights.
- `order_book.py` — Full limit order book with bid/ask depth, spread, VWAP computation, and an auto-execution engine that fires when the simulated market prints through a resting limit.
- `connectors.py` — External integrations: **Trivago** (hotel price comparison), **Blockscout** (on-chain transaction verification), **GoDaddy** (domain verification to flag sketchy stores).

**Algo Trading Terminal (Book tab)** — Real-time price charts (24H / 48H / ALL), live order book, spread and VWAP readouts, and four technical overlays (RSI, MACD, EMA crossover, Bollinger Bands) computed server-side and streamed to the client. Strategy P&L tracker compares retail price vs AlgoShop execution price per order.

**AI Agent** — LLM-powered, tool-use chain across search, quant signals, and price comparison. Outputs are formatted markdown tables with inline action buttons that can set price alerts or trigger autonomous buys. Built to slot directly into Phia's agentic shopping paradigm.

**Personalization & Social** — Email subscription detection (auto-discovers deals from Nike, Sephora, etc.), contact import, friends' strategies and order books, live activity feed of friends' fills and savings.

## Technical Challenges & How We Solved Them

**1. Phia has no public API.** Phia's underlying shopping graph is proprietary, so there was no drop-in data source. I built a Phia-aligned data pipeline that composes web data extraction, email subscription parsing, and real-time price monitoring across a 40,000+ site universe into a normalized product graph that mirrors Phia's discovery surface. This is the unlock that made everything else possible.

**2. Realistic real-time price microstructure.** Static mock data would make the trading terminal meaningless. I wrote a custom simulation engine that generates price paths with **volatility clustering, trend momentum, and mean reversion** — a simplified GBM-with-regime-switching analogue to real market microstructure — so that RSI, MACD, and Bollinger signals actually behave the way a trader expects and limit orders fill plausibly.

**3. InterestRank on the product graph.** Adapting PageRank to shopping required redefining the graph: products are nodes, edges are bought-together / same-brand / similar-category / co-viewed relationships, weighted by user signal. I then combined **PageRank** (global authority), **HITS** (hub/authority split for brand vs product), and **Eigenvector Centrality** (spectral importance) over the live adjacency matrix. Personalization is injected as a teleportation vector derived from the user's email subs, browsing, social graph, and purchase history, with weights the user can tune in-app.

**4. Markdown rendering inside the agent chat.** The agent's power comes from returning rich formatted output — tables of comparable SKUs, blockquote callouts, horizontal-rule-separated sections. Off-the-shelf parsers either broke on streaming tokens or pulled in too much weight. I wrote a custom block-level markdown parser (headers, tables, blockquotes, horizontal rules, inline emphasis) tuned for incremental rendering so responses paint smoothly as the LLM streams.

## Impact

AlgoShop reframes shopping as a market you can trade, not a menu you accept. The demo catalog shows a **44% average save rate** vs retail when strategies execute end-to-end. For Phia, this is a direct extension of the autonomous agent thesis — the agent doesn't just find the product, it *times the entry* — while simultaneously deepening personalization (InterestRank) and social (shared order books, friends' strategies).

---

## Built By

**Yijia (Sandra) Cai** — NYU · Top Trader, [Duke Fintech Trading Competition 2026](https://www.linkedin.com/in/yijia-sandra-cai/) · Winner Trader, Phoenix Trading Competition 2023 · [LinkedIn](https://www.linkedin.com/in/yijia-sandra-cai/)

*Solo build over 24 hours at the Phia Hack, April 18–19, 2026.*
