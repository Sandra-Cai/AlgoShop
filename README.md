# AlgoShop — Agentic Commerce with Live Market Intelligence

**Builder:** Sandra Cai | NYU | [LinkedIn](https://www.linkedin.com/in/yijia-sandra-cai/) | [GitHub](https://github.com/Sandra-Cai/AlgoShop)

---

## What I Built

AlgoShop is a mobile-first agentic commerce platform that applies quantitative trading logic to everyday shopping. It combines a live algorithmic trading terminal, an LLM agent with tool-use chains, and real-time external data integrations into a single product — running on a FastAPI backend (~2,300 lines) and a raw-DOM single-page frontend (~8,500 lines, no React).

The core thesis: the same infrastructure that makes trading desks fast and data-driven should exist for consumers buying sneakers or hotel rooms.

---

## Why This Problem

Placing top at the Duke Fintech Trading Competition 2026 and Phoenix Trading Competition 2023, I learned that trading edge comes from better signals, faster execution, and disciplined strategy — not intuition. Retail consumers make purchase decisions with worse information than a first-year analyst making a $100 trade. AlgoShop is the fix.

---

## Technical Implementation

**Backend (FastAPI, Python)**
Five modules handle all logic: `agent_server.py` (LLM tool-use chain), `data_engine.py` (product scoring, VWAP, strategy execution), `interest_rank.py` (InterestRank v4.0 — PageRank + HITS + EigenCentrality on a product-interest graph), `order_book.py` (limit order placement and auto-execution), and `connectors.py` (live external data ingestion from three APIs).

**Live External Data — the critical proof of real-world readiness**

`connectors.py` drives three live integrations, all returning real API data with proper parsing and graceful fallbacks:

- **Trivago** — Hotel deals in the Stores tab with live prices, ratings, and images (25 NYC hotels, $135–$581/night). The same ranking and order logic that works on a $90 jacket works on a $300 hotel room.
- **Blockscout** — On-chain commerce intelligence panel: live ETH price ($2,311), gas (2.1 Gwei), 3.4B total transactions, SHOP/SHOPX/SHOPON token prices.
- **GoDaddy** — Domain verification on store listings. Every retailer gets a legitimacy signal before a limit order fires.

A **Live Data Sources Banner** surfaces all five connected sources (Web, Trivago, Blockscout, GoDaddy, Phia DB) with animated pulse indicators — data provenance always visible.

**Frontend (Single-page, raw DOM)**
The interface is organized into four tabs: Shop (26-product demo catalog, InterestRank-scored), Book (live algo trading terminal with candlestick charts, order book, VWAP, RSI/MACD/EMA/Bollinger Bands), Agent (LLM tool-use chain with streamed reasoning), and Stores (verified retailers + Trivago hotel deals). Dark/light mode, gift sharing, chat threads, and email subscription detection are all live.

**8 Quantitative Shopping Strategies:** Momentum Buy, Mean Reversion, VWAP Snipe, Breakout Entry, RSI Oversold, Bollinger Band Squeeze, Trend Follow, Limit Ladder — each maps a proven trading strategy onto product price history and inventory signals. Users set a price target; `order_book.py` auto-executes when conditions are met.

---

## Key Decisions

1. **No React.** Raw DOM manipulation kept the bundle lean and the mobile experience fast. Complexity lives in the backend, not the client.
2. **connectors.py as a unified data layer.** Rather than ad-hoc API calls, all three external sources funnel through one module with consistent error handling. This made adding Trivago, Blockscout, and GoDaddy in parallel tractable for a solo build.
3. **InterestRank over simple collaborative filtering.** PageRank + HITS + EigenCentrality on the product-interest graph surfaces non-obvious recommendations with explainable math — a signal that can be tuned against Phia's conversion and return-rate KPIs.

---

## Alignment with Phia's Vision

AlgoShop directly addresses three of Phia's five interest areas:

- **Autonomous Agents** — The LLM agent with tool-use chain handles limit order execution, strategy selection, and cross-tab coordination without user intervention. It "just handles it."
- **Personalized Shopping** — InterestRank v4.0 and 8 quantitative strategies produce a ranked, personalized product surface that updates on every session signal.
- **Social Shopping** — Friends' strategies, shared order books, and a live savings feed make every user's alpha visible to their network.

AlgoShop is built to be the technical intelligence layer underneath Phia's consumer-facing experience — the same role Phia plays as the "AI alignment layer" between consumers and brands.

---

## What I'd Build Next with Full Phia Access

Given access to Phia's 350M+ product catalog, 7,200 brand partners, and 1M+ user graph, the next three builds would be:

1. **InterestRank on the full Phia graph.** Running EigenCentrality across 1M users and 350M products would produce a recommendation signal that no individual retailer can match. The math scales; the current demo catalog of 26 products is the proof-of-concept.
2. **Limit orders against live Phia price data.** The order book infrastructure is complete. Wiring it to real-time price feeds from brand partners would make autonomous price-target execution production-ready, directly improving conversion rates and reducing return rates.
3. **Post-purchase intelligence loop.** After execution, feed order outcomes (satisfaction signals, return events, reorder rates) back into the strategy scoring model. Every completed order makes the next recommendation more accurate — closing the loop that turns AlgoShop from a shopping tool into a compounding intelligence system.

---

**GitHub:** https://github.com/Sandra-Cai/AlgoShop  
**Built by:** Sandra Cai
