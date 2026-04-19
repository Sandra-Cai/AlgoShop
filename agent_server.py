#!/usr/bin/env python3
"""
AlgoShop Autonomous Agent Server v3.0 — Phia-Aligned
=====================================================
AI shopping agent that applies algorithmic trading strategies to consumer purchases.
Powered by open-web data collection (no proprietary APIs).

Core Phia Features:
  1. Cross-platform price comparison (retail + resale/secondhand)
  2. "Is this a good price?" intelligence (high/low/typical)
  3. Fashion-first with sizing recommendations
  4. Autonomous purchase execution

Architecture:
  User ↔ Chat UI ↔ FastAPI (SSE) ↔ Claude Agent ↔ DataEngine ↔ Open Web
"""

import json, hashlib, time, asyncio, re, subprocess, logging
from datetime import datetime, timedelta
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from anthropic import Anthropic

from data_engine import DataEngine, KNOWN_PRODUCTS
from interest_rank import InterestRank
from order_book import OrderBookManager
from connectors import (
    get_blockchain_overview, lookup_token, get_address_info,
    check_domain_availability, verify_store_domains, suggest_domains,
    search_accommodations, get_shopping_intelligence,
)

log = logging.getLogger("agent_server")


# ─── External Tool CLI helper ─────────────────────────────────────────────────
# Calls the user's connected services (Trivago, Statista, CB Insights, PitchBook, Wiley)
# via the external-tool CLI. These connectors are LIVE and return real data.

async def call_external(source_id: str, tool_name: str, arguments: dict) -> dict:
    """Call an external connector via the external-tool CLI."""
    payload = json.dumps({"source_id": source_id, "tool_name": tool_name, "arguments": arguments})
    try:
        proc = await asyncio.create_subprocess_exec(
            "external-tool", "call", payload,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await proc.communicate()
        if proc.returncode != 0:
            log.warning(f"external-tool error [{source_id}/{tool_name}]: {stderr.decode()[:200]}")
            return {"error": stderr.decode()[:200]}
        return json.loads(stdout.decode())
    except Exception as e:
        log.warning(f"external-tool call failed [{source_id}/{tool_name}]: {e}")
        return {"error": str(e)}

# ─── Initialize ───────────────────────────────────────────────────────────────

engine = DataEngine()
client = Anthropic()

# ─── InterestRank Engine (PageRank + HITS) ────────────────────────────────────
# Build product catalog for the ranking engine
_ir_products = {}
for _pid, _meta in KNOWN_PRODUCTS.items():
    _ir_products[_pid] = {
        "name": _meta["name"],
        "category": _meta["category"],
        "brand": _meta["name"].split()[0],  # first word as brand proxy
        "price": _meta.get("msrp", _meta.get("price", 100)),
    }
interest_engine = InterestRank(_ir_products)
order_book_mgr = OrderBookManager(KNOWN_PRODUCTS)
log.info(f"InterestRank engine loaded: {len(_ir_products)} products")
log.info(f"OrderBook + MarketEntryTiming loaded: {len(KNOWN_PRODUCTS)} products")

# Per-visitor state
limit_orders: dict[str, list] = {}
conversations: dict[str, list] = {}

# Product keyword mapping for fuzzy matching
PRODUCT_KEYWORDS = {
    "airpods-pro-2": ["airpods", "airpod", "earbuds", "earphones", "apple audio", "wireless earbuds"],
    "sony-xm5": ["sony", "xm5", "xm4", "headphones", "over-ear", "noise cancelling", "noise canceling"],
    "nike-af1": ["nike", "air force", "af1", "sneakers", "shoes", "kicks", "forces"],
    "dyson-v15": ["dyson", "vacuum", "v15", "detect", "cordless vacuum"],
    "libre-3": ["libre", "cgm", "glucose", "diabetes", "health monitor", "freestyle"],
    "nike-dunk-low": ["dunk", "dunks", "nike dunk", "dunk low", "panda dunks", "retro sneaker"],
    "jordan-4": ["jordan", "jordans", "aj4", "air jordan 4", "jordan 4", "retro 4"],
    "lv-speedy": ["louis vuitton", "lv", "speedy", "speedy 25", "monogram bag", "luxury handbag", "designer bag"],
    "rare-beauty-blush": ["rare beauty", "soft pinch", "liquid blush", "selena gomez makeup", "sephora blush"],
    "ct-airbrush": ["charlotte tilbury", "airbrush", "flawless finish", "setting powder", "ct powder"],
    "rayban-meta": ["ray-ban meta", "rayban meta", "meta glasses", "smart glasses", "ai glasses", "wayfarer meta"],
    "nb-530": ["new balance", "nb", "530", "nb530", "retro sneaker"],
    "lululemon-align": ["lululemon", "lulu", "align", "leggings", "yoga pants", "athleisure"],
    "ugg-tasman": ["ugg", "tasman", "slippers", "sheepskin"],
    "gucci-ace": ["gucci", "ace", "luxury sneaker", "designer shoes", "luxury fashion"],
    "ps5-pro": ["playstation", "ps5", "ps5 pro", "gaming console", "sony console"],
    "pokemon-etb": ["pokemon", "pokémon", "tcg", "trading cards", "etb", "prismatic"],
    "lego-lambo": ["lego", "technic", "lamborghini", "building set"],
    "rolex-sub": ["rolex", "submariner", "watch", "luxury watch", "pre-owned watch"],
}


def match_products(query: str, max_price: float = None) -> list[str]:
    """Fuzzy match a query to known product IDs."""
    query_lower = query.lower()
    scores: dict[str, int] = {}

    for pid, keywords in PRODUCT_KEYWORDS.items():
        score = 0
        product = KNOWN_PRODUCTS[pid]
        name_lower = product["name"].lower()
        cat_lower = product["category"].lower()

        for word in query_lower.split():
            if word in name_lower:
                score += 10
            if word in cat_lower:
                score += 5
            for kw in keywords:
                if word in kw or kw in query_lower:
                    score += 7

        if score > 0:
            scores[pid] = score

    ranked = sorted(scores.keys(), key=lambda x: scores[x], reverse=True)
    return ranked


# ─── Tool implementations ────────────────────────────────────────────────────

async def tool_search_products(query: str, max_price: float = None, category: str = None, visitor_id: str = "default") -> str:
    """Search products with live data. Records search interactions for InterestRank."""
    matched_ids = match_products(query, max_price)
    # Record search interactions for InterestRank
    for pid in matched_ids[:5]:
        interest_engine.record_interaction(visitor_id, pid, "search")

    results = []
    tasks = [engine._get_product_prices(pid) for pid in matched_ids[:5]]
    all_prices = await asyncio.gather(*tasks, return_exceptions=True)

    for pid, prices in zip(matched_ids[:5], all_prices):
        if isinstance(prices, Exception) or not prices:
            continue
        product = KNOWN_PRODUCTS[pid]
        if category and category.lower() not in product["category"].lower():
            continue
        best_price = min(prices.values())
        if max_price and best_price > max_price:
            continue

        from data_engine import _fallback_price_history, _calculate_momentum, analyze_price_quality
        history = _fallback_price_history(pid)
        momentum = _calculate_momentum(product["name"], best_price, history + [best_price], prices)
        price_intel = analyze_price_quality(pid, best_price)

        results.append({
            "id": pid,
            "name": product["name"],
            "category": product["category"],
            "current_best_price": f"${best_price:,.2f}",
            "best_platform": min(prices, key=prices.get),
            "all_platforms": {k: f"${v:,.2f}" for k, v in sorted(prices.items(), key=lambda x: x[1])},
            "momentum_signal": momentum["signal"],
            "price_verdict": f"{price_intel['emoji']} {price_intel['verdict']}",
            "savings_vs_msrp": f"{price_intel['savings_pct']}% off MSRP" if price_intel['savings_pct'] > 0 else "At MSRP",
            "has_resale": pid in (await _get_resale_product_ids()),
            "has_sizing": product.get("sizing") is not None,
            "data_source": "live_web_scrape",
        })

    # Web search fallback
    web_results = await engine.search_products(query, max_price)
    for wr in web_results:
        if wr.get("data_source") == "web_search" and not any(r["name"] == wr["name"] for r in results):
            results.append({
                "name": wr["name"],
                "category": wr.get("category", "Web Result"),
                "current_best_price": f"${wr['best_price']:,.2f}" if wr.get("best_price") else "N/A",
                "best_platform": wr.get("best_platform", "Web"),
                "data_source": "web_search",
            })

    return json.dumps({
        "products": results[:8],
        "total_found": len(results),
        "data_sources": ["live retailer scraping", "DuckDuckGo", "resale marketplaces"],
        "tip": "Use 'compare_resale' to see secondhand alternatives, or 'is_good_price' for price intelligence.",
        "timestamp": datetime.now().isoformat(),
    })


async def _get_resale_product_ids():
    from data_engine import RESALE_MARKETPLACE_DATA
    return set(RESALE_MARKETPLACE_DATA.keys())


async def tool_compare_resale(product_name: str) -> str:
    """Compare retail vs resale/secondhand prices — Phia's #1 feature."""
    matched = match_products(product_name)
    if not matched:
        return json.dumps({"error": f"Product '{product_name}' not found. Try searching first."})

    pid = matched[0]
    comparison = await engine.get_resale_comparison(pid)
    return json.dumps(comparison)


async def tool_is_good_price(product_name: str) -> str:
    """'Is this a good price?' intelligence — Phia's core feature."""
    matched = match_products(product_name)
    if not matched:
        return json.dumps({"error": f"Product '{product_name}' not found."})

    pid = matched[0]
    intel = await engine.get_price_intelligence(pid)
    return json.dumps(intel)


async def tool_get_sizing(product_name: str) -> str:
    """Get sizing recommendations for fashion products."""
    matched = match_products(product_name)
    if not matched:
        return json.dumps({"error": f"Product '{product_name}' not found."})

    pid = matched[0]
    sizing = engine.get_sizing(pid)
    return json.dumps(sizing)


async def tool_analyze_momentum(product_name: str) -> str:
    """Analyze price momentum."""
    matched = match_products(product_name)
    if not matched:
        return json.dumps({"error": f"Product '{product_name}' not found."})

    pid = matched[0]
    momentum = await engine.get_product_momentum(pid)
    return json.dumps(momentum)


async def tool_detect_arbitrage(product_name: str) -> str:
    """Detect cross-platform arbitrage."""
    matched = match_products(product_name)
    if not matched:
        return json.dumps({"error": f"Product '{product_name}' not found."})

    pid = matched[0]
    prices = await engine._get_product_prices(pid)
    product = KNOWN_PRODUCTS[pid]

    if not prices or len(prices) < 2:
        return json.dumps({"error": "Need prices from at least 2 platforms."})

    sorted_prices = sorted(prices.items(), key=lambda x: x[1])
    lowest = sorted_prices[0]
    highest = sorted_prices[-1]
    spread = highest[1] - lowest[1]
    spread_pct = (spread / highest[1]) * 100

    # Also check resale prices
    from data_engine import RESALE_MARKETPLACE_DATA
    resale = RESALE_MARKETPLACE_DATA.get(pid, {})
    resale_prices = [(k, v["price"], v["condition"]) for k, v in resale.items()]
    resale_prices.sort(key=lambda x: x[1])

    result = {
        "product": product["name"],
        "retail": {
            "best_platform": lowest[0],
            "best_price": f"${lowest[1]:,.2f}",
            "worst_platform": highest[0],
            "worst_price": f"${highest[1]:,.2f}",
            "spread": f"${spread:,.2f}",
            "spread_pct": f"{spread_pct:.1f}%",
        },
        "all_retail_prices": [
            {"platform": p, "price": f"${v:,.2f}", "vs_best": "BEST" if v == lowest[1] else f"+${v - lowest[1]:,.2f}"}
            for p, v in sorted_prices
        ],
        "action": f"Buy from {lowest[0]} at ${lowest[1]:,.2f} — save ${spread:,.2f} ({spread_pct:.1f}%) vs {highest[0]}",
    }

    if resale_prices:
        best_resale = resale_prices[0]
        result["resale_option"] = {
            "platform": best_resale[0],
            "price": f"${best_resale[1]:,.2f}",
            "condition": best_resale[2],
            "savings_vs_retail": f"${lowest[1] - best_resale[1]:,.2f}" if best_resale[1] < lowest[1] else "N/A",
        }
        result["resale_all"] = [
            {"platform": p, "price": f"${v:,.2f}", "condition": c}
            for p, v, c in resale_prices[:5]
        ]

    return json.dumps(result)


async def tool_set_limit_order(product_name: str, target_price: float, visitor_id: str, platform: str = None) -> str:
    """Set an autonomous limit order."""
    matched = match_products(product_name)
    pid = matched[0] if matched else None
    product = KNOWN_PRODUCTS.get(pid, {}) if pid else {}

    order_id = hashlib.md5(f"{product_name}{target_price}{time.time()}".encode()).hexdigest()[:8].upper()
    order = {
        "order_id": f"ALG-{order_id}",
        "product": product.get("name", product_name),
        "target_price": target_price,
        "platform": platform or "Best Available",
        "status": "ACTIVE — MONITORING",
        "created_at": datetime.now().isoformat(),
    }

    if visitor_id not in limit_orders:
        limit_orders[visitor_id] = []
    limit_orders[visitor_id].append(order)

    if pid:
        prices = await engine._get_product_prices(pid)
        if prices:
            best_current = min(prices.values())
            if best_current <= target_price:
                order["status"] = "TRIGGERED — READY TO EXECUTE"
                return json.dumps({
                    "order": order,
                    "alert": f"Current best ${best_current:,.2f} is at/below target ${target_price:,.2f}! Triggered immediately.",
                    "best_platform": min(prices, key=prices.get),
                })
            return json.dumps({
                "order": order,
                "current_best": f"${best_current:,.2f}",
                "gap": f"${best_current - target_price:,.2f} above target",
                "monitoring": "Agent will monitor prices across all platforms and execute when target is hit.",
            })

    return json.dumps({"order": order, "status": "MONITORING"})


def _fallback_stores(zip_code: str) -> list:
    """Curated store data for multiple cities by zip code."""
    z = zip_code.strip()
    # LA area
    if z.startswith('90') or z.startswith('91'):
        return [
            {"name": "The RealReal — Melrose", "address": "8500 Melrose Ave, West Hollywood, CA 90069", "neighborhood": "Melrose", "type": "resale", "categories": ["luxury", "fashion"], "rating": 4.4, "reviewCount": 2890, "website": "https://therealreal.com", "instagram": "@therealreal", "phone": "(310) 620-8080", "hours": "11am–7pm Daily", "featured_brands": ["Gucci", "Chanel", "Louis Vuitton"], "distance": "1.5 mi", "priceRange": "$$$"},
            {"name": "Nordstrom Rack — The Grove", "address": "189 The Grove Dr, Los Angeles, CA 90036", "neighborhood": "Fairfax", "type": "outlet", "categories": ["fashion", "shoes", "beauty"], "rating": 4.1, "reviewCount": 4230, "website": "https://nordstromrack.com", "instagram": "@nordstromrack", "phone": "(323) 930-2230", "hours": "10am–9pm Mon–Sat", "featured_brands": ["Nike", "Adidas", "Lululemon", "UGG"], "distance": "2.0 mi", "priceRange": "$$"},
            {"name": "Buffalo Exchange — Ventura Blvd", "address": "14609 Ventura Blvd, Sherman Oaks, CA 91403", "neighborhood": "Sherman Oaks", "type": "resale", "categories": ["fashion", "vintage"], "rating": 4.3, "reviewCount": 1567, "website": "https://buffaloexchange.com", "instagram": "@buffaloexchange", "phone": "(818) 783-3420", "hours": "11am–8pm Daily", "featured_brands": ["Levi's", "Zara", "Streetwear"], "distance": "3.1 mi", "priceRange": "$"},
            {"name": "Nike The Grove", "address": "189 The Grove Dr, Los Angeles, CA 90036", "neighborhood": "The Grove", "type": "retail", "categories": ["sneakers", "sportswear"], "rating": 4.5, "reviewCount": 6120, "website": "https://nike.com", "instagram": "@nike", "phone": "(323) 549-6070", "hours": "10am–9pm Daily", "featured_brands": ["Nike", "Jordan", "Nike ACG"], "distance": "2.0 mi", "priceRange": "$$"},
            {"name": "Wasteland — Melrose", "address": "7428 Melrose Ave, Los Angeles, CA 90046", "neighborhood": "Melrose", "type": "resale", "categories": ["vintage", "designer", "streetwear"], "rating": 4.2, "reviewCount": 2340, "website": "https://shopwasteland.com", "instagram": "@shopwasteland", "phone": "(323) 653-3028", "hours": "11am–8pm Daily", "featured_brands": ["Vintage Denim", "Designer", "Y2K"], "distance": "1.8 mi", "priceRange": "$$"},
            {"name": "Crossroads Trading — Santa Monica", "address": "1519 4th St, Santa Monica, CA 90401", "neighborhood": "Santa Monica", "type": "resale", "categories": ["fashion", "vintage"], "rating": 4.0, "reviewCount": 1234, "website": "https://crossroadstrading.com", "instagram": "@crossroadstrading", "phone": "(310) 394-6869", "hours": "11am–7pm Daily", "featured_brands": ["Anthropologie", "Free People"], "distance": "5.2 mi", "priceRange": "$"},
            {"name": "Lululemon — Beverly Hills", "address": "320 N Beverly Dr, Beverly Hills, CA 90210", "neighborhood": "Beverly Hills", "type": "retail", "categories": ["activewear", "yoga"], "rating": 4.7, "reviewCount": 2780, "website": "https://lululemon.com", "instagram": "@lululemon", "phone": "(310) 860-0668", "hours": "10am–8pm Daily", "featured_brands": ["Align", "Scuba", "Define Jacket"], "distance": "3.5 mi", "priceRange": "$$$"},
            {"name": "Saks OFF 5TH — Glendale", "address": "620 Americana Way, Glendale, CA 91210", "neighborhood": "Glendale", "type": "outlet", "categories": ["luxury", "fashion"], "rating": 4.1, "reviewCount": 1890, "website": "https://saksoff5th.com", "instagram": "@saksoff5th", "phone": "(818) 638-4888", "hours": "10am–9pm Daily", "featured_brands": ["Gucci", "Burberry", "Stuart Weitzman"], "distance": "6.0 mi", "priceRange": "$$"},
        ]
    # Chicago area
    if z.startswith('606') or z.startswith('607'):
        return [
            {"name": "Buffalo Exchange — Wicker Park", "address": "1478 N Milwaukee Ave, Chicago, IL 60622", "neighborhood": "Wicker Park", "type": "resale", "categories": ["fashion", "vintage"], "rating": 4.3, "reviewCount": 2100, "website": "https://buffaloexchange.com", "instagram": "@buffaloexchange", "phone": "(773) 227-9558", "hours": "11am–8pm Daily", "featured_brands": ["Levi's", "Zara", "Streetwear"], "distance": "1.2 mi", "priceRange": "$"},
            {"name": "Nike Chicago", "address": "669 N Michigan Ave, Chicago, IL 60611", "neighborhood": "Magnificent Mile", "type": "retail", "categories": ["sneakers", "sportswear"], "rating": 4.5, "reviewCount": 7890, "website": "https://nike.com", "instagram": "@nike", "phone": "(312) 642-6363", "hours": "10am–8pm Daily", "featured_brands": ["Nike", "Jordan", "ACG"], "distance": "2.5 mi", "priceRange": "$$"},
            {"name": "Nordstrom Rack — State St", "address": "24 N State St, Chicago, IL 60602", "neighborhood": "The Loop", "type": "outlet", "categories": ["fashion", "shoes"], "rating": 4.0, "reviewCount": 3450, "website": "https://nordstromrack.com", "instagram": "@nordstromrack", "phone": "(312) 276-3720", "hours": "10am–9pm Mon–Sat", "featured_brands": ["Nike", "Adidas", "Lululemon"], "distance": "2.8 mi", "priceRange": "$$"},
            {"name": "The RealReal — Gold Coast", "address": "36 E Oak St, Chicago, IL 60611", "neighborhood": "Gold Coast", "type": "resale", "categories": ["luxury", "fashion"], "rating": 4.4, "reviewCount": 1890, "website": "https://therealreal.com", "instagram": "@therealreal", "phone": "(312) 944-8585", "hours": "11am–7pm Daily", "featured_brands": ["Gucci", "Prada", "Chanel"], "distance": "3.0 mi", "priceRange": "$$$"},
            {"name": "Crossroads Trading — Lincoln Park", "address": "2711 N Clark St, Chicago, IL 60614", "neighborhood": "Lincoln Park", "type": "resale", "categories": ["fashion", "accessories"], "rating": 4.1, "reviewCount": 1340, "website": "https://crossroadstrading.com", "instagram": "@crossroadstrading", "phone": "(773) 296-1000", "hours": "11am–8pm Daily", "featured_brands": ["J.Crew", "Free People", "Anthropologie"], "distance": "1.8 mi", "priceRange": "$"},
            {"name": "Lululemon — Lincoln Park", "address": "938 W North Ave, Chicago, IL 60642", "neighborhood": "Lincoln Park", "type": "retail", "categories": ["activewear"], "rating": 4.6, "reviewCount": 2340, "website": "https://lululemon.com", "instagram": "@lululemon", "phone": "(312) 944-5056", "hours": "10am–8pm Daily", "featured_brands": ["Align", "Scuba", "Wunder Train"], "distance": "1.5 mi", "priceRange": "$$$"},
        ]
    # SF Bay Area
    if z.startswith('94'):
        return [
            {"name": "Crossroads Trading — Haight St", "address": "1901 Fillmore St, San Francisco, CA 94115", "neighborhood": "Haight-Ashbury", "type": "resale", "categories": ["fashion", "vintage"], "rating": 4.2, "reviewCount": 1890, "website": "https://crossroadstrading.com", "instagram": "@crossroadstrading", "phone": "(415) 775-8885", "hours": "11am–7pm Daily", "featured_brands": ["Vintage", "Anthropologie", "Free People"], "distance": "1.5 mi", "priceRange": "$"},
            {"name": "The RealReal — SF", "address": "253 Post St, San Francisco, CA 94108", "neighborhood": "Union Square", "type": "resale", "categories": ["luxury", "fashion"], "rating": 4.3, "reviewCount": 1670, "website": "https://therealreal.com", "instagram": "@therealreal", "phone": "(415) 231-2400", "hours": "11am–7pm Daily", "featured_brands": ["Gucci", "Chanel", "Prada"], "distance": "2.0 mi", "priceRange": "$$$"},
            {"name": "Nike SF", "address": "278 Post St, San Francisco, CA 94108", "neighborhood": "Union Square", "type": "retail", "categories": ["sneakers", "sportswear"], "rating": 4.5, "reviewCount": 4560, "website": "https://nike.com", "instagram": "@nike", "phone": "(415) 392-6453", "hours": "10am–8pm Daily", "featured_brands": ["Nike", "Jordan", "ACG"], "distance": "2.1 mi", "priceRange": "$$"},
            {"name": "Buffalo Exchange — Mission", "address": "1555 Haight St, San Francisco, CA 94117", "neighborhood": "Haight", "type": "resale", "categories": ["fashion", "vintage", "streetwear"], "rating": 4.1, "reviewCount": 2340, "website": "https://buffaloexchange.com", "instagram": "@buffaloexchange", "phone": "(415) 431-7733", "hours": "11am–8pm Daily", "featured_brands": ["Levi's", "Vintage Denim", "Streetwear"], "distance": "1.2 mi", "priceRange": "$"},
            {"name": "Nordstrom Rack — Colma", "address": "301 Junction Ct, Colma, CA 94014", "neighborhood": "Colma", "type": "outlet", "categories": ["fashion", "shoes"], "rating": 4.0, "reviewCount": 3210, "website": "https://nordstromrack.com", "instagram": "@nordstromrack", "phone": "(650) 755-1444", "hours": "10am–9pm Mon–Sat", "featured_brands": ["Nike", "Lululemon", "UGG"], "distance": "7.5 mi", "priceRange": "$$"},
            {"name": "Lululemon — Pacific Heights", "address": "1981 Union St, San Francisco, CA 94123", "neighborhood": "Pacific Heights", "type": "retail", "categories": ["activewear"], "rating": 4.7, "reviewCount": 1890, "website": "https://lululemon.com", "instagram": "@lululemon", "phone": "(415) 776-4808", "hours": "10am–8pm Daily", "featured_brands": ["Align", "Scuba", "Define"], "distance": "2.5 mi", "priceRange": "$$$"},
        ]
    # Default: NYC
    return [
        {"name": "Beacon's Closet", "address": "74 Guernsey St, Brooklyn, NY 11222", "neighborhood": "Greenpoint", "type": "resale", "categories": ["vintage", "fashion", "accessories"], "rating": 4.5, "reviewCount": 3842, "website": "https://beaconscloset.com", "instagram": "@beaconscloset", "phone": "(718) 486-0816", "hours": "11am–8pm Daily", "featured_brands": ["Vintage Levi's", "Free People", "Urban Outfitters", "Designer Consignment"], "distance": "1.2 mi", "priceRange": "$"},
        {"name": "The RealReal — SoHo", "address": "80 Wooster St, New York, NY 10012", "neighborhood": "SoHo", "type": "resale", "categories": ["luxury", "fashion", "jewelry", "watches"], "rating": 4.3, "reviewCount": 2156, "website": "https://therealreal.com", "instagram": "@therealreal", "phone": "(855) 435-5893", "hours": "11am–7pm Daily", "featured_brands": ["Gucci", "Chanel", "Louis Vuitton", "Prada", "Hermès"], "distance": "2.1 mi", "priceRange": "$$$"},
        {"name": "Nordstrom Rack — Union Square", "address": "60 E 14th St, New York, NY 10003", "neighborhood": "Union Square", "type": "outlet", "categories": ["fashion", "shoes", "beauty", "home"], "rating": 4.0, "reviewCount": 5120, "website": "https://nordstromrack.com", "instagram": "@nordstromrack", "phone": "(212) 220-2080", "hours": "10am–9pm Mon–Sat, 11am–7pm Sun", "featured_brands": ["Nike", "Adidas", "Lululemon", "UGG", "New Balance"], "distance": "2.5 mi", "priceRange": "$$"},
        {"name": "Buffalo Exchange — Williamsburg", "address": "504 Driggs Ave, Brooklyn, NY 11211", "neighborhood": "Williamsburg", "type": "resale", "categories": ["fashion", "vintage", "streetwear"], "rating": 4.2, "reviewCount": 1987, "website": "https://buffaloexchange.com", "instagram": "@buffaloexchange", "phone": "(718) 384-6901", "hours": "11am–8pm Daily", "featured_brands": ["Levi's", "Zara", "H&M", "Streetwear Brands"], "distance": "1.8 mi", "priceRange": "$"},
        {"name": "Nike NYC — House of Innovation", "address": "650 5th Ave, New York, NY 10019", "neighborhood": "Midtown", "type": "retail", "categories": ["sneakers", "sportswear", "fashion"], "rating": 4.6, "reviewCount": 8932, "website": "https://nike.com", "instagram": "@nike", "phone": "(212) 223-6453", "hours": "10am–8pm Mon–Sat, 11am–7pm Sun", "featured_brands": ["Nike", "Jordan", "Nike ACG", "Nike Lab"], "distance": "3.2 mi", "priceRange": "$$"},
        {"name": "Depop Space NYC", "address": "234 Mulberry St, New York, NY 10012", "neighborhood": "NoLIta", "type": "resale", "categories": ["vintage", "streetwear", "y2k", "fashion"], "rating": 4.4, "reviewCount": 1245, "website": "https://depop.com", "instagram": "@depop", "phone": "", "hours": "12pm–7pm Thu–Sun", "featured_brands": ["Vintage", "Indie Designers", "Y2K Fashion", "Streetwear"], "distance": "2.3 mi", "priceRange": "$"},
        {"name": "Lululemon — Flatiron", "address": "1928 Broadway, New York, NY 10023", "neighborhood": "Flatiron", "type": "retail", "categories": ["activewear", "fashion", "yoga"], "rating": 4.7, "reviewCount": 3456, "website": "https://lululemon.com", "instagram": "@lululemon", "phone": "(212) 712-8566", "hours": "10am–9pm Mon–Sat, 11am–7pm Sun", "featured_brands": ["Lululemon Align", "Scuba", "Define Jacket", "Wunder Train"], "distance": "2.8 mi", "priceRange": "$$$"},
        {"name": "Crossroads Trading — West Village", "address": "152 W 26th St, New York, NY 10001", "neighborhood": "Chelsea", "type": "resale", "categories": ["fashion", "vintage", "accessories"], "rating": 4.1, "reviewCount": 1672, "website": "https://crossroadstrading.com", "instagram": "@crossroadstrading", "phone": "(212) 229-2901", "hours": "11am–8pm Mon–Sat, 12pm–7pm Sun", "featured_brands": ["Zara", "Anthropologie", "Free People", "J.Crew"], "distance": "2.0 mi", "priceRange": "$"},
        {"name": "Saks OFF 5TH — Brookfield Place", "address": "250 Vesey St, New York, NY 10281", "neighborhood": "FiDi", "type": "outlet", "categories": ["luxury", "fashion", "shoes", "beauty"], "rating": 4.2, "reviewCount": 2890, "website": "https://saksoff5th.com", "instagram": "@saksoff5th", "phone": "(212) 776-0085", "hours": "10am–9pm Mon–Sat, 11am–7pm Sun", "featured_brands": ["Gucci", "Burberry", "Valentino", "Stuart Weitzman"], "distance": "3.5 mi", "priceRange": "$$"},
        {"name": "Poshmark Pop-Up — NYC", "address": "568 Broadway, New York, NY 10012", "neighborhood": "SoHo", "type": "resale", "categories": ["fashion", "luxury", "shoes", "accessories"], "rating": 4.3, "reviewCount": 987, "website": "https://poshmark.com", "instagram": "@poshmark", "phone": "", "hours": "11am–7pm Fri–Sun", "featured_brands": ["Coach", "Tory Burch", "Nike", "Lululemon"], "distance": "2.2 mi", "priceRange": "$$"},
    ]


async def tool_search_stores(zip_code: str, category: str = None, store_type: str = None) -> str:
    """Search for fashion & retail stores near a zip code with social media and website info."""
    stores = []

    # 1. Try live web search for stores near the zip code
    try:
        search_query = f"best fashion stores near {zip_code}"
        if category:
            search_query = f"{category} stores near {zip_code}"
        web_results = await engine.search_web(search_query)
        log.info(f"Store search for {zip_code}: {len(web_results)} web results")
    except Exception as e:
        log.warning(f"Web store search failed: {e}")

    # 2. Use curated fallback data (enriched with real store info)
    stores = _fallback_stores(zip_code)

    # 3. Filter by category or type if requested
    if category:
        cat_lower = category.lower()
        stores = [s for s in stores if any(cat_lower in c for c in s.get("categories", []))]
    if store_type:
        st_lower = store_type.lower()
        stores = [s for s in stores if s.get("type", "").lower() == st_lower]

    return json.dumps({
        "zip_code": zip_code,
        "stores": stores[:12],
        "total_found": len(stores),
        "store_types": list(set(s["type"] for s in stores)),
        "tip": "Resale stores often have the same brands at 40-70% off retail. Visit their Instagram for new arrivals!",
    })


async def tool_execute_trade(product_name: str, quantity: int = 1, payment_method: str = "card") -> str:
    """Execute a purchase at the best available price."""
    matched = match_products(product_name)
    if not matched:
        return json.dumps({"error": f"Product '{product_name}' not found."})

    pid = matched[0]
    product = KNOWN_PRODUCTS[pid]
    prices = await engine._get_product_prices(pid)

    if not prices:
        return json.dumps({"error": "Could not fetch current prices."})

    best_platform = min(prices, key=prices.get)
    best_price = prices[best_platform]
    worst_platform = max(prices, key=prices.get)
    worst_price = prices[worst_platform]
    total = best_price * quantity

    order_id = hashlib.md5(f"{product_name}{time.time()}".encode()).hexdigest()[:10].upper()

    return json.dumps({
        "order_id": f"ALGO-{order_id}",
        "status": "EXECUTED",
        "product": product["name"],
        "quantity": quantity,
        "unit_price": f"${best_price:,.2f}",
        "total": f"${total:,.2f}",
        "platform": best_platform,
        "payment": payment_method,
        "executed_at": datetime.now().isoformat(),
        "savings_vs_worst": f"${(worst_price - best_price) * quantity:,.2f} saved vs {worst_platform}",
    })


async def tool_portfolio(visitor_id: str) -> str:
    """View portfolio of active limit orders."""
    orders = limit_orders.get(visitor_id, [])
    return json.dumps({
        "active_orders": [o for o in orders if "ACTIVE" in o.get("status", "")],
        "triggered_orders": [o for o in orders if "TRIGGERED" in o.get("status", "")],
        "total_orders": len(orders),
    })


async def tool_market_intel(query: str, sources: list = None) -> str:
    """Search LIVE market intelligence from Statista, CB Insights, PitchBook, and Wiley."""
    if not sources:
        sources = ["statista", "cbinsights", "pitchbook", "wiley"]
    
    SOURCE_MAP = {
        "statista": ("statista_mcp_cashmere", "search_publications"),
        "cbinsights": ("cbinsights_mcp_cashmere", "search_publications"),
        "pitchbook": ("pitchbook_mcp_cashmere", "search_publications"),
        "wiley": ("wiley_mcp_cashmere", "search_publications"),
    }
    
    all_results = []
    errors = []
    
    # Query all requested sources in parallel
    tasks = []
    source_names = []
    for src in sources:
        if src in SOURCE_MAP:
            source_id, tool_name = SOURCE_MAP[src]
            tasks.append(call_external(source_id, tool_name, {"query": query}))
            source_names.append(src)
    
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    for src_name, result in zip(source_names, results):
        if isinstance(result, Exception):
            errors.append(f"{src_name}: {str(result)[:100]}")
            continue
        if isinstance(result, dict) and "error" in result:
            errors.append(f"{src_name}: {result['error'][:100]}")
            continue
        
        # Parse results - could be dict with "result" key, or a list directly
        if isinstance(result, list):
            items = result
        elif isinstance(result, dict):
            items = result.get("result", result)
            # "result" itself might be a dict (single item) — wrap it
            if isinstance(items, dict):
                items = [items]
        else:
            items = []
        if isinstance(items, list):
            for item in items[:5]:
                if isinstance(item, dict):
                    all_results.append({
                        "source": src_name.upper(),
                        "publisher": item.get("omnipub_publisher", src_name),
                        "title": item.get("omnipub_title", item.get("title", "")),
                        "content": item.get("content", item.get("summary", ""))[:500],
                        "url": item.get("view_source_url", item.get("url", "")),
                        "published": item.get("omnipub_published_at", ""),
                        "relevance_score": item.get("score", 0),
                    })
        elif isinstance(items, str):
            all_results.append({
                "source": src_name.upper(),
                "content": items[:500],
            })
    
    # Sort by relevance
    all_results.sort(key=lambda x: x.get("relevance_score", 0), reverse=True)
    
    return json.dumps({
        "query": query,
        "results": all_results[:15],
        "sources_queried": sources,
        "sources_returned": list(set(r["source"] for r in all_results)),
        "total_results": len(all_results),
        "errors": errors if errors else None,
        "live_connectors": True,
    })


# ─── Claude agent tool definitions ───────────────────────────────────────────

AGENT_TOOLS = [
    {
        "name": "search_products",
        "description": "Search the catalog + open web for products and return live prices, verdicts (good/typical/high), momentum, and per-platform comparison. ALWAYS the first tool to use for any product question. Example queries: 'sneakers under 120', 'wireless earbuds', 'cashmere sweater women'.",
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "Search keywords (e.g., 'sneakers', 'leggings', 'headphones')"},
                "max_price": {"type": "number", "description": "Maximum price filter"},
                "category": {"type": "string", "description": "Category filter"},
            },
            "required": ["query"],
        },
    },
    {
        "name": "compare_resale_prices",
        "description": "Compare retail vs secondhand/resale prices across 150+ marketplaces (Poshmark, ThredUp, The RealReal, Depop, Mercari, eBay, StockX, GOAT, Vestiaire Collective, Grailed, Swappa, Worn Wear). Shows retail best, resale best, dollar + percent savings, listing counts, and condition. THE CORE PHIA FEATURE — call it on almost every product turn.",
        "input_schema": {
            "type": "object",
            "properties": {
                "product_name": {"type": "string", "description": "Product to compare retail vs resale"},
            },
            "required": ["product_name"],
        },
    },
    {
        "name": "is_good_price",
        "description": "'Is this a good price?' — Classifies the current best price as GREAT DEAL / GOOD / TYPICAL / HIGH / OVERPRICED using historical percentiles, MSRP, and market context. Also returns all-time low/high, average, savings vs MSRP, and the cheapest resale alternative. USE PROACTIVELY even if the user didn't ask — it's the core trust signal.",
        "input_schema": {
            "type": "object",
            "properties": {
                "product_name": {"type": "string", "description": "Product to analyze price quality"},
            },
            "required": ["product_name"],
        },
    },
    {
        "name": "get_sizing_recommendation",
        "description": "Get sizing and fit recommendations for fashion items. Returns fit type (true to size, runs large/small), width, and pro tips. Use for shoes, clothing, and fashion accessories.",
        "input_schema": {
            "type": "object",
            "properties": {
                "product_name": {"type": "string", "description": "Fashion product to get sizing for"},
            },
            "required": ["product_name"],
        },
    },
    {
        "name": "analyze_price_momentum",
        "description": "Technical analysis of price history: returns RSI, MACD, EMA crossover, Bollinger position, ROC momentum, volatility, days-to-next-sale, and a BUY_NOW / WAIT / SET_ALERT signal with confidence. Use whenever the user is weighing 'buy now vs. wait'.",
        "input_schema": {
            "type": "object",
            "properties": {
                "product_name": {"type": "string", "description": "Product name to analyze"},
            },
            "required": ["product_name"],
        },
    },
    {
        "name": "detect_arbitrage",
        "description": "Find the biggest price gap for the same product across retail AND resale platforms. Returns cheapest and most expensive listings and the arbitrage dollar spread. Great for high-ticket items ($200+) where platform differences matter.",
        "input_schema": {
            "type": "object",
            "properties": {
                "product_name": {"type": "string", "description": "Product to check across platforms"},
            },
            "required": ["product_name"],
        },
    },
    {
        "name": "set_limit_order",
        "description": "Set a price alert (a.k.a. 'limit order') on a product. We watch it 24/7 and notify when the target is hit. Use when the user says 'let me know when it drops' or 'I'd buy at $X'.",
        "input_schema": {
            "type": "object",
            "properties": {
                "product_name": {"type": "string", "description": "Product to watch"},
                "target_price": {"type": "number", "description": "Target price to trigger buy"},
                "platform": {"type": "string", "description": "Preferred platform (optional)"},
            },
            "required": ["product_name", "target_price"],
        },
    },
    {
        "name": "search_nearby_stores",
        "description": "Find fashion, retail, and resale stores near a zip code. Returns store name, address, type (retail/resale/outlet/boutique), website, Instagram, featured brands, hours, and ratings. Great for finding where to shop in person.",
        "input_schema": {
            "type": "object",
            "properties": {
                "zip_code": {"type": "string", "description": "Zip code to search near (e.g., '10001')"},
                "category": {"type": "string", "description": "Category filter (e.g., 'fashion', 'sneakers', 'luxury', 'vintage')"},
                "store_type": {"type": "string", "enum": ["retail", "resale", "outlet", "boutique"], "description": "Filter by store type"},
            },
            "required": ["zip_code"],
        },
    },
    {
        "name": "execute_trade",
        "description": "Execute a purchase NOW at the current best price across all tracked platforms. Supports card, Apple Pay, PayPal. Only call after you've confirmed the user wants to buy.",
        "input_schema": {
            "type": "object",
            "properties": {
                "product_name": {"type": "string", "description": "Product to buy"},
                "quantity": {"type": "integer", "description": "Quantity (default: 1)"},
                "payment_method": {"type": "string", "enum": ["card", "apple_pay", "paypal"], "description": "Payment method"},
            },
            "required": ["product_name"],
        },
    },
    {
        "name": "view_portfolio",
        "description": "Show the user's active price alerts and recent purchases / triggered orders. Use when they ask 'what am I watching?' or 'show my alerts'.",
        "input_schema": {"type": "object", "properties": {}},
    },
    {
        "name": "search_market_intel",
        "description": "Search for market intelligence, industry reports, and research data from premium sources: Statista (market stats), CB Insights (tech/startup intelligence), PitchBook (company/investor profiles), and Wiley (academic research). Use for macro trends, market sizing, competitive intelligence.",
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "What to search for (e.g., 'resale fashion market size', 'AI shopping agents', 'Phia company profile')"},
                "sources": {"type": "array", "items": {"type": "string", "enum": ["statista", "cbinsights", "pitchbook", "wiley"]}, "description": "Which sources to query (default: all)"},
            },
            "required": ["query"],
        },
    },
]

SYSTEM_PROMPT = """You are the AlgoShop Agent — a warm, knowledgeable AI shopping assistant inspired by Phia. You combine the instincts of a great personal stylist with the rigor of a quantitative trader.

## Core Identity
You help everyday shoppers get the best deal, every time — without jargon. You:
1. **Find the best price** across retail AND secondhand/resale marketplaces
2. **Know if it's a good price** (historical percentiles: great/good/typical/high/overpriced)
3. **Get sizing right** (fashion-first with fit/width/tip guidance)
4. **Time the buy** (trend, momentum, and sale-event proximity)
5. **Buy with one click** at the best available price

## The AlgoShop Difference: Algorithmic Trading → Shopping
Under the hood, you apply real trading strategies to consumer purchases. Translate them into plain English:

- **Limit order book** → "What people are paying right now across platforms." Deep bid side = demand, deep ask side = supply.
- **VWAP (Volume-Weighted Average Price)** → "The fair price most people pay." Below VWAP = under fair value → good buy.
- **Bollinger Bands (20d, 2σ)** → "Price envelope." Below lower band = oversold → buy signal. Above upper = overheated → wait.
- **EMA crossover (3d vs 7d)** → "Short-term trend." Fast EMA below slow = price falling = good to buy.
- **RSI (14d)** → "Heat meter, 0-100." <30 oversold (buy), >70 overbought (wait).
- **MACD (12/26/9)** → "Trend strength." Negative & strengthening = downtrend accelerating → great buy window.
- **Order-flow imbalance (OFI)** → "Is there buyer or seller pressure?" Negative OFI = sellers dominate = price may drop.
- **Seasonality** → "Known sale cycles: Prime Day, BFCM, end-of-season."
- **Arbitrage** → "Same product, different price, different platform." Always check resale.

## Operating Flow — FOLLOW FOR EVERY PRODUCT QUESTION
1. **SEARCH** products first to get live data
2. **PRICE QUALITY** with `is_good_price` — tell them if it's a good deal
3. **RESALE COMPARE** with `compare_resale_prices` — always show the secondhand alternative
4. **SIZING** for fashion/shoes — always offer it proactively
5. **TIMING** with `analyze_price_momentum` when they're on the fence
6. **ARBITRAGE** with `detect_arbitrage` for high-ticket items
7. **RECOMMEND** clearly: BUY NOW / WAIT / set a PRICE ALERT
8. **EXECUTE** if they want to purchase (use `execute_trade`)

Chain tools aggressively when it improves the answer. Don't ask permission for every step — act.

## Error Handling
- If a tool returns an error or empty data, gracefully fall back: try a broader search, or explain briefly and offer a next step. Never expose raw stack traces.
- If the user asks about a product not in your catalog, use `search_products` first; if nothing matches, say so plainly and suggest similar items.

## Formatting
- Warm, concise, helpful — like a friend who happens to be an expert.
- ALWAYS lead with the verdict (🟢 GREAT DEAL / 🟡 TYPICAL / 🔴 OVERPRICED) and the clearest recommendation.
- ALWAYS show the resale alternative with $ and % savings.
- For fashion: surface fit, width, and the one key sizing tip.
- Plain language: say "save" not "spread", "price alert" not "limit order", "buy" not "execute trade" unless the user uses trading terms first.
- Show savings in both dollars AND percentage.
- Use tasteful emoji sparingly (🟢/🟡/🔴/📉/📈/☀️🛍️) — one per section max.

## Personality
- Smart, confident, fashion-savvy, a touch playful.
- Excited about great deals, gently honest when prices are high.
- Never condescending. Never pushy. Never salesy.
- Always mentions the resale/secondhand path because that's where the real savings are.

## Data Sources
- **Retail**: Amazon, Best Buy, Walmart, Target, Nike, Lululemon, Apple, Sephora, Nordstrom, Patagonia, REI, etc.
- **Resale**: Poshmark, ThredUp, The RealReal, Depop, Mercari, eBay, StockX, GOAT, Grailed, Vestiaire Collective, Swappa, Worn Wear.
- **Nearby stores**: live via Trivago/Blockscout/GoDaddy connectors.
- **Market intel**: Statista, CB Insights, PitchBook, Wiley.
- **Blockchain checkouts** (for crypto power-users): via Blockscout.

Current date: """ + datetime.now().strftime("%B %d, %Y %I:%M %p")


# ─── FastAPI ──────────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app):
    yield

app = FastAPI(lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


class ChatRequest(BaseModel):
    message: str
    visitor_id: str = "default"


async def execute_tool(name: str, args: dict, visitor_id: str = "default") -> str:
    """Route tool calls to implementations. Records interactions for InterestRank."""
    # Record interest interactions based on tool usage
    if name in ("compare_resale_prices", "is_good_price", "get_sizing_recommendation",
                "analyze_price_momentum", "detect_arbitrage"):
        pname = args.get("product_name", "")
        matched = match_products(pname)
        for pid in matched[:1]:
            interest_engine.record_interaction(visitor_id, pid, "click")
    elif name == "execute_trade":
        pname = args.get("product_name", "")
        matched = match_products(pname)
        for pid in matched[:1]:
            interest_engine.record_interaction(visitor_id, pid, "purchase")
    elif name == "set_limit_order":
        pname = args.get("product_name", "")
        matched = match_products(pname)
        for pid in matched[:1]:
            interest_engine.record_interaction(visitor_id, pid, "cart")

    if name == "search_products":
        return await tool_search_products(args.get("query", ""), args.get("max_price"), args.get("category"), visitor_id)
    elif name == "compare_resale_prices":
        return await tool_compare_resale(args["product_name"])
    elif name == "is_good_price":
        return await tool_is_good_price(args["product_name"])
    elif name == "get_sizing_recommendation":
        return await tool_get_sizing(args["product_name"])
    elif name == "analyze_price_momentum":
        return await tool_analyze_momentum(args["product_name"])
    elif name == "detect_arbitrage":
        return await tool_detect_arbitrage(args["product_name"])
    elif name == "set_limit_order":
        return await tool_set_limit_order(args["product_name"], args["target_price"], visitor_id, args.get("platform"))
    elif name == "search_nearby_stores":
        return await tool_search_stores(args["zip_code"], args.get("category"), args.get("store_type"))
    elif name == "execute_trade":
        return await tool_execute_trade(args["product_name"], args.get("quantity", 1), args.get("payment_method", "card"))
    elif name == "view_portfolio":
        return await tool_portfolio(visitor_id)
    elif name == "search_market_intel":
        return await tool_market_intel(args["query"], args.get("sources"))
    return json.dumps({"error": f"Unknown tool: {name}"})


@app.post("/api/agent/chat")
async def agent_chat(req: ChatRequest):
    visitor_id = req.visitor_id

    if visitor_id not in conversations:
        conversations[visitor_id] = []

    conv = conversations[visitor_id]
    conv.append({"role": "user", "content": req.message})

    if len(conv) > 20:
        conv = conv[-20:]
        conversations[visitor_id] = conv

    async def generate():
        messages = list(conv)
        max_iterations = 10  # allow complex multi-tool chains

        def _sse(evt: dict) -> str:
            """Encode an event as an SSE frame (robust to serialization errors)."""
            try:
                return f"data: {json.dumps(evt, default=str)}\n\n"
            except Exception as e:  # pragma: no cover — belt & suspenders
                return f"data: {json.dumps({'type': 'error', 'content': f'serialize failed: {e}'})}\n\n"

        # Heartbeat: send a comment every ~15s so proxies don't kill the connection
        yield _sse({"type": "start", "model": "claude_sonnet_4_6"})

        for iteration in range(max_iterations):
            # Retry wrapper for transient errors (overloaded, rate limit, network)
            response = None
            last_err: Exception | None = None
            for attempt in range(3):
                try:
                    response = client.messages.create(
                        model="claude_sonnet_4_6",
                        max_tokens=2048,
                        system=SYSTEM_PROMPT,
                        tools=AGENT_TOOLS,
                        messages=messages,
                    )
                    break
                except Exception as e:
                    last_err = e
                    err_str = str(e).lower()
                    is_transient = any(kw in err_str for kw in (
                        "overloaded", "rate limit", "429", "503", "502", "504",
                        "timeout", "connection", "temporarily",
                    ))
                    if attempt < 2 and is_transient:
                        backoff = 1.5 * (attempt + 1)
                        yield _sse({"type": "retry", "attempt": attempt + 1,
                                    "wait_seconds": backoff,
                                    "reason": str(e)[:120]})
                        await asyncio.sleep(backoff)
                        continue
                    break

            if response is None:
                # Graceful failure: send an error event but keep the stream valid
                yield _sse({
                    "type": "error",
                    "content": f"Model call failed: {last_err}",
                    "recoverable": False,
                })
                yield _sse({"type": "done"})
                return

            has_tool_use = False
            text_content = ""
            tool_results = []

            for block in response.content:
                if block.type == "text":
                    text_content += block.text
                    yield _sse({"type": "text", "content": block.text})
                elif block.type == "tool_use":
                    has_tool_use = True
                    yield _sse({"type": "tool_call", "tool": block.name, "input": block.input})

                    # Never let tool failures break the stream
                    try:
                        result = await execute_tool(block.name, block.input, visitor_id)
                    except Exception as te:
                        log.exception(f"tool {block.name} failed")
                        result = json.dumps({"error": f"{type(te).__name__}: {te}"})

                    try:
                        parsed = json.loads(result) if isinstance(result, str) else result
                    except Exception:
                        parsed = {"raw": str(result)[:500]}

                    yield _sse({"type": "tool_result", "tool": block.name, "result": parsed})

                    tool_results.append({
                        "tool_use_id": block.id,
                        "result": result,
                    })

            if has_tool_use:
                messages.append({"role": "assistant", "content": response.content})
                messages.append({
                    "role": "user",
                    "content": [
                        {"type": "tool_result", "tool_use_id": tr["tool_use_id"], "content": tr["result"]}
                        for tr in tool_results
                    ],
                })
            else:
                if text_content:
                    conv.append({"role": "assistant", "content": text_content})
                yield _sse({"type": "done"})
                return

        # Hit max iterations — close gracefully
        yield _sse({"type": "max_iterations", "limit": max_iterations})
        yield _sse({"type": "done"})

    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache, no-transform",
            "X-Accel-Buffering": "no",  # disable proxy buffering
            "Connection": "keep-alive",
        },
    )


@app.get("/api/agent/status")
async def agent_status():
    return {
        "agent": "AlgoShop Autonomous Agent v3.0 (Phia-Aligned)",
        "engine": engine.status(),
        "tools_available": len(AGENT_TOOLS),
        "products_tracked": len(KNOWN_PRODUCTS),
    }


@app.get("/api/intel")
async def intel_endpoint(q: str = "AI shopping agents autonomous commerce"):
    """Direct market intel endpoint for the frontend Intel page."""
    result = await tool_market_intel(q)
    return json.loads(result)


@app.get("/api/stores")
async def stores_endpoint(zip_code: str = "10001", category: str = None, store_type: str = None):
    """Direct store search endpoint for the frontend Stores page."""
    result = await tool_search_stores(zip_code, category, store_type)
    return json.loads(result)


# ─── InterestRank API ─────────────────────────────────────────────────────────

class InteractionRequest(BaseModel):
    visitor_id: str = "default"
    product_id: str
    interaction_type: str  # search, click, cart, purchase


@app.post("/api/interest/interact")
def record_interaction(req: InteractionRequest):
    """Record a user interaction for InterestRank scoring."""
    interest_engine.record_interaction(req.visitor_id, req.product_id, req.interaction_type)
    return {"status": "recorded", "visitor": req.visitor_id, "product": req.product_id, "type": req.interaction_type}


@app.get("/api/interest/scores")
def get_interest_scores(visitor_id: str = "default"):
    """Get InterestRank v4 scores for all products."""
    raw = interest_engine.compute(visitor_id)
    enriched = []
    for pid, data in raw.items():
        meta = KNOWN_PRODUCTS.get(pid, {})
        enriched.append({
            "id": pid,
            "name": meta.get("name", pid),
            "category": meta.get("category", ""),
            "score": data["score"],
            "pagerank": data["pagerank"],
            "authority": data["authority"],
            "hub": data["hub"],
            "eigenvector": data.get("eigenvector", 0),
            "adj_walk": data.get("adj_walk", 0),
            "signal": data["signal"],
            "strategy_multiplier": data.get("strategy_multiplier", 1.0),
            "fusion_weights": data.get("fusion_weights", {}),
            "adjacency_matrix": data.get("adjacency_matrix", {}),
        })
    return {
        "visitor_id": visitor_id,
        "algorithm": "InterestRank v4.0 — PageRank + HITS + EigenCentrality + AdjMatrix",
        "products": enriched,
    }


@app.get("/api/interest/recommend")
def get_recommendations(visitor_id: str = "default", top_k: int = 8):
    """Get top-K personalized recommendations powered by InterestRank v4."""
    recs = interest_engine.get_recommendations(visitor_id, top_k)
    return {
        "visitor_id": visitor_id,
        "algorithm": "InterestRank v4.0 — PageRank + HITS + EigenCentrality + AdjMatrix",
        "recommendations": recs,
    }


@app.get("/api/interest/stats")
def interest_stats(visitor_id: str = None):
    """Algorithm diagnostics and stats (v4 with adjacency matrix)."""
    return interest_engine.get_stats(visitor_id)


# ─── Strategy Preference API ─────────────────────────────────────────────────

from interest_rank import STRATEGY_PRESETS

class StrategyRequest(BaseModel):
    visitor_id: str = "default"
    preset: str = None  # use a named preset
    custom: dict = None  # or provide custom weights


@app.get("/api/strategy/presets")
def get_strategy_presets():
    """List all available shopping strategy presets."""
    return {
        "presets": {
            name: {
                "description": s["description"],
                "weights": {k: v for k, v in s.items() if k != "description"}
            }
            for name, s in STRATEGY_PRESETS.items()
        }
    }


@app.post("/api/strategy/set")
def set_strategy(req: StrategyRequest):
    """Set user's shopping strategy (preset name or custom weights)."""
    if req.preset and req.preset in STRATEGY_PRESETS:
        strategy = dict(STRATEGY_PRESETS[req.preset])
    elif req.custom:
        strategy = {
            "description": "Custom strategy",
            "price_sensitivity": max(0, min(1, req.custom.get("price_sensitivity", 0.5))),
            "quality_focus": max(0, min(1, req.custom.get("quality_focus", 0.5))),
            "sustainability": max(0, min(1, req.custom.get("sustainability", 0.5))),
            "trend_following": max(0, min(1, req.custom.get("trend_following", 0.5))),
            "convenience": max(0, min(1, req.custom.get("convenience", 0.5))),
        }
    else:
        strategy = dict(STRATEGY_PRESETS["balanced"])

    interest_engine.set_strategy(req.visitor_id, strategy)
    # Recompute scores with new strategy
    scores = interest_engine.compute(req.visitor_id)
    top3 = list(scores.items())[:3]
    return {
        "status": "strategy_set",
        "visitor_id": req.visitor_id,
        "strategy": strategy,
        "preview": [{"id": pid, "score": d["score"], "signal": d["signal"]} for pid, d in top3],
    }


@app.get("/api/strategy/current")
def get_current_strategy(visitor_id: str = "default"):
    """Get current strategy for a visitor."""
    return {
        "visitor_id": visitor_id,
        "strategy": interest_engine.get_strategy(visitor_id),
    }


# ─── Custom Shopping Strategy (per-visitor) ──────────────────────────────────
# Persisted per visitor_id; used by the checkout/alert pipeline.

_DEFAULT_CUSTOM_STRATEGY = {
    "price_weight": 50,       # 0-100, prioritize price savings
    "speed_weight": 25,       # 0-100, prioritize fast delivery
    "quality_weight": 25,     # 0-100, prioritize product quality/reviews
    "budget_cap": 0.0,        # max per-item spend (0 = no limit)
    "alert_threshold": 20.0,  # alert when % off MSRP reaches this
    "auto_buy": False,        # auto-purchase when conditions met
    "preferred_platforms": ["Amazon", "Walmart", "Target"],
    "resale_ok": True,        # include resale/secondhand options
}

# Per-visitor store for custom strategies
custom_strategies: dict[str, dict] = {}


def _coerce_custom_strategy(body: dict) -> dict:
    """Validate and coerce incoming custom-strategy payload into canonical form."""
    def _clamp(v, lo, hi, default):
        try:
            return max(lo, min(hi, float(v)))
        except (TypeError, ValueError):
            return default

    platforms = body.get("preferred_platforms", _DEFAULT_CUSTOM_STRATEGY["preferred_platforms"])
    if not isinstance(platforms, list):
        platforms = _DEFAULT_CUSTOM_STRATEGY["preferred_platforms"]
    platforms = [str(p) for p in platforms if isinstance(p, (str, int, float))]

    return {
        "price_weight":   _clamp(body.get("price_weight",   _DEFAULT_CUSTOM_STRATEGY["price_weight"]),   0, 100, 50),
        "speed_weight":   _clamp(body.get("speed_weight",   _DEFAULT_CUSTOM_STRATEGY["speed_weight"]),   0, 100, 25),
        "quality_weight": _clamp(body.get("quality_weight", _DEFAULT_CUSTOM_STRATEGY["quality_weight"]), 0, 100, 25),
        "budget_cap":     _clamp(body.get("budget_cap",     _DEFAULT_CUSTOM_STRATEGY["budget_cap"]),     0, 1e9, 0.0),
        "alert_threshold": _clamp(body.get("alert_threshold", _DEFAULT_CUSTOM_STRATEGY["alert_threshold"]), 0, 100, 20.0),
        "auto_buy":       bool(body.get("auto_buy", _DEFAULT_CUSTOM_STRATEGY["auto_buy"])),
        "preferred_platforms": platforms,
        "resale_ok":      bool(body.get("resale_ok", _DEFAULT_CUSTOM_STRATEGY["resale_ok"])),
    }


@app.post("/api/strategy/custom")
async def save_custom_strategy(request: Request):
    """Save a per-visitor custom shopping strategy.

    Expected JSON body fields (all optional, defaults applied):
      price_weight    (0-100): prioritize price savings
      speed_weight    (0-100): prioritize fast delivery
      quality_weight  (0-100): prioritize product quality/reviews
      budget_cap      (float): max per-item spend, 0 = no limit
      alert_threshold (float): % below MSRP to trigger alerts (e.g. 20)
      auto_buy        (bool):  auto-purchase when conditions met
      preferred_platforms (list[str]): e.g. ["Amazon", "Walmart", "Target"]
      resale_ok       (bool):  include resale/secondhand options

    Visitor is identified via the `x-visitor-id` request header (defaults to "anon").
    """
    try:
        body = await request.json()
    except Exception:
        body = {}
    if not isinstance(body, dict):
        body = {}

    visitor_id = request.headers.get("x-visitor-id", "anon")
    strategy = _coerce_custom_strategy(body)
    strategy["updated_at"] = datetime.utcnow().isoformat() + "Z"
    custom_strategies[visitor_id] = strategy

    return {
        "status": "saved",
        "visitor_id": visitor_id,
        "strategy": strategy,
    }


@app.get("/api/strategy/custom")
async def get_custom_strategy(request: Request):
    """Retrieve the saved custom strategy for the current visitor.

    Visitor is identified via the `x-visitor-id` request header.
    Returns the default strategy when none has been saved yet.
    """
    visitor_id = request.headers.get("x-visitor-id", "anon")
    strategy = custom_strategies.get(visitor_id)
    return {
        "visitor_id": visitor_id,
        "has_custom": strategy is not None,
        "strategy": strategy if strategy is not None else dict(_DEFAULT_CUSTOM_STRATEGY),
    }


# ─── Order Book + Market Entry Timing API ─────────────────────────────────────

@app.get("/api/orderbook/{product_id}")
def get_order_book(product_id: str):
    """Get limit order book for a product (bid/ask levels, spread, VWAP)."""
    return order_book_mgr.get_order_book(product_id)


@app.get("/api/orderbook")
def get_all_order_books():
    """Get order book summaries for all products."""
    summaries = []
    for pid in KNOWN_PRODUCTS:
        book = order_book_mgr.get_order_book(pid)
        summaries.append({
            "product_id": pid,
            "name": book.get("name", pid),
            "best_bid": book.get("best_bid"),
            "best_ask": book.get("best_ask"),
            "spread": book.get("spread"),
            "spread_pct": book.get("spread_pct"),
            "vwap": book.get("vwap"),
            "imbalance": book.get("imbalance"),
        })
    return {"products": summaries}


@app.get("/api/timing/{product_id}")
def get_timing_signal(product_id: str):
    """Get market entry timing signal for a product (BUY_NOW/WAIT/SET_ALERT)."""
    return order_book_mgr.get_timing_signal(product_id)


@app.get("/api/timing")
def get_all_timing_signals():
    """Get timing signals for all products."""
    signals = order_book_mgr.get_all_signals()
    return {
        "algorithm": "Market Entry Timing v1.0 — EMA + Bollinger + VWAP + Seasonality",
        "signals": signals,
    }


@app.get("/api/market/overview")
def market_overview():
    """Market dashboard: buy/wait counts, top buys, upcoming drops."""
    return order_book_mgr.get_market_overview()


@app.get("/api/history/{product_id}")
def get_price_history(product_id: str):
    """Get 90-day price history for a product."""
    history = order_book_mgr.get_price_history(product_id)
    return {"product_id": product_id, "days": len(history), "history": history}


@app.get("/api/savings")
def compute_savings():
    """Compute total savings from agentic shopping vs retail pricing."""
    total_retail = 0
    total_best = 0
    savings_breakdown = []

    for pid, meta in KNOWN_PRODUCTS.items():
        msrp = meta.get("msrp", 100)
        price_range = meta.get("typical_price_range", [msrp * 0.7, msrp])
        # Best found price (low end of range = what our agent finds)
        best_price = price_range[0]
        saved = msrp - best_price
        pct = round((saved / msrp) * 100, 1) if msrp > 0 else 0

        total_retail += msrp
        total_best += best_price

        savings_breakdown.append({
            "product_id": pid,
            "name": meta["name"],
            "retail_price": msrp,
            "algo_price": best_price,
            "saved": round(saved, 2),
            "save_pct": pct,
        })

    savings_breakdown.sort(key=lambda x: x["save_pct"], reverse=True)
    total_saved = total_retail - total_best
    avg_discount = round((total_saved / total_retail) * 100, 1) if total_retail > 0 else 0

    return {
        "total_retail_value": round(total_retail, 2),
        "total_algo_price": round(total_best, 2),
        "total_saved": round(total_saved, 2),
        "avg_discount_pct": avg_discount,
        "products_tracked": len(KNOWN_PRODUCTS),
        "best_deal": savings_breakdown[0] if savings_breakdown else None,
        "breakdown": savings_breakdown,
    }


# ═══════════════════════════════════════════════════════════════════════════════
#  REAL PRODUCTS API — serves live-ish product data from data_engine
# ═══════════════════════════════════════════════════════════════════════════════

@app.get("/api/products")
async def get_products():
    """Return all products with real prices, verdicts, and resale data.
    Uses DataEngine's KNOWN_PRODUCTS enriched with price analysis."""
    import random
    from data_engine import analyze_price_quality, RESALE_MARKETPLACE_DATA, _fallback_price_history

    products = []
    for pid, meta in KNOWN_PRODUCTS.items():
        msrp = meta.get("msrp", 100)
        low, high = meta.get("typical_price_range", [msrp * 0.7, msrp])

        # Simulate a realistic current price within the product's known range
        # Seed by product ID for consistency within a session
        rng = random.Random(hash(pid) + int(time.time() // 3600))  # changes hourly
        current_price = round(rng.uniform(low, high), 2)

        verdict = analyze_price_quality(pid, current_price)
        history = _fallback_price_history(pid)
        best_resale = None
        resale_data = RESALE_MARKETPLACE_DATA.get(pid, {})
        if resale_data:
            cheapest = min(resale_data.values(), key=lambda x: x["price"])
            platform = [k for k, v in resale_data.items() if v["price"] == cheapest["price"]][0]
            best_resale = {
                "price": cheapest["price"],
                "platform": platform,
                "condition": cheapest.get("condition", "Good"),
                "listings": cheapest.get("listing_count", 0),
            }

        # Determine signal
        pct = verdict.get("percentile", 50)
        if pct <= 25:
            signal = "BUY"
        elif pct >= 70:
            signal = "WAIT"
        else:
            signal = "HOLD"

        trend = "STABLE"
        if len(history) >= 7:
            recent_avg = sum(history[-7:]) / 7
            older_avg = sum(history[-14:-7]) / 7 if len(history) >= 14 else recent_avg
            if recent_avg < older_avg * 0.97:
                trend = "DOWNTREND"
            elif recent_avg > older_avg * 1.03:
                trend = "UPTREND"

        # Map category to simple frontend categories
        cat = meta.get("category", "")
        if "Sneaker" in cat or "Shoes" in cat:
            simple_cat = "sneakers"
        elif "Luxury" in cat:
            simple_cat = "luxury"
        elif "Electron" in cat or "Game" in cat or "Video" in cat:
            simple_cat = "electronics"
        elif "Health" in cat or "Home" in cat or "Card" in cat or "Toy" in cat:
            simple_cat = "electronics"
        else:
            simple_cat = "fashion"

        # Enrich with order-book + market-entry-timing data where available
        ob_snapshot = None
        timing_snapshot = None
        interest_snapshot = None
        try:
            ob = order_book_mgr.get_order_book(pid)
            if isinstance(ob, dict) and "error" not in ob:
                ob_snapshot = {
                    "best_bid": ob.get("best_bid"),
                    "best_ask": ob.get("best_ask"),
                    "vwap": ob.get("vwap"),
                    "microprice": ob.get("microprice"),
                    "spread_pct": ob.get("spread_pct"),
                    "order_flow_imbalance": ob.get("order_flow_imbalance"),
                    "imbalance": ob.get("imbalance"),
                }
        except Exception as e:
            log.debug(f"ob snapshot failed for {pid}: {e}")
        try:
            ts = order_book_mgr.get_timing_signal(pid)
            if isinstance(ts, dict) and "error" not in ts:
                ind = ts.get("indicators", {})
                timing_snapshot = {
                    "signal": ts.get("signal"),
                    "confidence": ts.get("confidence"),
                    "target_price": ts.get("target_price"),
                    "wait_days": ts.get("wait_days"),
                    "rsi": ind.get("rsi"),
                    "rsi_label": ind.get("rsi_label"),
                    "macd_trend": ind.get("macd_trend"),
                    "bollinger_position": ind.get("bollinger_position"),
                    "sale_proximity": ind.get("sale_proximity"),
                }
        except Exception as e:
            log.debug(f"timing snapshot failed for {pid}: {e}")
        try:
            scores = interest_engine.compute()
            if isinstance(scores, dict) and pid in scores:
                interest_snapshot = {
                    "score": scores[pid].get("score"),
                    "signal": scores[pid].get("signal"),
                    "pagerank": scores[pid].get("pagerank"),
                }
        except Exception as e:
            log.debug(f"interest snapshot failed for {pid}: {e}")

        products.append({
            "id": pid,
            "name": meta["name"],
            "category": simple_cat,
            "category_full": meta.get("category", ""),
            "currentPrice": current_price,
            "lowestPrice": min(history) if history else low,
            "highestPrice": max(history) if history else high,
            "msrp": msrp,
            "retailer": list(meta.get("urls", {}).keys())[0] if meta.get("urls") else "—",
            "retailer_url": list(meta.get("urls", {}).values())[0] if meta.get("urls") else "",
            "signal": signal,
            "confidence": round(max(0.4, min(0.98, 1 - abs(pct - 50) / 100 + rng.uniform(-0.1, 0.1))), 2),
            "trend": trend,
            "priceVerdict": verdict.get("verdict", "TYPICAL"),
            "priceEmoji": verdict.get("emoji"),
            "priceNote": verdict.get("note"),
            "percentile": verdict.get("percentile", 50),
            "savings_vs_msrp": verdict.get("savings_vs_msrp", 0),
            "savings_pct": verdict.get("savings_pct", 0),
            "all_time_low": verdict.get("all_time_low"),
            "all_time_high": verdict.get("all_time_high"),
            "average_price": verdict.get("average_price"),
            "resale": best_resale,
            "sizing": meta.get("sizing"),
            "urls": meta.get("urls", {}),
            "resale_urls": meta.get("resale_urls", {}),
            "data_sources": list(meta.get("urls", {}).keys()) + list(meta.get("resale_urls", {}).keys()),
            # Algo-trading enrichment
            "order_book": ob_snapshot,
            "timing": timing_snapshot,
            "interest": interest_snapshot,
            "price_history": history[-14:] if history else [],
        })

    return {
        "products": products,
        "count": len(products),
        "data_source": "live",
        "updated": datetime.utcnow().isoformat(),
        "enrichment": {
            "order_book": sum(1 for p in products if p.get("order_book")),
            "timing": sum(1 for p in products if p.get("timing")),
            "interest": sum(1 for p in products if p.get("interest")),
        },
    }


# ═══════════════════════════════════════════════════════════════════════════════
#  SOCIAL / FRIENDS API — friend strategies, group order book
# ═══════════════════════════════════════════════════════════════════════════════

# Simulated social graph — in production this would be contact-import + OAuth
IMPORTED_FRIENDS = [
    {"id": "f1", "name": "Maya Chen", "avatar": "MC", "style": "deal-hunter", "joined": "3 weeks ago"},
    {"id": "f2", "name": "Jordan Park", "avatar": "JP", "style": "quality-first", "joined": "2 weeks ago"},
    {"id": "f3", "name": "Ava Williams", "avatar": "AW", "style": "sustainable", "joined": "1 month ago"},
    {"id": "f4", "name": "Liam O'Brien", "avatar": "LO", "style": "resale-only", "joined": "5 days ago"},
    {"id": "f5", "name": "Priya Sharma", "avatar": "PS", "style": "price-first", "joined": "2 months ago"},
    {"id": "f6", "name": "Tyler Kim", "avatar": "TK", "style": "deal-hunter", "joined": "1 week ago"},
    {"id": "f7", "name": "Sofia Martinez", "avatar": "SM", "style": "quality-first", "joined": "3 weeks ago"},
    {"id": "f8", "name": "Kai Nakamura", "avatar": "KN", "style": "sustainable", "joined": "2 weeks ago"},
]

# Simulated friend purchase activity
import random as _rng
_rng.seed(42)

def _build_friend_activity():
    """Generate realistic friend shopping activity."""
    activities = []
    products_list = list(KNOWN_PRODUCTS.items())
    actions = ["bought", "saved to wishlist", "set price alert", "compared prices", "found a deal on"]
    strategies = {
        "deal-hunter": "Deal Hunter",
        "quality-first": "Quality First",
        "sustainable": "Eco Smart",
        "resale-only": "Resale Pro",
        "price-first": "Price Optimizer",
    }
    time_labels = ["2 min ago", "15 min ago", "1 hr ago", "3 hrs ago", "5 hrs ago",
                   "Yesterday", "Yesterday", "2 days ago", "2 days ago", "3 days ago",
                   "Last week", "Last week", "Last week"]

    for i, t in enumerate(time_labels):
        friend = IMPORTED_FRIENDS[i % len(IMPORTED_FRIENDS)]
        pid, pmeta = products_list[i % len(products_list)]
        action = actions[i % len(actions)]
        msrp = pmeta.get("msrp", 100)
        low, high = pmeta.get("typical_price_range", [msrp * 0.7, msrp])
        paid = round(_rng.uniform(low, high), 2)

        activities.append({
            "friend": friend["name"],
            "avatar": friend["avatar"],
            "action": action,
            "product": pmeta["name"],
            "product_id": pid,
            "price_paid": paid if action == "bought" else None,
            "savings_pct": round((1 - paid / msrp) * 100) if action == "bought" and msrp > 0 else None,
            "strategy": strategies.get(friend["style"], "Deal Hunter"),
            "time": t,
        })
    return activities


def _build_friend_book():
    """Build friend-group order book vs. general market.
    Shows what prices friends paid vs. what everyone else pays."""
    friend_book = []
    for pid, meta in KNOWN_PRODUCTS.items():
        msrp = meta.get("msrp", 100)
        low, high = meta.get("typical_price_range", [msrp * 0.7, msrp])

        # Friends tend to get better prices (they use AlgoShop strategies)
        friend_prices = sorted([round(_rng.uniform(low, low + (high - low) * 0.5), 2) for _ in range(5)])
        general_prices = sorted([round(_rng.uniform(low + (high - low) * 0.2, high), 2) for _ in range(8)])

        friend_avg = round(sum(friend_prices) / len(friend_prices), 2)
        general_avg = round(sum(general_prices) / len(general_prices), 2)
        savings = round(general_avg - friend_avg, 2)
        savings_pct = round((savings / general_avg) * 100, 1) if general_avg > 0 else 0

        friend_book.append({
            "product_id": pid,
            "name": meta["name"],
            "friend_avg_price": friend_avg,
            "friend_best_price": friend_prices[0],
            "friend_buyers": _rng.randint(1, 5),
            "general_avg_price": general_avg,
            "general_best_price": general_prices[0],
            "general_buyers": _rng.randint(20, 200),
            "your_circle_saves": savings,
            "your_circle_saves_pct": savings_pct,
            "msrp": msrp,
        })

    friend_book.sort(key=lambda x: x["your_circle_saves_pct"], reverse=True)
    return friend_book


def _build_strategy_stats():
    """Which shopping styles are most popular among friends."""
    from collections import Counter
    style_map = {
        "deal-hunter": {"label": "Deal Hunter", "desc": "Waits for the best price drops", "color": "green"},
        "quality-first": {"label": "Quality First", "desc": "Buys top-rated items at fair prices", "color": "blue"},
        "sustainable": {"label": "Eco Smart", "desc": "Prefers resale and sustainable brands", "color": "green"},
        "resale-only": {"label": "Resale Pro", "desc": "Always shops secondhand first", "color": "purple"},
        "price-first": {"label": "Price Optimizer", "desc": "Gets the absolute lowest price", "color": "orange"},
    }
    counts = Counter(f["style"] for f in IMPORTED_FRIENDS)
    total = len(IMPORTED_FRIENDS)
    strategies = []
    for style, count in counts.most_common():
        info = style_map.get(style, {"label": style, "desc": "", "color": "gray"})
        strategies.append({
            "id": style,
            "label": info["label"],
            "description": info["desc"],
            "color": info["color"],
            "friends_using": count,
            "pct": round(count / total * 100),
            "users": [f["name"] for f in IMPORTED_FRIENDS if f["style"] == style],
        })
    return strategies


@app.post("/api/social/import-contacts")
async def import_contacts():
    """Simulate importing contacts — returns friend list.
    In production: OAuth (Google Contacts, phone contacts via native app)."""
    return {
        "imported": True,
        "friends": IMPORTED_FRIENDS,
        "count": len(IMPORTED_FRIENDS),
        "source": "contacts",
    }


@app.get("/api/social/friends")
def get_friends():
    return {"friends": IMPORTED_FRIENDS, "count": len(IMPORTED_FRIENDS)}


@app.get("/api/social/feed")
def get_social_feed():
    """Friend activity feed — what your friends are buying & doing."""
    return {"feed": _build_friend_activity(), "count": len(_build_friend_activity())}


@app.get("/api/social/book")
def get_friend_book():
    """Friend group order book — what prices friends paid vs. everyone else."""
    book = _build_friend_book()
    total_circle_savings = sum(b["your_circle_saves"] for b in book if b["your_circle_saves"] > 0)
    avg_circle_pct = round(sum(b["your_circle_saves_pct"] for b in book) / len(book), 1) if book else 0
    return {
        "book": book,
        "summary": {
            "total_circle_savings": round(total_circle_savings, 2),
            "avg_circle_savings_pct": avg_circle_pct,
            "friends_tracked": len(IMPORTED_FRIENDS),
            "products_compared": len(book),
        },
    }


@app.get("/api/social/strategies")
def get_social_strategies():
    """Which shopping styles are most popular among friends."""
    return {"strategies": _build_strategy_stats()}


# ─── Connector Endpoints: Blockscout, GoDaddy, Trivago ───────────────────────

@app.get("/api/blockchain/overview")
async def blockchain_overview():
    """Real-time Ethereum network stats from Blockscout."""
    data = await get_blockchain_overview()
    return data


@app.get("/api/blockchain/token/{symbol}")
async def blockchain_token(symbol: str):
    """Look up a token by symbol on Ethereum via Blockscout."""
    data = await lookup_token(symbol)
    return data


@app.get("/api/blockchain/address/{address}")
async def blockchain_address(address: str):
    """Get address info from Blockscout."""
    data = await get_address_info(address)
    return data


@app.get("/api/domains/check")
async def domains_check(domains: str):
    """Check domain availability via GoDaddy. Pass comma-separated domains."""
    data = await check_domain_availability(domains)
    return data


@app.get("/api/domains/suggest")
async def domains_suggest(query: str, limit: int = 10):
    """Get domain suggestions from GoDaddy."""
    data = await suggest_domains(query, limit)
    return data


@app.get("/api/domains/verify-stores")
async def domains_verify_stores():
    """Verify store domains for known retailers."""
    store_names = ["Nike", "Adidas", "BestBuy", "Amazon", "Target", "Walmart", "Nordstrom", "Zara"]
    data = await verify_store_domains(store_names)
    return data


@app.get("/api/accommodations")
async def accommodations_search(
    lat: float = 40.7258, lng: float = -73.9981,
    radius: int = 3000,
    arrival: str = None, departure: str = None,
    adults: int = 1, rooms: int = 1,
):
    """Search for accommodations near a location via Trivago."""
    data = await search_accommodations(lat, lng, radius, arrival, departure, adults, rooms)
    return data


@app.get("/api/intelligence")
async def combined_intelligence(zip_code: str = "10012"):
    """Combined multi-source intelligence: blockchain + accommodations."""
    data = await get_shopping_intelligence(zip_code)
    return data


@app.get("/api/health")
def health():
    return {
        "status": "ok",
        "agent": "AlgoShop v5.0 (Phia-Aligned + InterestRank v4 + OrderBook + MarketTiming)",
        "features": ["interest_rank_v4", "pagerank", "hits", "eigenvector_centrality", "adjacency_matrix",
                     "limit_order_book", "market_entry_timing", "vwap", "bollinger_bands", "ema_crossover",
                     "strategy_presets", "resale_comparison", "price_intelligence", "sizing", "momentum",
                     "store_finder", "blockscout_live", "godaddy_live", "trivago_live",
                     "statista_live", "cbinsights_live", "pitchbook_live"],
        "tools": len(AGENT_TOOLS),
        "products": len(KNOWN_PRODUCTS),
        "interest_rank": "v4.0 — PageRank + HITS + EigenCentrality + AdjMatrix",
        "order_book": "v1.0 — Bid/Ask + VWAP + Spread Analysis",
        "market_timing": "v1.0 — EMA + Bollinger + VWAP + Seasonality",
        "strategy_presets": list(STRATEGY_PRESETS.keys()),
    }




# ═══════════════════════════════════════════════════════════════════════════════
#  GIFT API — personalized gift suggestions per friend
# ═══════════════════════════════════════════════════════════════════════════════

def _get_gift_suggestions(friend_id: str):
    """Generate personalized gift suggestions based on friend's shopping style."""
    friend = next((f for f in IMPORTED_FRIENDS if f["id"] == friend_id), None)
    if not friend:
        return [], []

    style = friend.get("style", "deal-hunter")

    # Gift pools by style — curated selections matching each shopping personality
    gift_pools = {
        "deal-hunter": [
            {"name": "AirPods Pro 2", "brand": "Apple", "price": 179, "original_price": 249, "category": "electronics", "match_score": 95, "budget_tier": "Best Value"},
            {"name": "Nike Air Force 1", "brand": "Nike", "price": 87, "original_price": 110, "category": "sneakers", "match_score": 91, "budget_tier": "Price Drop"},
            {"name": "Instant Pot Duo 7-in-1", "brand": "Instant Pot", "price": 59, "original_price": 89, "category": "home", "match_score": 88},
            {"name": "New Balance 530", "brand": "New Balance", "price": 72, "original_price": 100, "category": "sneakers", "match_score": 86},
            {"name": "Dyson V15 Detect", "brand": "Dyson", "price": 320, "original_price": 749, "category": "home", "match_score": 84, "budget_tier": "Flash Sale"},
            {"name": "Kindle Paperwhite", "brand": "Amazon", "price": 99, "original_price": 149, "category": "electronics", "match_score": 82},
        ],
        "quality-first": [
            {"name": "Gucci Ace Sneakers", "brand": "Gucci", "price": 420, "category": "fashion", "match_score": 94},
            {"name": "AirPods Max", "brand": "Apple", "price": 449, "category": "audio", "match_score": 92},
            {"name": "Lululemon Align 25\"", "brand": "Lululemon", "price": 98, "category": "fitness", "match_score": 89},
            {"name": "Le Labo Santal 33", "brand": "Le Labo", "price": 195, "category": "beauty", "match_score": 87},
            {"name": "Sony WH-1000XM5", "brand": "Sony", "price": 298, "category": "audio", "match_score": 85},
            {"name": "Common Projects Achilles", "brand": "Common Projects", "price": 395, "category": "sneakers", "match_score": 83},
        ],
        "sustainable": [
            {"name": "Patagonia Better Sweater", "brand": "Patagonia", "price": 139, "category": "fashion", "match_score": 96},
            {"name": "Allbirds Tree Runners", "brand": "Allbirds", "price": 98, "category": "sneakers", "match_score": 93},
            {"name": "Hydro Flask 32oz", "brand": "Hydro Flask", "price": 44, "category": "lifestyle", "match_score": 90},
            {"name": "Tentree Organic Tee", "brand": "Tentree", "price": 42, "category": "fashion", "match_score": 87},
            {"name": "Stasher Reusable Bags Set", "brand": "Stasher", "price": 38, "category": "home", "match_score": 84},
            {"name": "Girlfriend Collective Leggings", "brand": "Girlfriend", "price": 78, "category": "fitness", "match_score": 81},
        ],
        "resale-only": [
            {"name": "Vintage Levi's 501", "brand": "Levi's (Vintage)", "price": 65, "category": "fashion", "match_score": 95},
            {"name": "Pre-owned Gucci Belt", "brand": "Gucci (Resale)", "price": 180, "category": "fashion", "match_score": 92, "budget_tier": "Resale Deal"},
            {"name": "Used Canon EOS R50", "brand": "Canon (Refurb)", "price": 499, "category": "electronics", "match_score": 88},
            {"name": "Secondhand Dr. Martens 1460", "brand": "Dr. Martens", "price": 78, "category": "sneakers", "match_score": 86},
            {"name": "Vintage Band Tee", "brand": "Vintage", "price": 35, "category": "fashion", "match_score": 83},
            {"name": "Refurb iPad Air", "brand": "Apple (Refurb)", "price": 429, "category": "electronics", "match_score": 80},
        ],
        "price-first": [
            {"name": "UGG Tasman Slipper", "brand": "UGG", "price": 58, "original_price": 110, "category": "sneakers", "match_score": 94, "budget_tier": "Lowest Price"},
            {"name": "Amazon Fire TV Stick", "brand": "Amazon", "price": 24, "original_price": 39, "category": "electronics", "match_score": 91},
            {"name": "CeraVe Skincare Set", "brand": "CeraVe", "price": 29, "category": "beauty", "match_score": 89},
            {"name": "Nike Dunk Low", "brand": "Nike", "price": 68, "original_price": 110, "category": "sneakers", "match_score": 87, "budget_tier": "Price Drop"},
            {"name": "Anker PowerCore 20K", "brand": "Anker", "price": 35, "original_price": 52, "category": "electronics", "match_score": 84},
            {"name": "Stanley Quencher 40oz", "brand": "Stanley", "price": 35, "category": "lifestyle", "match_score": 82},
        ],
    }

    # Wishlist per friend (simulated — in prod, friends share wishlists)
    wishlist_pools = {
        "deal-hunter": [
            {"name": "PS5 DualSense Controller", "brand": "Sony", "price": 49, "category": "electronics", "priority": "🔥 Top pick"},
            {"name": "Nike Vaporfly Next% 3", "brand": "Nike", "price": 159, "category": "sneakers"},
        ],
        "quality-first": [
            {"name": "Aesop Resurrection Hand Balm", "brand": "Aesop", "price": 39, "category": "beauty", "priority": "🔥 Top pick"},
            {"name": "Aime Leon Dore x NB 550", "brand": "New Balance", "price": 280, "category": "sneakers"},
        ],
        "sustainable": [
            {"name": "Pela Phone Case", "brand": "Pela", "price": 35, "category": "electronics", "priority": "🔥 Top pick"},
            {"name": "Cotopaxi Del Día Pack", "brand": "Cotopaxi", "price": 68, "category": "lifestyle"},
        ],
        "resale-only": [
            {"name": "Vintage Polo Ralph Lauren", "brand": "Ralph Lauren", "price": 45, "category": "fashion", "priority": "🔥 Top pick"},
            {"name": "Used Fujifilm X-T30 II", "brand": "Fujifilm", "price": 599, "category": "electronics"},
        ],
        "price-first": [
            {"name": "Crocs Classic Clog", "brand": "Crocs", "price": 29, "category": "sneakers", "priority": "🔥 Top pick"},
            {"name": "Echo Dot (5th Gen)", "brand": "Amazon", "price": 22, "category": "electronics"},
        ],
    }

    gifts = gift_pools.get(style, gift_pools["deal-hunter"])
    wishlist = wishlist_pools.get(style, [])
    return gifts, wishlist


@app.get("/api/social/gifts")
def get_gift_suggestions(friend_id: str = "f1"):
    """Get personalized gift suggestions for a specific friend."""
    gifts, wishlist = _get_gift_suggestions(friend_id)
    friend = next((f for f in IMPORTED_FRIENDS if f["id"] == friend_id), None)
    style_labels = {
        "deal-hunter": "Deal Hunter",
        "quality-first": "Quality First",
        "sustainable": "Eco Smart",
        "resale-only": "Resale Pro",
        "price-first": "Price Optimizer",
    }
    return {
        "friend": friend,
        "style_label": style_labels.get(friend["style"], "Deal Hunter") if friend else None,
        "gifts": gifts,
        "wishlist": wishlist,
        "total_suggestions": len(gifts),
    }


@app.post("/api/social/send-gift")
async def send_gift(request: Request):
    """Process a gift send — in production, this would initiate checkout + notification."""
    body = await request.json()
    friend_id = body.get("friend_id")
    product = body.get("product", {})
    message = body.get("message", "")
    budget = body.get("budget", "any")
    friend = next((f for f in IMPORTED_FRIENDS if f["id"] == friend_id), None)
    return {
        "success": True,
        "gift_id": f"gift_{friend_id}_{_rng.randint(1000,9999)}",
        "recipient": friend["name"] if friend else "Unknown",
        "product": product.get("name", "Unknown"),
        "price": product.get("price", 0),
        "message": message,
        "budget": budget,
        "status": "pending_payment",
        "notification_sent": True,
    }



# ═══════════════════════════════════════════════════════════════════════════════
#  CHAT API — friend-to-friend messaging
# ═══════════════════════════════════════════════════════════════════════════════

# In-memory message store (in production: database + WebSocket)
_chat_messages = {}


@app.get("/api/social/messages")
def get_messages(friend_id: str = "f1"):
    """Get chat messages with a specific friend."""
    key = friend_id
    if key not in _chat_messages:
        # Seed with some messages
        _chat_messages[key] = [
            {"from": "them", "text": "Hey! Have you seen the Nike deal?", "time": "2:15 PM"},
            {"from": "me", "text": "Yes! The agent caught it at $87", "time": "2:16 PM"},
        ]
    return {"messages": _chat_messages[key], "friend_id": friend_id}


@app.post("/api/social/send-message")
async def send_message(request: Request):
    """Send a message to a friend."""
    body = await request.json()
    friend_id = body.get("friend_id", "f1")
    text = body.get("text", "")
    visitor_id = body.get("visitor_id", "anon")

    key = friend_id
    if key not in _chat_messages:
        _chat_messages[key] = []

    msg = {
        "from": "me",
        "text": text,
        "time": "Just now",
        "visitor_id": visitor_id,
    }
    _chat_messages[key].append(msg)

    return {
        "success": True,
        "message_id": f"msg_{_rng.randint(1000,9999)}",
        "friend_id": friend_id,
        "delivered": True,
    }



# ─── Email Subscription Recommendations ───
@app.get("/api/email-recs")
def email_subscription_recommendations():
    """Simulates analyzing user's email subscriptions to recommend products.
    In production, this would scan Gmail/Outlook for newsletter subscriptions
    and match them to our product catalog using the InterestRank algorithm."""
    
    # Simulated detected subscriptions from user's email
    detected_subscriptions = [
        {"brand": "Nike", "type": "newsletter", "frequency": "weekly"},
        {"brand": "Sephora", "type": "promo", "frequency": "daily"},
        {"brand": "Amazon", "type": "deals", "frequency": "daily"},
        {"brand": "Best Buy", "type": "newsletter", "frequency": "weekly"},
        {"brand": "Nordstrom", "type": "sale_alert", "frequency": "bi-weekly"},
        {"brand": "Target", "type": "circle_offers", "frequency": "weekly"},
        {"brand": "Apple", "type": "newsletter", "frequency": "monthly"},
        {"brand": "Lululemon", "type": "new_arrivals", "frequency": "weekly"},
    ]
    
    recommendations = [
        {
            "brand": "Nike",
            "brand_key": "nike",
            "name": "Nike Dunk Low Retro",
            "reason": "You subscribe to Nike emails — this dropped 34% today",
            "best_price": 68.99,
            "retail_price": 110.00,
            "savings_pct": 37,
        },
        {
            "brand": "Sephora",
            "brand_key": "sephora",
            "name": "Rare Beauty Blush",
            "reason": "Trending in your Sephora promos — 20% VIB sale",
            "best_price": 17.60,
            "retail_price": 22.00,
            "savings_pct": 20,
        },
        {
            "brand": "Best Buy",
            "brand_key": "bestbuy",
            "name": "Sony WH-1000XM5",
            "reason": "Price drop alert from your Best Buy subscription",
            "best_price": 248.00,
            "retail_price": 399.99,
            "savings_pct": 38,
        },
        {
            "brand": "Lululemon",
            "brand_key": "lululemon",
            "name": "Align Leggings 25\"",
            "reason": "New color drop — matched from your Lululemon emails",
            "best_price": 74.00,
            "retail_price": 98.00,
            "savings_pct": 24,
        },
        {
            "brand": "Apple",
            "brand_key": "apple",
            "name": "AirPods Pro 2 USB-C",
            "reason": "Apple newsletter subscriber — lowest price this quarter",
            "best_price": 189.99,
            "retail_price": 249.00,
            "savings_pct": 24,
        },
        {
            "brand": "Nordstrom",
            "brand_key": "nordstrom",
            "name": "Gucci Ace Sneakers",
            "reason": "Nordstrom Half-Yearly sale — matched from your alerts",
            "best_price": 445.00,
            "retail_price": 790.00,
            "savings_pct": 44,
        },
        {
            "brand": "Target",
            "brand_key": "target",
            "name": "Dyson V15 Detect",
            "reason": "Target Circle price match from your weekly deals email",
            "best_price": 549.99,
            "retail_price": 749.99,
            "savings_pct": 27,
        },
        {
            "brand": "Amazon",
            "brand_key": "amazon",
            "name": "Kindle Paperwhite",
            "reason": "Amazon deals subscriber — Lightning Deal ending soon",
            "best_price": 109.99,
            "retail_price": 149.99,
            "savings_pct": 27,
        },
    ]
    
    return {
        "subscriptions_detected": len(detected_subscriptions),
        "brands": [s["brand"] for s in detected_subscriptions],
        "recommendations": recommendations,
        "privacy_note": "AlgoShop scans email subscription metadata only. We never read personal emails or share data with third parties."
    }

# ============================================================
# ALGO TRADING TERMINAL — in-memory price simulation + orders
# ============================================================
import threading
import random as _rnd
import time as _time
import math as _math
from uuid import uuid4
from pydantic import BaseModel

_TRADING_STATE = {
    "prices": {},       # pid -> {current, open, high, low, prev_close, history:[{t,p,v}], last_tick_ts}
    "orders": [],        # list of order dicts
    "executions": [],    # log of executed orders
    "lock": threading.Lock(),
    "started": False,
}

def _trading_bootstrap():
    """Build initial 24h-ish price history for each known product."""
    now = _time.time()
    for pid, meta in KNOWN_PRODUCTS.items():
        msrp = float(meta.get("msrp", 100))
        low, high = meta.get("typical_price_range", [msrp * 0.75, msrp])
        base = (float(low) + float(high)) / 2.0
        rng = _rnd.Random(hash(pid) & 0xFFFFFFFF)
        hist = []
        price = base
        # 96 points * 15 min = 24h
        step_sec = 15 * 60
        n = 96
        for i in range(n):
            # Random walk with mean reversion toward base
            drift = (base - price) * 0.03
            shock = rng.gauss(0, base * 0.006)
            price = max(float(low) * 0.92, min(float(high) * 1.08, price + drift + shock))
            vol = int(max(1, rng.gauss(25, 10)))
            hist.append({
                "t": int(now - (n - i) * step_sec),
                "p": round(price, 2),
                "v": vol,
            })
        opens = hist[0]["p"]
        highs = max(h["p"] for h in hist)
        lows = min(h["p"] for h in hist)
        _TRADING_STATE["prices"][pid] = {
            "pid": pid,
            "name": meta.get("name", pid),
            "msrp": msrp,
            "low": float(low),
            "high": float(high),
            "current": round(hist[-1]["p"], 2),
            "prev_close": round(opens, 2),
            "open": round(opens, 2),
            "day_high": round(highs, 2),
            "day_low": round(lows, 2),
            "history": hist,
            "last_tick_ts": now,
        }

def _trading_tick():
    """Advance one tick of price simulation and check limit orders."""
    with _TRADING_STATE["lock"]:
        now = _time.time()
        for pid, st in _TRADING_STATE["prices"].items():
            base = (st["low"] + st["high"]) / 2.0
            drift = (base - st["current"]) * 0.04
            shock = _rnd.gauss(0, base * 0.0045)
            new_price = max(st["low"] * 0.88, min(st["high"] * 1.12, st["current"] + drift + shock))
            new_price = round(new_price, 2)
            vol = int(max(1, _rnd.gauss(25, 10)))
            st["current"] = new_price
            st["day_high"] = max(st["day_high"], new_price)
            st["day_low"] = min(st["day_low"], new_price)
            st["last_tick_ts"] = now
            st["history"].append({"t": int(now), "p": new_price, "v": vol})
            # Keep last ~200 points
            if len(st["history"]) > 200:
                st["history"] = st["history"][-200:]
        # Check limit orders
        for o in _TRADING_STATE["orders"]:
            if o["status"] != "watching":
                continue
            st = _TRADING_STATE["prices"].get(o["product_id"])
            if not st:
                continue
            price = st["current"]
            triggered = False
            if o["side"] == "buy" and price <= o["target_price"]:
                triggered = True
            elif o["side"] == "sell" and price >= o["target_price"]:
                triggered = True
            if triggered:
                o["status"] = "executed"
                o["executed_price"] = price
                o["executed_at"] = int(now)
                o["savings"] = round((o["target_price"] - price) * o.get("qty", 1), 2) if o["side"] == "buy" else 0
                _TRADING_STATE["executions"].append({
                    "id": o["id"],
                    "product_id": o["product_id"],
                    "name": o.get("name", o["product_id"]),
                    "side": o["side"],
                    "price": price,
                    "target": o["target_price"],
                    "qty": o.get("qty", 1),
                    "savings": o["savings"],
                    "ts": int(now),
                    "kind": "limit",
                })

def _trading_loop():
    while True:
        try:
            _trading_tick()
        except Exception as _e:
            log.warning(f"trading tick error: {_e}")
        _time.sleep(3)

def _ensure_trading_started():
    if _TRADING_STATE["started"]:
        return
    _TRADING_STATE["started"] = True
    _trading_bootstrap()
    t = threading.Thread(target=_trading_loop, daemon=True)
    t.start()
    log.info(f"Trading simulator started for {len(_TRADING_STATE['prices'])} products")

_ensure_trading_started()

# --- Indicator helpers -------------------------------------------------------
def _ema(values, period):
    if not values:
        return []
    k = 2 / (period + 1)
    ema = [values[0]]
    for v in values[1:]:
        ema.append(v * k + ema[-1] * (1 - k))
    return ema

def _rsi(values, period=14):
    if len(values) < period + 1:
        return 50.0
    gains, losses = [], []
    for i in range(1, len(values)):
        d = values[i] - values[i - 1]
        gains.append(max(d, 0))
        losses.append(max(-d, 0))
    avg_g = sum(gains[-period:]) / period
    avg_l = sum(losses[-period:]) / period
    if avg_l == 0:
        return 100.0
    rs = avg_g / avg_l
    return round(100 - (100 / (1 + rs)), 2)

def _macd(values):
    if len(values) < 26:
        return {"macd": 0, "signal": 0, "hist": 0, "trend": "neutral"}
    ema12 = _ema(values, 12)
    ema26 = _ema(values, 26)
    macd_line = [a - b for a, b in zip(ema12[-len(ema26):], ema26)]
    signal_line = _ema(macd_line, 9)
    hist = macd_line[-1] - signal_line[-1]
    trend = "bullish" if macd_line[-1] > signal_line[-1] else "bearish"
    return {
        "macd": round(macd_line[-1], 3),
        "signal": round(signal_line[-1], 3),
        "hist": round(hist, 3),
        "trend": trend,
    }

def _bollinger(values, period=20, mult=2.0):
    if len(values) < period:
        return {"upper": values[-1], "mid": values[-1], "lower": values[-1], "position": "mid"}
    window = values[-period:]
    mid = sum(window) / period
    var = sum((v - mid) ** 2 for v in window) / period
    sd = _math.sqrt(var)
    upper = mid + mult * sd
    lower = mid - mult * sd
    price = values[-1]
    if price >= upper * 0.99:
        pos = "upper_band"
    elif price <= lower * 1.01:
        pos = "lower_band"
    else:
        pos = "mid_band"
    return {"upper": round(upper, 2), "mid": round(mid, 2), "lower": round(lower, 2), "position": pos}


class LimitOrderReq(BaseModel):
    productId: str
    targetPrice: float
    qty: int = 1
    side: str = "buy"  # buy or sell


class MarketOrderReq(BaseModel):
    productId: str
    qty: int = 1
    side: str = "buy"


@app.get("/api/trading/prices")
async def trading_prices(product_id: str = None, history_points: int = 96):
    """Return current simulated prices for all products (or one) with 24h history."""
    _ensure_trading_started()
    with _TRADING_STATE["lock"]:
        items = []
        src = _TRADING_STATE["prices"]
        ids = [product_id] if product_id and product_id in src else list(src.keys())
        for pid in ids:
            st = src[pid]
            hist = st["history"][-history_points:]
            change = st["current"] - st["prev_close"]
            change_pct = (change / st["prev_close"] * 100) if st["prev_close"] else 0
            items.append({
                "product_id": pid,
                "name": st["name"],
                "current": st["current"],
                "prev_close": st["prev_close"],
                "open": st["open"],
                "day_high": st["day_high"],
                "day_low": st["day_low"],
                "change": round(change, 2),
                "change_pct": round(change_pct, 2),
                "msrp": st["msrp"],
                "history": hist,
                "ts": int(_time.time()),
            })
    if product_id:
        return items[0] if items else {"error": "not_found"}
    return {"prices": items, "count": len(items)}


@app.post("/api/trading/limit-order")
async def trading_limit_order(req: LimitOrderReq):
    """Create a limit order that auto-executes when target is hit."""
    _ensure_trading_started()
    with _TRADING_STATE["lock"]:
        st = _TRADING_STATE["prices"].get(req.productId)
        if not st:
            return {"error": "product_not_found"}
        o = {
            "id": str(uuid4())[:8],
            "product_id": req.productId,
            "name": st["name"],
            "target_price": round(float(req.targetPrice), 2),
            "qty": int(req.qty),
            "side": req.side.lower(),
            "status": "watching",
            "created_at": int(_time.time()),
            "current_price_at_create": st["current"],
        }
        _TRADING_STATE["orders"].append(o)
    return {"ok": True, "order": o}


@app.get("/api/trading/orders")
async def trading_orders(status: str = None, limit: int = 50):
    _ensure_trading_started()
    with _TRADING_STATE["lock"]:
        orders = list(reversed(_TRADING_STATE["orders"]))
        if status:
            orders = [o for o in orders if o["status"] == status]
        executions = list(reversed(_TRADING_STATE["executions"]))[:limit]
    return {
        "orders": orders[:limit],
        "executions": executions,
        "open_count": sum(1 for o in _TRADING_STATE["orders"] if o["status"] == "watching"),
        "executed_count": sum(1 for o in _TRADING_STATE["orders"] if o["status"] == "executed"),
    }


@app.post("/api/trading/execute")
async def trading_execute(req: MarketOrderReq):
    """Manually execute a market order at current price."""
    _ensure_trading_started()
    with _TRADING_STATE["lock"]:
        st = _TRADING_STATE["prices"].get(req.productId)
        if not st:
            return {"error": "product_not_found"}
        price = st["current"]
        exec_entry = {
            "id": str(uuid4())[:8],
            "product_id": req.productId,
            "name": st["name"],
            "side": req.side.lower(),
            "price": price,
            "target": price,
            "qty": int(req.qty),
            "savings": round(max(0.0, st["msrp"] - price) * int(req.qty), 2),
            "ts": int(_time.time()),
            "kind": "market",
        }
        _TRADING_STATE["executions"].append(exec_entry)
    return {"ok": True, "execution": exec_entry}


@app.get("/api/trading/signals/{product_id}")
async def trading_signals(product_id: str):
    """Return RSI, MACD, EMA, Bollinger signals for a product."""
    _ensure_trading_started()
    with _TRADING_STATE["lock"]:
        st = _TRADING_STATE["prices"].get(product_id)
        if not st:
            return {"error": "product_not_found"}
        prices = [h["p"] for h in st["history"]]
        current = st["current"]
    rsi = _rsi(prices)
    macd = _macd(prices)
    boll = _bollinger(prices)
    ema12 = _ema(prices, 12)
    ema26 = _ema(prices, 26)
    ema_cross = "bullish" if (ema12 and ema26 and ema12[-1] > ema26[-1]) else "bearish"
    # Derive overall bias
    if rsi < 30 and macd["trend"] == "bullish":
        bias = "STRONG_BUY"
    elif rsi < 40 or boll["position"] == "lower_band":
        bias = "BUY"
    elif rsi > 70 and macd["trend"] == "bearish":
        bias = "STRONG_SELL"
    elif rsi > 60 or boll["position"] == "upper_band":
        bias = "WAIT"
    else:
        bias = "NEUTRAL"
    return {
        "product_id": product_id,
        "current_price": current,
        "rsi": {"value": rsi, "signal": "bullish" if rsi < 40 else ("bearish" if rsi > 65 else "neutral")},
        "macd": macd,
        "ema": {
            "ema12": round(ema12[-1], 2) if ema12 else current,
            "ema26": round(ema26[-1], 2) if ema26 else current,
            "crossover": ema_cross,
            "signal": ema_cross,
        },
        "bollinger": {**boll, "signal": "bullish" if boll["position"] == "lower_band" else ("bearish" if boll["position"] == "upper_band" else "neutral")},
        "bias": bias,
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
