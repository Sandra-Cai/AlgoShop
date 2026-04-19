"""
connectors.py — External data source integrations for AlgoShop

Connects to CONNECTED external services via the external-tool CLI:
  • Blockscout  — blockchain transparency / on-chain intelligence
  • GoDaddy     — domain verification & suggestion
  • Trivago     — accommodations / travel pricing

Features:
  • Robust parsing of nested / string-wrapped JSON payloads
  • In-process TTL cache to avoid redundant upstream calls
  • Exponential-backoff retry for transient failures
  • Unified error envelope: callers always get a dict (never raises)
  • Graceful fallback: if an upstream call fails, we return a well-formed
    empty structure so the frontend keeps rendering.
"""

import asyncio
import hashlib
import json
import logging
import time
from datetime import datetime, timedelta
from typing import Any, Optional

logger = logging.getLogger("connectors")

# ─── Simple TTL cache ─────────────────────────────────────────────────────────

_CACHE: dict[str, tuple[Any, float]] = {}
_CACHE_TTL_DEFAULT = 120  # 2 min — most connector data is moderately fresh


def _cache_key(*parts: Any) -> str:
    raw = "::".join(str(p) for p in parts)
    return hashlib.md5(raw.encode()).hexdigest()


def _cache_get(key: str) -> Optional[Any]:
    entry = _CACHE.get(key)
    if not entry:
        return None
    data, expires_at = entry
    if time.time() >= expires_at:
        _CACHE.pop(key, None)
        return None
    return data


def _cache_set(key: str, value: Any, ttl: int = _CACHE_TTL_DEFAULT):
    _CACHE[key] = (value, time.time() + ttl)


def cache_stats() -> dict:
    now = time.time()
    live = sum(1 for _, (_, exp) in _CACHE.items() if exp > now)
    return {"entries": len(_CACHE), "live": live, "expired": len(_CACHE) - live}


def clear_cache():
    _CACHE.clear()


# ─── CLI dispatcher with retry ────────────────────────────────────────────────

async def call_tool(
    source_id: str,
    tool_name: str,
    arguments: dict,
    retries: int = 2,
    timeout: float = 30.0,
) -> dict:
    """Call an external tool via the external-tool CLI.

    Robust wrapper: retries on transient failures, enforces a timeout, and
    always returns a dict. Callers never need try/except — a failed call
    yields ``{"error": "..."}``.
    """
    last_err = "unknown"
    for attempt in range(retries + 1):
        try:
            proc = await asyncio.create_subprocess_exec(
                "external-tool", "call", json.dumps({
                    "source_id": source_id,
                    "tool_name": tool_name,
                    "arguments": arguments,
                }),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            try:
                stdout, stderr = await asyncio.wait_for(
                    proc.communicate(), timeout=timeout
                )
            except asyncio.TimeoutError:
                try:
                    proc.kill()
                except Exception:
                    pass
                last_err = f"timeout after {timeout}s"
                logger.warning(f"[{source_id}/{tool_name}] {last_err} (attempt {attempt+1})")
                continue

            if proc.returncode != 0:
                last_err = stderr.decode()[:300] or "non-zero exit"
                logger.warning(f"[{source_id}/{tool_name}] {last_err}")
                # Don't retry auth errors
                if "auth_required" in last_err or "auth" in last_err.lower():
                    return {"error": last_err, "auth_required": True}
                await asyncio.sleep(0.4 * (attempt + 1))  # backoff
                continue

            raw = stdout.decode()
            try:
                return json.loads(raw)
            except json.JSONDecodeError:
                # Some tools return raw text — wrap it
                return {"raw": raw}

        except FileNotFoundError:
            return {"error": "external-tool CLI not available"}
        except Exception as e:
            last_err = str(e)[:300]
            logger.warning(f"[{source_id}/{tool_name}] unexpected: {last_err}")
            await asyncio.sleep(0.3 * (attempt + 1))

    return {"error": last_err}


def _deep_parse_json(value: Any) -> Any:
    """Recursively parse JSON-encoded strings hiding inside dicts/lists."""
    if isinstance(value, str):
        stripped = value.strip()
        if stripped and stripped[0] in "{[":
            try:
                return _deep_parse_json(json.loads(stripped))
            except (json.JSONDecodeError, ValueError):
                return value
        return value
    if isinstance(value, dict):
        return {k: _deep_parse_json(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_deep_parse_json(v) for v in value]
    return value


# ══════════════════════════════════════════════════════════════════════════════
#  Blockscout — Blockchain Intelligence
# ══════════════════════════════════════════════════════════════════════════════

_blockscout_initialized = False
_blockscout_init_lock = asyncio.Lock()


async def _ensure_blockscout():
    global _blockscout_initialized
    if _blockscout_initialized:
        return
    async with _blockscout_init_lock:
        if _blockscout_initialized:
            return
        try:
            await call_tool("blockscout", "__unlock_blockchain_analysis__", {})
            _blockscout_initialized = True
            logger.info("Blockscout initialized")
        except Exception as e:
            logger.error(f"Blockscout init failed: {e}")
            # Don't re-raise — allow fallback path


def _parse_blockscout_result(result: Any) -> dict:
    """Blockscout responses are often double-encoded. Unwrap aggressively."""
    parsed = _deep_parse_json(result)
    if isinstance(parsed, dict):
        # Common wrappers: {"data": ...}, {"result": ...}, {"output": ...}
        for key in ("data", "result", "output"):
            if key in parsed and isinstance(parsed[key], dict):
                return parsed[key]
        return parsed
    return {}


async def get_network_stats(chain_id: str = "1") -> dict:
    """Get real-time network stats from Blockscout (cached 60s)."""
    ck = _cache_key("bs_stats", chain_id)
    cached = _cache_get(ck)
    if cached:
        return cached

    await _ensure_blockscout()
    try:
        result = await call_tool("blockscout", "direct_api_call", {
            "chain_id": chain_id,
            "endpoint_path": "/api/v2/stats",
            "query_params": {},
        })
        data = _parse_blockscout_result(result)
        payload = {
            "source": "blockscout",
            "chain": "Ethereum Mainnet" if chain_id == "1" else f"Chain {chain_id}",
            "chain_id": chain_id,
            "data": data,
            "fetched_at": datetime.utcnow().isoformat() + "Z",
        }
        _cache_set(ck, payload, ttl=60)
        return payload
    except Exception as e:
        logger.error(f"Network stats error: {e}")
        return {"source": "blockscout", "chain_id": chain_id, "data": {}, "error": str(e)}


async def lookup_token(symbol: str, chain_id: str = "1") -> dict:
    """Look up a token by symbol on Blockscout (cached 10 min)."""
    ck = _cache_key("bs_token", chain_id, symbol.lower())
    cached = _cache_get(ck)
    if cached:
        return cached

    await _ensure_blockscout()
    try:
        result = await call_tool("blockscout", "lookup_token_by_symbol", {
            "chain_id": chain_id,
            "symbol": symbol,
        })
        data = _deep_parse_json(result)
        payload = {
            "source": "blockscout",
            "symbol": symbol.upper(),
            "chain_id": chain_id,
            "data": data,
            "fetched_at": datetime.utcnow().isoformat() + "Z",
        }
        _cache_set(ck, payload, ttl=600)
        return payload
    except Exception as e:
        logger.error(f"Token lookup error: {e}")
        return {"source": "blockscout", "symbol": symbol, "data": {}, "error": str(e)}


async def get_address_info(address: str, chain_id: str = "1") -> dict:
    """Get address info from Blockscout (cached 30s — balances change fast)."""
    ck = _cache_key("bs_addr", chain_id, address.lower())
    cached = _cache_get(ck)
    if cached:
        return cached

    await _ensure_blockscout()
    try:
        result = await call_tool("blockscout", "get_address_info", {
            "chain_id": chain_id,
            "address": address,
        })
        data = _deep_parse_json(result)
        payload = {
            "source": "blockscout",
            "address": address,
            "chain_id": chain_id,
            "data": data,
            "fetched_at": datetime.utcnow().isoformat() + "Z",
        }
        _cache_set(ck, payload, ttl=30)
        return payload
    except Exception as e:
        logger.error(f"Address info error: {e}")
        return {"source": "blockscout", "address": address, "data": {}, "error": str(e)}


async def get_blockchain_overview() -> dict:
    """Comprehensive Ethereum overview: stats + counters + summary metrics.

    Used by the frontend Intel tab. Fetches two endpoints in parallel and
    extracts the most useful top-line metrics into a compact summary.
    """
    ck = _cache_key("bs_overview")
    cached = _cache_get(ck)
    if cached:
        return cached

    await _ensure_blockscout()
    try:
        stats_task = call_tool("blockscout", "direct_api_call", {
            "chain_id": "1",
            "endpoint_path": "/api/v2/stats",
            "query_params": {},
        })
        counters_task = call_tool("blockscout", "direct_api_call", {
            "chain_id": "1",
            "endpoint_path": "/stats-service/api/v1/counters",
            "query_params": {},
        })
        stats_result, counters_result = await asyncio.gather(
            stats_task, counters_task, return_exceptions=True
        )
        stats_data = {} if isinstance(stats_result, Exception) else _parse_blockscout_result(stats_result)
        counters_data = {} if isinstance(counters_result, Exception) else _parse_blockscout_result(counters_result)

        # Build a clean top-line summary for easy consumption
        summary = _build_blockchain_summary(stats_data, counters_data)

        payload = {
            "source": "blockscout",
            "network_stats": stats_data,
            "counters": counters_data,
            "summary": summary,
            "fetched_at": datetime.utcnow().isoformat() + "Z",
        }
        _cache_set(ck, payload, ttl=60)
        return payload
    except Exception as e:
        logger.error(f"Blockchain overview error: {e}")
        return {
            "source": "blockscout", "network_stats": {}, "counters": {},
            "summary": {}, "error": str(e),
        }


def _build_blockchain_summary(stats: dict, counters: dict) -> dict:
    """Build a compact, frontend-friendly summary of Ethereum state."""
    summary: dict = {}

    def _num(v):
        if v is None:
            return None
        try:
            return float(str(v).replace(",", ""))
        except (ValueError, TypeError):
            return None

    # Common stat fields across Blockscout versions
    for src_key, dst_key in [
        ("total_blocks", "total_blocks"),
        ("total_transactions", "total_transactions"),
        ("total_addresses", "total_addresses"),
        ("average_block_time", "avg_block_time_ms"),
        ("coin_price", "eth_price_usd"),
        ("coin_price_change_percentage", "eth_price_change_pct"),
        ("market_cap", "market_cap_usd"),
        ("gas_prices_update_in", "gas_update_in_ms"),
        ("tvl", "tvl_usd"),
        ("static_gas_price", "gas_gwei"),
        ("network_utilization_percentage", "network_utilization_pct"),
    ]:
        if src_key in stats:
            summary[dst_key] = _num(stats[src_key]) if src_key != "coin_price" else stats[src_key]

    # Gas prices (nested)
    gas = stats.get("gas_prices") or {}
    if isinstance(gas, dict):
        for k in ("slow", "average", "fast"):
            if k in gas and isinstance(gas[k], dict):
                summary[f"gas_{k}_gwei"] = gas[k].get("price") or gas[k].get("fiat_price")
            elif k in gas:
                summary[f"gas_{k}_gwei"] = gas[k]

    # Counters are typically a list of {id, value}
    if isinstance(counters, list):
        for c in counters:
            if isinstance(c, dict) and "id" in c and "value" in c:
                summary[f"counter_{c['id']}"] = _num(c["value"])
    elif isinstance(counters, dict):
        cnt_list = counters.get("counters", [])
        if isinstance(cnt_list, list):
            for c in cnt_list:
                if isinstance(c, dict) and "id" in c and "value" in c:
                    summary[f"counter_{c['id']}"] = _num(c["value"])

    return summary


# ══════════════════════════════════════════════════════════════════════════════
#  GoDaddy — Domain Verification
# ══════════════════════════════════════════════════════════════════════════════

def _parse_godaddy_result(result: Any) -> list:
    """Extract a list of {domain, available, ...} records from GoDaddy output.
    
    Handles both structured JSON and text-format responses from the connector.
    """
    # Handle text-format responses (GoDaddy connector returns markdown text)
    if isinstance(result, str) and ('UNAVAILABLE' in result or 'AVAILABLE' in result or 'SUGGESTIONS' in result):
        import re
        records = []
        # Parse unavailable domains
        for m in re.finditer(r'\u2022\s+([\w.-]+\.\w+)', result):
            domain = m.group(1)
            available = 'AVAILABLE' in result.split(domain)[0].split('\n')[-1] if domain in result else False
            # Check context: is this in an AVAILABLE section?
            before = result[:result.index(domain)]
            is_available = '\u2705' in before.split('\n')[-3:] or 'STANDARD SUGGESTIONS' in before[-200:] or 'AVAILABLE' in before[-200:]
            is_unavailable = '\u274c' in before.split('\n')[-3:] or 'UNAVAILABLE' in before[-200:]
            records.append({
                'domain': domain,
                'available': is_available and not is_unavailable,
            })
        if records:
            return records
        # If we couldn't parse specific domains, create synthetic records from the summary
        total_match = re.search(r'Total domains checked:\s*(\d+)', result)
        unavail_match = re.search(r'Unavailable domains:\s*(\d+)', result)
        if total_match:
            return [{'domain': 'verified', 'available': False, 'total_checked': int(total_match.group(1)),
                     'unavailable': int(unavail_match.group(1)) if unavail_match else 0}]
    
    parsed = _deep_parse_json(result)
    if isinstance(parsed, list):
        return parsed
    if isinstance(parsed, dict):
        for key in ("domains", "data", "result", "output"):
            val = parsed.get(key)
            if isinstance(val, list):
                return val
            if isinstance(val, dict) and isinstance(val.get("domains"), list):
                return val["domains"]
        # Single domain record
        if "domain" in parsed:
            return [parsed]
    return []


async def check_domain_availability(domains: str) -> dict:
    """Check domain availability via GoDaddy.

    `domains` is a comma-separated string. Returns a normalized list.
    Cached 5 min.
    """
    ck = _cache_key("gd_check", domains.lower())
    cached = _cache_get(ck)
    if cached:
        return cached

    try:
        result = await call_tool("godaddy", "domains_check_availability", {
            "domains": domains,
        })
        records = _parse_godaddy_result(result)
        normalized = []
        for r in records:
            if not isinstance(r, dict):
                continue
            normalized.append({
                "domain": r.get("domain", ""),
                "available": bool(r.get("available", False)),
                "price": r.get("price"),
                "currency": r.get("currency", "USD"),
                "period": r.get("period"),
                "definitive": r.get("definitive", True),
            })
        payload = {
            "source": "godaddy",
            "domains_queried": [d.strip() for d in domains.split(",") if d.strip()],
            "results": normalized,
            "count": len(normalized),
            "available_count": sum(1 for r in normalized if r["available"]),
            "raw": records if not normalized else None,
            "fetched_at": datetime.utcnow().isoformat() + "Z",
        }
        _cache_set(ck, payload, ttl=300)
        return payload
    except Exception as e:
        logger.error(f"Domain check error: {e}")
        return {"source": "godaddy", "results": [], "error": str(e)}


async def suggest_domains(query: str, limit: int = 10) -> dict:
    """Get domain suggestions from GoDaddy (cached 10 min)."""
    ck = _cache_key("gd_suggest", query.lower(), limit)
    cached = _cache_get(ck)
    if cached:
        return cached

    try:
        result = await call_tool("godaddy", "domains_suggest", {
            "query": query,
            "limit": limit,
        })
        records = _parse_godaddy_result(result)
        suggestions = []
        for r in records:
            if not isinstance(r, dict):
                continue
            suggestions.append({
                "domain": r.get("domain", ""),
                "score": r.get("score"),
                "tld": r.get("domain", "").split(".")[-1] if "." in r.get("domain", "") else None,
            })
        payload = {
            "source": "godaddy",
            "query": query,
            "suggestions": suggestions,
            "count": len(suggestions),
            "fetched_at": datetime.utcnow().isoformat() + "Z",
        }
        _cache_set(ck, payload, ttl=600)
        return payload
    except Exception as e:
        logger.error(f"Domain suggest error: {e}")
        return {"source": "godaddy", "query": query, "suggestions": [], "error": str(e)}


async def verify_store_domains(store_names: list) -> dict:
    """Verify primary .com domains for a list of store names.

    Returns a dict (not a list — the original return type was inconsistent).
    """
    # Build and normalize domain list
    cleaned = []
    for name in store_names:
        slug = "".join(c for c in name.lower() if c.isalnum())
        if slug:
            cleaned.append(f"{slug}.com")

    domains = ", ".join(cleaned)
    ck = _cache_key("gd_verify", domains)
    cached = _cache_get(ck)
    if cached:
        return cached

    try:
        result = await call_tool("godaddy", "domains_check_availability", {
            "domains": domains,
        })
        records = _parse_godaddy_result(result)

        # Map back to store names
        domain_status = {
            r.get("domain", "").lower(): r
            for r in records if isinstance(r, dict)
        }

        stores = []
        for name, slug_dom in zip(store_names, cleaned):
            rec = domain_status.get(slug_dom.lower(), {})
            stores.append({
                "store": name,
                "domain": slug_dom,
                "registered": (not bool(rec.get("available", True))),
                "available_for_registration": bool(rec.get("available", False)),
                "price": rec.get("price"),
            })

        payload = {
            "source": "godaddy",
            "stores": stores,
            "count": len(stores),
            "registered_count": sum(1 for s in stores if s["registered"]),
            "fetched_at": datetime.utcnow().isoformat() + "Z",
        }
        _cache_set(ck, payload, ttl=3600)  # 1h — store domains are stable
        return payload
    except Exception as e:
        logger.error(f"Store domain verify error: {e}")
        return {"source": "godaddy", "stores": [], "error": str(e)}


# ══════════════════════════════════════════════════════════════════════════════
#  Trivago — Accommodation Search
# ══════════════════════════════════════════════════════════════════════════════

def _parse_trivago_result(raw: Any) -> list:
    """Trivago results arrive as a JSON-wrapped string. Unwrap aggressively."""
    # Case 1: direct string
    if isinstance(raw, str):
        # Find the first JSON object/array in the string
        for opener in ("{", "["):
            idx = raw.find(opener)
            if idx < 0:
                continue
            try:
                parsed = json.loads(raw[idx:])
                return _parse_trivago_result(parsed)
            except json.JSONDecodeError:
                continue
        return []

    # Case 2: dict wrapper
    if isinstance(raw, dict):
        for key in ("output", "data", "result", "hotels", "accommodations"):
            val = raw.get(key)
            if val is None:
                continue
            if isinstance(val, list):
                return val
            if isinstance(val, str):
                try:
                    inner = json.loads(val)
                    if isinstance(inner, list):
                        return inner
                    if isinstance(inner, dict):
                        return _parse_trivago_result(inner)
                except json.JSONDecodeError:
                    continue
        # The dict itself might be a single hotel
        if any(k in raw for k in ("Accommodation Name", "name")):
            return [raw]
        return []

    # Case 3: list already
    if isinstance(raw, list):
        return raw

    return []


def _normalize_hotel(h: dict) -> dict:
    """Normalize a Trivago hotel dict — tolerant of multiple key formats."""
    def g(*keys, default=""):
        for k in keys:
            v = h.get(k)
            if v is not None and v != "":
                return v
        return default

    def to_float(v):
        if v is None or v == "":
            return None
        try:
            # Strip currency symbols: $, €, £, ¥, etc.
            cleaned = str(v).replace(",", "").replace("$", "").replace("€", "").replace("£", "").replace("¥", "").strip()
            # Remove any remaining non-numeric prefixes
            import re
            cleaned = re.sub(r'^[^\d.]+', '', cleaned)
            return float(cleaned) if cleaned else None
        except (ValueError, TypeError):
            return None

    price_raw = g("Price Per Night", "price_per_night", "price", "Price")
    rating_raw = g("Review Rating", "rating", "review_rating")
    stars_raw = g("Hotel Rating", "stars", "hotel_rating")

    return {
        "name": g("Accommodation Name", "name", "accommodation_name", default="Unknown"),
        "address": g("Address", "address"),
        "city": g("Country City", "city", "country_city"),
        "lat": to_float(g("Latitude", "lat", "latitude")),
        "lng": to_float(g("Longitude", "lng", "longitude")),
        "rating": to_float(rating_raw) or rating_raw or None,
        "hotel_stars": to_float(stars_raw) or stars_raw or None,
        "reviews": g("Review Count", "reviews", "review_count", default="0"),
        "price": to_float(price_raw),
        "price_display": price_raw if price_raw else "N/A",
        "distance": g("Distance", "distance"),
        "image": g("Main Image", "image", "main_image"),
        "url": g("Accommodation URL", "url", "accommodation_url"),
        "amenities": g("Top Amenities", "amenities", "top_amenities"),
    }


async def search_accommodations(
    latitude: float,
    longitude: float,
    radius: int = 3000,
    arrival: Optional[str] = None,
    departure: Optional[str] = None,
    adults: int = 1,
    rooms: int = 1,
    limit: int = 15,
) -> dict:
    """Search for accommodations near a location via Trivago.

    Returns a normalized dict with hotels sorted by price. Cached for 15 min.
    """
    if not arrival:
        arrival = (datetime.now() + timedelta(days=3)).strftime("%Y-%m-%d")
    if not departure:
        try:
            arr_date = datetime.strptime(arrival, "%Y-%m-%d")
        except ValueError:
            arr_date = datetime.now() + timedelta(days=3)
        departure = (arr_date + timedelta(days=1)).strftime("%Y-%m-%d")

    ck = _cache_key("tv_hotels", round(latitude, 3), round(longitude, 3),
                    radius, arrival, departure, adults, rooms)
    cached = _cache_get(ck)
    if cached:
        return cached

    try:
        result = await call_tool("trivago", "trivago-accommodation-radius-search", {
            "latitude": latitude,
            "longitude": longitude,
            "radius": radius,
            "arrival": arrival,
            "departure": departure,
            "adults": adults,
            "rooms": rooms,
        }, timeout=45.0)

        hotels_raw = _parse_trivago_result(result)
        normalized = [_normalize_hotel(h) for h in hotels_raw if isinstance(h, dict)]

        # Sort by price (None prices last)
        normalized.sort(key=lambda h: (h["price"] is None, h["price"] or 9e9))

        # Compute summary stats
        priced = [h["price"] for h in normalized if h["price"] is not None]
        summary = {
            "total_hotels": len(normalized),
            "priced_hotels": len(priced),
            "min_price": min(priced) if priced else None,
            "max_price": max(priced) if priced else None,
            "avg_price": round(sum(priced) / len(priced), 2) if priced else None,
            "median_price": sorted(priced)[len(priced) // 2] if priced else None,
        }

        payload = {
            "source": "trivago",
            "location": {"lat": latitude, "lng": longitude, "radius_m": radius},
            "arrival": arrival,
            "departure": departure,
            "adults": adults,
            "rooms": rooms,
            "hotels": normalized[:limit],
            "total": len(normalized),
            "summary": summary,
            "fetched_at": datetime.utcnow().isoformat() + "Z",
        }
        _cache_set(ck, payload, ttl=900)  # 15 min
        return payload
    except Exception as e:
        logger.error(f"Accommodation search error: {e}")
        return {
            "source": "trivago",
            "location": {"lat": latitude, "lng": longitude, "radius_m": radius},
            "arrival": arrival, "departure": departure,
            "hotels": [], "total": 0, "error": str(e),
        }


# ══════════════════════════════════════════════════════════════════════════════
#  Combined: Multi-source intelligence
# ══════════════════════════════════════════════════════════════════════════════

# Expanded NYC zip → coords (kept to NY because Trivago is most useful there for demo)
ZIP_COORDS = {
    "10001": (40.7484, -73.9967),  # Midtown
    "10002": (40.7157, -73.9863),  # Lower East Side
    "10003": (40.7317, -73.9893),  # East Village
    "10010": (40.7390, -73.9826),  # Gramercy
    "10011": (40.7418, -74.0002),  # Chelsea
    "10012": (40.7258, -73.9981),  # SoHo / NoHo
    "10013": (40.7200, -74.0048),  # Tribeca
    "10014": (40.7340, -74.0054),  # West Village
    "10016": (40.7459, -73.9777),  # Murray Hill / Kips Bay
    "10017": (40.7522, -73.9727),  # Midtown East
    "10018": (40.7549, -73.9926),  # Garment District
    "10019": (40.7653, -73.9845),  # Midtown West / Hell's Kitchen
    "10021": (40.7694, -73.9584),  # Upper East Side
    "10022": (40.7587, -73.9687),  # Midtown East
    "10023": (40.7764, -73.9826),  # Upper West Side
    "10024": (40.7870, -73.9754),  # Upper West Side
    "10025": (40.7988, -73.9681),  # Upper West Side / Morningside
    "10028": (40.7762, -73.9532),  # Upper East Side / Yorkville
    "10036": (40.7592, -73.9889),  # Times Square
    "10065": (40.7648, -73.9620),  # Upper East Side
    "10128": (40.7812, -73.9530),  # Upper East Side
    "11201": (40.6955, -73.9897),  # Brooklyn Heights / DUMBO
    "11211": (40.7081, -73.9571),  # Williamsburg
    "11222": (40.7278, -73.9501),  # Greenpoint
    "11215": (40.6682, -73.9820),  # Park Slope
}


async def get_shopping_intelligence(zip_code: str = "10012") -> dict:
    """Get combined multi-source intelligence for a location.

    Runs blockchain + accommodations queries in parallel so the page loads fast.
    """
    lat, lng = ZIP_COORDS.get(zip_code, (40.7258, -73.9981))

    blockchain_task = get_blockchain_overview()
    hotels_task = search_accommodations(lat, lng, radius=2000)

    blockchain_data, hotels_data = await asyncio.gather(
        blockchain_task, hotels_task, return_exceptions=True
    )
    if isinstance(blockchain_data, Exception):
        blockchain_data = {"error": str(blockchain_data)}
    if isinstance(hotels_data, Exception):
        hotels_data = {"error": str(hotels_data)}

    return {
        "zip_code": zip_code,
        "coordinates": {"lat": lat, "lng": lng},
        "blockchain": blockchain_data,
        "accommodations": hotels_data,
        "fetched_at": datetime.utcnow().isoformat() + "Z",
    }


# ─── Diagnostics ──────────────────────────────────────────────────────────────

def connector_status() -> dict:
    """Reports which connectors are wired up and basic cache health."""
    return {
        "connectors": {
            "blockscout": {
                "initialized": _blockscout_initialized,
                "endpoints": ["overview", "network_stats", "lookup_token", "address_info"],
            },
            "godaddy": {
                "endpoints": ["check", "suggest", "verify_stores"],
            },
            "trivago": {
                "endpoints": ["search_accommodations"],
            },
        },
        "cache": cache_stats(),
        "supported_zip_codes": list(ZIP_COORDS.keys()),
    }
