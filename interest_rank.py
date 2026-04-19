"""
InterestRank v4.0 — PageRank + HITS + Eigenvector Centrality + Adjacency Matrix
=================================================================================
Four-signal hybrid ranking with explicit adjacency matrix analysis and
user-adjustable strategy weights.

Algorithms:
  1. PageRank (Brin & Page 1998) — random-walk interest propagation
  2. HITS (Kleinberg 1999) — hub/authority decomposition
  3. Eigenvector Centrality (Bonacich 1972) — influence score via dominant
     eigenvector of weighted adjacency matrix (Perron-Frobenius theorem)
  4. Adjacency Matrix Analysis — explicit A^n walk counting for multi-hop
     product discovery, spectral gap for recommendation confidence, and
     triangle density for product cluster strength

Adjacency Matrix Features:
  - Walk counting: A^n[i][j] = number of walks of length n from product i to j
    → discovers non-obvious product connections through multi-hop paths
  - Spectral gap: λ₁ - λ₂ of adjacency matrix → measures graph connectivity
    → higher gap = more confidence in recommendations (well-connected graph)
  - Triangle counting: trace(A³)/6 → cluster density metric
    → products in tight triangles are strongly related
  - Degree centrality from row/column sums → fast popularity measure

User Strategy Weights:
  Users adjust HOW products are ranked based on personal priorities:
  - price_sensitivity  → cheap is better (boosts low-price products)
  - quality_focus      → reviews & brand reputation matter most
  - sustainability     → prefer secondhand/resale options
  - trend_following    → follow what's hot / momentum picks
  - convenience        → proximity, fast shipping, availability

Final score = α·PageRank + β·Authority + γ·Hub + δ·EigenCentrality + ε·AdjWalk
  (α,β,γ,δ,ε adjusted by user strategy → different users get different rankings)

References:
  - Brin & Page (1998) — PageRank
  - Kleinberg (1999) — HITS / Hubs & Authorities
  - Bonacich (1972/1987) — Eigenvector centrality
  - https://en.wikipedia.org/wiki/Adjacency_matrix
  - https://en.wikipedia.org/wiki/Eigenvector_centrality
  - https://en.wikipedia.org/wiki/HITS_algorithm
"""

import math
import time
import logging
from collections import defaultdict

log = logging.getLogger("interest_rank")

# ─── Algorithm Constants ─────────────────────────────────────────────────────

DAMPING_FACTOR = 0.85
HITS_ITERATIONS = 60             # max — early-exits on convergence
PAGERANK_ITERATIONS = 100        # max — early-exits on convergence
EIGEN_ITERATIONS = 80            # max — early-exits on convergence
ADJ_WALK_DEPTH = 3               # A^n walk depth (length of multi-hop paths)
EPSILON = 1e-8                   # tighter convergence tolerance
EPSILON_NORM = 1e-10             # anti-divide-by-zero guard
TEMPORAL_DECAY_HALF_LIFE = 300   # 5 min
CACHE_TTL = 30                   # score cache TTL (seconds)

# Default fusion weights (before user strategy adjustment)
DEFAULT_ALPHA   = 0.25   # PageRank
DEFAULT_BETA    = 0.25   # HITS Authority
DEFAULT_GAMMA   = 0.08   # HITS Hub
DEFAULT_DELTA   = 0.25   # Eigenvector Centrality
DEFAULT_EPSILON = 0.17   # Adjacency Matrix Walk Score

# Edge type weights
EDGE_WEIGHTS = {
    "category": 1.0, "brand": 0.8, "price_band": 0.5,
    "co_view": 1.5, "search": 3.0, "click": 5.0,
    "cart": 8.0, "purchase": 10.0,
}

PRICE_BANDS = [
    (0, 50, "budget"), (50, 150, "mid"), (150, 400, "premium"),
    (400, 1000, "luxury"), (1000, float("inf"), "ultra-luxury"),
]

# ─── User Strategy Presets ────────────────────────────────────────────────────

STRATEGY_PRESETS = {
    "balanced": {
        "description": "Equal weight across all factors",
        "price_sensitivity": 0.5, "quality_focus": 0.5,
        "sustainability": 0.5, "trend_following": 0.5, "convenience": 0.5,
    },
    "price_hunter": {
        "description": "Cheapest price wins — maximize savings",
        "price_sensitivity": 1.0, "quality_focus": 0.2,
        "sustainability": 0.3, "trend_following": 0.1, "convenience": 0.3,
    },
    "quality_first": {
        "description": "Best brands and reviews — pay more for quality",
        "price_sensitivity": 0.2, "quality_focus": 1.0,
        "sustainability": 0.4, "trend_following": 0.3, "convenience": 0.4,
    },
    "eco_conscious": {
        "description": "Prefer secondhand, resale, and sustainable options",
        "price_sensitivity": 0.5, "quality_focus": 0.4,
        "sustainability": 1.0, "trend_following": 0.2, "convenience": 0.3,
    },
    "trend_setter": {
        "description": "What's hot and popular — follow the momentum",
        "price_sensitivity": 0.3, "quality_focus": 0.5,
        "sustainability": 0.2, "trend_following": 1.0, "convenience": 0.4,
    },
    "convenience_max": {
        "description": "Fast shipping, nearby stores, instant availability",
        "price_sensitivity": 0.3, "quality_focus": 0.4,
        "sustainability": 0.2, "trend_following": 0.3, "convenience": 1.0,
    },
}


def get_price_band(price: float) -> str:
    for lo, hi, label in PRICE_BANDS:
        if lo <= price < hi:
            return label
    return "ultra-luxury"


# ─── Product Graph ────────────────────────────────────────────────────────────

class ProductGraph:
    def __init__(self):
        self.adj: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))
        self.nodes: set[str] = set()
        self.node_meta: dict[str, dict] = {}

    def add_node(self, pid: str, meta: dict = None):
        self.nodes.add(pid)
        if meta:
            self.node_meta[pid] = meta

    def add_edge(self, src: str, dst: str, weight: float, edge_type: str = ""):
        self.nodes.add(src)
        self.nodes.add(dst)
        self.adj[src][dst] += weight

    def add_bidirectional(self, a: str, b: str, weight: float, edge_type: str = ""):
        self.add_edge(a, b, weight, edge_type)
        self.add_edge(b, a, weight, edge_type)

    def out_weight(self, node: str) -> float:
        return sum(self.adj[node].values()) if node in self.adj else 0.0

    def neighbors(self, node: str) -> dict[str, float]:
        return dict(self.adj.get(node, {}))

    def incoming(self, node: str) -> dict[str, float]:
        result = {}
        for src in self.adj:
            if node in self.adj[src]:
                result[src] = self.adj[src][node]
        return result


# ─── Adjacency Matrix Builder ────────────────────────────────────────────────

class AdjacencyMatrix:
    """
    Explicit adjacency matrix representation for spectral analysis.
    
    A[i][j] = weight of edge from node i to node j.
    For undirected edges, A is symmetric.
    
    Key operations:
      - A^n: walk counting (number of walks of length n between nodes)
      - trace(A^3)/6: triangle count (cluster density)
      - eigenvalues: spectral gap = λ₁ - λ₂ (connectivity confidence)
      - row sums: out-degree centrality
      - column sums: in-degree centrality
    
    Reference: https://en.wikipedia.org/wiki/Adjacency_matrix
    """
    
    def __init__(self, graph: ProductGraph, product_ids: list[str]):
        self.node_list = list(product_ids)
        self.n = len(self.node_list)
        self.node_idx = {pid: i for i, pid in enumerate(self.node_list)}
        
        # Build dense matrix
        self.matrix = [[0.0] * self.n for _ in range(self.n)]
        for src in self.node_list:
            i = self.node_idx[src]
            for dst, w in graph.adj.get(src, {}).items():
                if dst in self.node_idx:
                    j = self.node_idx[dst]
                    self.matrix[i][j] = w
    
    def _mat_mul(self, A: list[list[float]], B: list[list[float]]) -> list[list[float]]:
        """Matrix multiplication A × B."""
        n = len(A)
        C = [[0.0] * n for _ in range(n)]
        for i in range(n):
            for k in range(n):
                if A[i][k] == 0:
                    continue
                for j in range(n):
                    C[i][j] += A[i][k] * B[k][j]
        return C
    
    def power(self, n: int) -> list[list[float]]:
        """
        Compute A^n via repeated squaring.
        
        (A^n)[i][j] = number of walks of length n from i to j.
        This is the fundamental property of adjacency matrices for
        discovering multi-hop product connections.
        """
        if n == 0:
            # Identity matrix
            I = [[0.0] * self.n for _ in range(self.n)]
            for i in range(self.n):
                I[i][i] = 1.0
            return I
        if n == 1:
            return [row[:] for row in self.matrix]
        
        # Repeated squaring for efficiency
        if n % 2 == 0:
            half = self.power(n // 2)
            return self._mat_mul(half, half)
        else:
            return self._mat_mul(self.matrix, self.power(n - 1))
    
    def walk_scores(self, depth: int = ADJ_WALK_DEPTH) -> dict[str, float]:
        """
        Compute walk-based centrality: sum of walks of all lengths 1..depth
        reaching each node, normalized.
        
        walk_score[v] = Σ_{k=1}^{depth} Σ_u A^k[u][v] / k
        
        Discounting by 1/k ensures shorter paths have more influence
        (similar to Katz centrality). Products reachable by many short
        paths score higher — they're well-connected hubs.
        """
        if self.n == 0:
            return {}
        
        scores = {pid: 0.0 for pid in self.node_list}
        current = [row[:] for row in self.matrix]  # A^1
        
        for k in range(1, depth + 1):
            if k > 1:
                current = self._mat_mul(current, self.matrix)
            
            discount = 1.0 / k  # Shorter walks weigh more
            for j in range(self.n):
                col_sum = sum(current[i][j] for i in range(self.n))
                scores[self.node_list[j]] += col_sum * discount
        
        # Normalize to 0-1
        mx = max(scores.values()) if scores else 1.0
        if mx > 0:
            scores = {pid: v / mx for pid, v in scores.items()}
        return scores
    
    def triangle_count(self) -> int:
        """
        Count triangles in the graph: trace(A³) / 6
        
        For undirected: divide by 6 (3 starting vertices × 2 directions)
        Each triangle represents a tight product cluster.
        """
        if self.n == 0:
            return 0
        A3 = self.power(3)
        trace = sum(A3[i][i] for i in range(self.n))
        return max(0, int(trace / 6))
    
    def triangle_density(self) -> float:
        """Fraction of possible triangles that exist (clustering coefficient)."""
        if self.n < 3:
            return 0.0
        t = self.triangle_count()
        max_triangles = self.n * (self.n - 1) * (self.n - 2) / 6
        return t / max_triangles if max_triangles > 0 else 0.0
    
    def spectral_gap(self) -> float:
        """
        Estimate spectral gap λ₁ - λ₂ via power iteration.
        
        Higher spectral gap → graph is well-connected (expander-like)
        → more confidence in recommendations.
        Lower gap → disconnected clusters → recommendations may be biased.
        """
        if self.n < 2:
            return 0.0
        
        # Find λ₁ via power iteration
        x = [1.0 / self.n] * self.n
        lambda1 = 0.0
        for _ in range(30):
            new_x = [0.0] * self.n
            for i in range(self.n):
                for j in range(self.n):
                    new_x[i] += self.matrix[i][j] * x[j]
            norm = math.sqrt(sum(v*v for v in new_x)) or 1.0
            lambda1 = norm
            x = [v / norm for v in new_x]
        
        # Find λ₂ via deflated power iteration (subtract λ₁ component)
        v1 = x[:]
        y = [1.0 / self.n] * self.n
        # Make orthogonal to v1
        dot = sum(y[i] * v1[i] for i in range(self.n))
        y = [y[i] - dot * v1[i] for i in range(self.n)]
        norm = math.sqrt(sum(v*v for v in y)) or 1.0
        y = [v / norm for v in y]
        
        lambda2 = 0.0
        for _ in range(30):
            new_y = [0.0] * self.n
            for i in range(self.n):
                for j in range(self.n):
                    new_y[i] += self.matrix[i][j] * y[j]
            # Re-orthogonalize against v1
            dot = sum(new_y[i] * v1[i] for i in range(self.n))
            new_y = [new_y[i] - dot * v1[i] for i in range(self.n)]
            norm = math.sqrt(sum(v*v for v in new_y)) or 1.0
            lambda2 = norm
            y = [v / norm for v in new_y]
        
        return max(0.0, lambda1 - lambda2)
    
    def degree_centrality(self) -> dict[str, float]:
        """Row sums of adjacency matrix → out-degree centrality."""
        if self.n == 0:
            return {}
        degrees = {}
        max_d = 0.0
        for i, pid in enumerate(self.node_list):
            d = sum(self.matrix[i])
            degrees[pid] = d
            max_d = max(max_d, d)
        if max_d > 0:
            degrees = {pid: v / max_d for pid, v in degrees.items()}
        return degrees
    
    def get_diagnostics(self) -> dict:
        """Full spectral diagnostics for the product graph."""
        return {
            "matrix_size": f"{self.n}×{self.n}",
            "total_edges": sum(1 for i in range(self.n) for j in range(self.n) if self.matrix[i][j] > 0),
            "triangle_count": self.triangle_count(),
            "triangle_density": round(self.triangle_density(), 4),
            "spectral_gap": round(self.spectral_gap(), 4),
            "walk_depth": ADJ_WALK_DEPTH,
        }


# ─── Algorithm Implementations ───────────────────────────────────────────────

def compute_pagerank(graph: ProductGraph, teleport: dict = None,
                     damping: float = DAMPING_FACTOR,
                     max_iter: int = PAGERANK_ITERATIONS) -> dict[str, float]:
    """Weighted PageRank with teleport (personalized) vector + dangling-node handling.

    Follows the canonical formulation:
        PR(p) = (1-d) * teleport(p) + d * ( Σ_q∈incoming(p) PR(q)·w(q→p)/out(q)
                                           + dangling_mass * teleport(p) )

    Where `dangling_mass` is the total rank held by dangling nodes (no out-edges),
    redistributed proportional to the teleport vector — this guarantees total
    mass conservation and makes scores sum to 1.0 (a proper probability).
    """
    nodes = list(graph.nodes)
    N = len(nodes)
    if N == 0:
        return {}
    if teleport is None:
        teleport = {n: 1.0 / N for n in nodes}
    # Normalize teleport vector to sum to 1.0 (defensive)
    t_sum = sum(teleport.values()) or 1.0
    teleport = {n: teleport.get(n, 0.0) / t_sum for n in nodes}

    scores = {n: 1.0 / N for n in nodes}
    incoming_cache = {n: graph.incoming(n) for n in nodes}
    out_weight_cache = {n: graph.out_weight(n) for n in nodes}
    dangling_nodes = [n for n in nodes if out_weight_cache[n] <= 0]

    for iteration in range(max_iter):
        # Dangling mass: rank held by nodes with no out-edges, redistributed via teleport.
        dangling_mass = sum(scores[n] for n in dangling_nodes)

        new_scores = {}
        for p in nodes:
            rank_sum = 0.0
            for q, edge_w in incoming_cache[p].items():
                out_w = out_weight_cache.get(q, 0.0)
                if out_w > 0:
                    rank_sum += scores.get(q, 0.0) * edge_w / out_w
            tele = teleport.get(p, 1.0 / N)
            new_scores[p] = (1 - damping) * tele + damping * (rank_sum + dangling_mass * tele)

        # L1 convergence check
        delta = sum(abs(new_scores[n] - scores[n]) for n in nodes)
        scores = new_scores
        if delta < EPSILON:
            log.debug(f"PageRank converged at iter {iteration+1} (Δ={delta:.2e})")
            break

    # Final normalization to guarantee Σscores = 1.0
    total = sum(scores.values()) or 1.0
    return {n: v / total for n, v in scores.items()}


def compute_hits(graph: ProductGraph, k: int = HITS_ITERATIONS) -> tuple[dict, dict]:
    """HITS (Kleinberg 1999) — Authority & Hub scores via mutual reinforcement.

    Authority(p) ∝ Σ_q→p  Hub(q)·w(q→p)       (pointed to by good hubs)
    Hub(p)       ∝ Σ_p→r  Authority(r)·w(p→r) (points to good authorities)

    Iterates alternately and L2-normalizes each round. Exits early on
    convergence. Initial values use 1/sqrt(N) so scores live on the unit
    sphere from iteration 0 (faster convergence).
    """
    nodes = list(graph.nodes)
    if not nodes:
        return {}, {}

    N = len(nodes)
    init = 1.0 / math.sqrt(N)
    auth = {n: init for n in nodes}
    hub = {n: init for n in nodes}
    incoming_cache = {n: graph.incoming(n) for n in nodes}
    neighbors_cache = {n: graph.neighbors(n) for n in nodes}

    for step in range(k):
        # Authority update: authority = hubs pointing to me
        new_auth = {}
        for p in nodes:
            s = 0.0
            for q, w in incoming_cache[p].items():
                s += hub.get(q, 0.0) * w
            new_auth[p] = s
        norm = math.sqrt(sum(v * v for v in new_auth.values()))
        if norm < EPSILON_NORM:
            norm = 1.0
        new_auth = {n: v / norm for n, v in new_auth.items()}

        # Hub update: hub = authorities I point to
        new_hub = {}
        for p in nodes:
            s = 0.0
            for r, w in neighbors_cache[p].items():
                s += new_auth.get(r, 0.0) * w
            new_hub[p] = s
        norm = math.sqrt(sum(v * v for v in new_hub.values()))
        if norm < EPSILON_NORM:
            norm = 1.0
        new_hub = {n: v / norm for n, v in new_hub.items()}

        # Convergence: L2 distance between successive authority vectors
        delta = math.sqrt(sum((new_auth[n] - auth[n]) ** 2 for n in nodes))
        auth, hub = new_auth, new_hub
        if delta < EPSILON:
            log.debug(f"HITS converged at iter {step+1} (Δ={delta:.2e})")
            break

    return auth, hub


def compute_eigenvector_centrality(graph: ProductGraph,
                                    max_iter: int = EIGEN_ITERATIONS) -> dict[str, float]:
    """Eigenvector Centrality via power iteration on the (symmetrized) adjacency.

    x_v = (1/λ) · Σ_t  A[v,t] · x_t

    For our product graph edges are inserted bidirectionally
    (``add_bidirectional``), so the graph is effectively undirected and
    A = A^T. Using incoming(v) alone gives the correct undirected
    formulation. The Perron-Frobenius theorem guarantees convergence to
    the dominant eigenvector for connected non-negative graphs.

    Includes a Rayleigh-quotient eigenvalue estimate for diagnostics and
    early stopping once the eigenvalue estimate itself stabilizes.
    """
    nodes = list(graph.nodes)
    N = len(nodes)
    if N == 0:
        return {}

    # Initialize on the unit sphere (L2 = 1)
    init = 1.0 / math.sqrt(N)
    x = {n: init for n in nodes}

    incoming_cache = {n: graph.incoming(n) for n in nodes}
    prev_lambda = 0.0

    for iteration in range(max_iter):
        # A · x  (use incoming — correct for undirected graph)
        new_x = {}
        for v in nodes:
            s = 0.0
            for t, w in incoming_cache[v].items():
                s += w * x.get(t, 0.0)
            new_x[v] = s

        # Rayleigh quotient estimate of λ₁ = x^T A x / x^T x
        lam = math.sqrt(sum(v * v for v in new_x.values()))
        if lam < EPSILON_NORM:
            lam = 1.0
        new_x = {n: v / lam for n, v in new_x.items()}

        # Convergence: eigenvalue stable AND vector stable
        delta = sum(abs(new_x[n] - x[n]) for n in nodes)
        lambda_delta = abs(lam - prev_lambda)
        x = new_x
        prev_lambda = lam
        if delta < EPSILON and lambda_delta < EPSILON:
            log.debug(f"EigenCentrality converged at iter {iteration+1} "
                      f"(λ₁≈{lam:.4f}, Δ={delta:.2e})")
            break

    return x


# ─── Strategy Weight Engine ──────────────────────────────────────────────────

def compute_strategy_adjustments(products: dict, strategy: dict) -> dict[str, float]:
    """
    Compute per-product score adjustments based on user's strategy weights.
    Returns {pid: adjustment_multiplier} where 1.0 = neutral.
    """
    ps = strategy.get("price_sensitivity", 0.5)
    qf = strategy.get("quality_focus", 0.5)
    su = strategy.get("sustainability", 0.5)
    tf = strategy.get("trend_following", 0.5)
    cv = strategy.get("convenience", 0.5)

    adjustments = {}
    prices = [m.get("price", m.get("msrp", 100)) for m in products.values()]
    max_price = max(prices) if prices else 1
    min_price = min(prices) if prices else 0

    for pid, meta in products.items():
        price = meta.get("price", meta.get("msrp", 100))
        rating = meta.get("rating", 4.0)
        has_resale = meta.get("has_resale", False)
        momentum = meta.get("momentum", 0.5)
        availability = meta.get("availability", 0.5)

        if max_price > min_price:
            price_score = 1.0 - (price - min_price) / (max_price - min_price)
        else:
            price_score = 0.5
        price_adj = 1.0 + (price_score - 0.5) * ps * 0.6

        quality_score = (rating - 3.0) / 2.0
        quality_adj = 1.0 + (quality_score - 0.5) * qf * 0.5

        sus_adj = 1.0 + (0.3 if has_resale else -0.1) * su
        trend_adj = 1.0 + (momentum - 0.5) * tf * 0.4
        conv_adj = 1.0 + (availability - 0.5) * cv * 0.3

        adjustments[pid] = price_adj * quality_adj * sus_adj * trend_adj * conv_adj

    return adjustments


def adjust_fusion_weights(strategy: dict) -> tuple[float, float, float, float, float]:
    """
    Adjust algorithm fusion weights (α,β,γ,δ,ε) based on user strategy.

    - Price hunters → more PageRank (direct price-path propagation)
    - Quality focus → more Authority (HITS — pointed to by many hubs)
    - Trend setters → more Eigenvector Centrality (network influence)
    - Convenience → more Adjacency Walk (multi-hop path discovery)
    - Balanced → defaults
    """
    ps = strategy.get("price_sensitivity", 0.5)
    qf = strategy.get("quality_focus", 0.5)
    tf = strategy.get("trend_following", 0.5)
    su = strategy.get("sustainability", 0.5)
    cv = strategy.get("convenience", 0.5)

    alpha   = DEFAULT_ALPHA   + (ps - 0.5) * 0.12   # PageRank ↑ for price hunters
    beta    = DEFAULT_BETA    + (qf - 0.5) * 0.12   # Authority ↑ for quality focus
    gamma   = DEFAULT_GAMMA   + (su - 0.5) * 0.08   # Hub ↑ for sustainability
    delta   = DEFAULT_DELTA   + (tf - 0.5) * 0.12   # Eigen ↑ for trend following
    epsilon = DEFAULT_EPSILON + (cv - 0.5) * 0.10   # AdjWalk ↑ for convenience

    # Normalize to sum to 1.0
    total = alpha + beta + gamma + delta + epsilon
    return alpha/total, beta/total, gamma/total, delta/total, epsilon/total


# ─── InterestRank Engine ─────────────────────────────────────────────────────

class InterestRank:
    def __init__(self, products: dict, cache_ttl: int = CACHE_TTL):
        self.products = products
        self.base_graph = ProductGraph()
        self._build_base_graph()
        self.interactions: dict[str, list[tuple]] = defaultdict(list)
        self.visitor_strategies: dict[str, dict] = {}
        self._cache: dict[str, dict] = {}
        self._cache_ts: dict[str, float] = {}
        self._cache_ttl = cache_ttl
        self._max_interactions_per_visitor = 500  # bound memory
        log.info(f"InterestRank v4.0 initialized: {len(products)} products, "
                 f"4 algorithms (PageRank + HITS + EigenCentrality + AdjMatrix)")

    def _build_base_graph(self):
        g = self.base_graph
        by_cat = defaultdict(list)
        by_brand = defaultdict(list)
        by_band = defaultdict(list)

        for pid, meta in self.products.items():
            g.add_node(pid, meta)
            cat = meta.get("category", "").lower()
            brand = meta.get("brand", meta.get("name", "").split()[0]).lower()
            price = meta.get("price", meta.get("msrp", 100))
            by_cat[cat].append(pid)
            by_brand[brand].append(pid)
            by_band[get_price_band(price)].append(pid)

        for groups, weight, etype in [
            (by_cat, EDGE_WEIGHTS["category"], "category"),
            (by_brand, EDGE_WEIGHTS["brand"], "brand"),
            (by_band, EDGE_WEIGHTS["price_band"], "price_band"),
        ]:
            for key, pids in groups.items():
                for i, p1 in enumerate(pids):
                    for p2 in pids[i + 1:]:
                        g.add_bidirectional(p1, p2, weight, etype)

    def set_strategy(self, visitor_id: str, strategy: dict):
        """Set user's shopping strategy weights."""
        self.visitor_strategies[visitor_id] = strategy
        self._cache.pop(visitor_id, None)
        log.info(f"Strategy set for {visitor_id[:8]}…: {strategy}")

    def get_strategy(self, visitor_id: str) -> dict:
        return self.visitor_strategies.get(visitor_id, STRATEGY_PRESETS["balanced"])

    def record_interaction(self, visitor_id: str, product_id: str,
                           interaction_type: str, extra_weight: float = 1.0):
        # Silently ignore interactions for unknown products — keeps agent tool
        # usage from polluting the graph with ghost IDs.
        if product_id not in self.products:
            return
        base_w = EDGE_WEIGHTS.get(interaction_type, 1.0)
        ints = self.interactions[visitor_id]
        ints.append((product_id, interaction_type, time.time(), base_w * extra_weight))
        # Bound memory: drop oldest entries
        if len(ints) > self._max_interactions_per_visitor:
            del ints[: len(ints) - self._max_interactions_per_visitor]
        self._cache.pop(visitor_id, None)

    def _temporal_weight(self, timestamp: float) -> float:
        age = time.time() - timestamp
        return math.exp(-0.693 * age / TEMPORAL_DECAY_HALF_LIFE)

    def _build_visitor_graph(self, visitor_id: str) -> ProductGraph:
        g = ProductGraph()
        for node in self.base_graph.nodes:
            g.add_node(node, self.base_graph.node_meta.get(node))
        for src in self.base_graph.adj:
            for dst, w in self.base_graph.adj[src].items():
                g.add_edge(src, dst, w)

        visitor_ints = self.interactions.get(visitor_id, [])
        recent = []
        for pid, itype, ts, base_w in visitor_ints:
            if pid not in self.products:
                continue
            decay = self._temporal_weight(ts)
            w = base_w * decay
            g.add_edge(pid, pid, w * 2.0)
            for neighbor in self.base_graph.adj.get(pid, {}):
                g.add_edge(pid, neighbor, w * 0.5)
            recent.append((pid, ts, w))

        for i, (p1, t1, w1) in enumerate(recent):
            for p2, t2, w2 in recent[i + 1:]:
                if abs(t1 - t2) < 120 and p1 != p2:
                    co_w = EDGE_WEIGHTS["co_view"] * min(w1, w2) / 10.0
                    g.add_bidirectional(p1, p2, co_w)
        return g

    def _build_teleport(self, visitor_id: str, nodes: list) -> dict:
        N = len(nodes)
        teleport = {n: 1.0 / N for n in nodes}
        visitor_ints = self.interactions.get(visitor_id, [])
        if not visitor_ints:
            return teleport
        int_w = defaultdict(float)
        for pid, _, ts, w in visitor_ints:
            if pid in teleport:
                int_w[pid] += w * self._temporal_weight(ts)
        total = sum(int_w.values())
        if total > 0:
            for pid, w in int_w.items():
                teleport[pid] = 0.3 / N + 0.7 * (w / total)
            t_sum = sum(teleport.values())
            teleport = {n: v / t_sum for n, v in teleport.items()}
        return teleport

    def compute(self, visitor_id: str = "default") -> dict[str, dict]:
        if visitor_id in self._cache:
            if time.time() - self._cache_ts.get(visitor_id, 0) < self._cache_ttl:
                return self._cache[visitor_id]

        t0 = time.time()
        graph = self._build_visitor_graph(visitor_id)
        nodes = list(graph.nodes)
        if not nodes:
            return {}

        strategy = self.get_strategy(visitor_id)
        alpha, beta, gamma, delta, epsilon = adjust_fusion_weights(strategy)

        # === Pass 1: PageRank ===
        teleport = self._build_teleport(visitor_id, nodes)
        pr_scores = compute_pagerank(graph, teleport=teleport)

        # === Pass 2: HITS ===
        auth_scores, hub_scores = compute_hits(graph)

        # === Pass 3: Eigenvector Centrality ===
        eigen_scores = compute_eigenvector_centrality(graph)

        # === Pass 4: Adjacency Matrix Walk Scoring ===
        product_ids = [n for n in nodes if n in self.products]
        adj_matrix = AdjacencyMatrix(graph, product_ids)
        walk_scores = adj_matrix.walk_scores(depth=ADJ_WALK_DEPTH)
        spectral = adj_matrix.spectral_gap()
        triangles = adj_matrix.triangle_count()
        tri_density = adj_matrix.triangle_density()

        # === Normalize each to 0–100 ===
        # Min-max normalization with degenerate-case handling: if all values
        # are identical (or empty), return a flat 50 — neutral prior, so no
        # single signal dominates when it carries no information.
        def norm100(d):
            if not self.products:
                return {}
            vals = [d.get(n, 0) for n in self.products]
            mn, mx = min(vals), max(vals)
            rng = mx - mn
            if rng < EPSILON_NORM:
                return {n: 50.0 for n in self.products}
            return {n: ((d.get(n, 0) - mn) / rng) * 100.0 for n in self.products}

        pr_n = norm100(pr_scores)
        auth_n = norm100(auth_scores)
        hub_n = norm100(hub_scores)
        eigen_n = norm100(eigen_scores)
        walk_n = norm100(walk_scores)

        # === Strategy adjustments ===
        strat_adj = compute_strategy_adjustments(self.products, strategy)

        # === Fuse: α·PR + β·Auth + γ·Hub + δ·Eigen + ε·Walk, then apply strategy ===
        result = {}
        for pid in self.products:
            raw = (alpha * pr_n.get(pid, 0) + beta * auth_n.get(pid, 0) +
                   gamma * hub_n.get(pid, 0) + delta * eigen_n.get(pid, 0) +
                   epsilon * walk_n.get(pid, 0))
            adjusted = raw * strat_adj.get(pid, 1.0)
            adjusted = round(min(100, max(0, adjusted)), 2)
            result[pid] = {
                "score": adjusted,
                "pagerank": round(pr_n.get(pid, 0), 2),
                "authority": round(auth_n.get(pid, 0), 2),
                "hub": round(hub_n.get(pid, 0), 2),
                "eigenvector": round(eigen_n.get(pid, 0), 2),
                "adj_walk": round(walk_n.get(pid, 0), 2),
                "strategy_multiplier": round(strat_adj.get(pid, 1.0), 3),
                "signal": self._signal_label(adjusted),
                "fusion_weights": {
                    "alpha": round(alpha, 3), "beta": round(beta, 3),
                    "gamma": round(gamma, 3), "delta": round(delta, 3),
                    "epsilon": round(epsilon, 3),
                },
                "adjacency_matrix": {
                    "spectral_gap": round(spectral, 4),
                    "triangle_density": round(tri_density, 4),
                    "confidence": "high" if spectral > 2.0 else "medium" if spectral > 0.5 else "low",
                },
            }

        result = dict(sorted(result.items(), key=lambda x: x[1]["score"], reverse=True))
        elapsed = (time.time() - t0) * 1000
        log.info(f"InterestRank v4.0: visitor={visitor_id[:8]}… "
                 f"elapsed={elapsed:.1f}ms spectral_gap={spectral:.3f} "
                 f"triangles={triangles} strategy={strategy.get('description','custom')}")

        self._cache[visitor_id] = result
        self._cache_ts[visitor_id] = time.time()
        return result

    def get_recommendations(self, visitor_id: str, top_k: int = 8) -> list[dict]:
        raw = self.compute(visitor_id)
        if not raw:
            return self._cold_start_recommendations(top_k)
        recs = []
        for pid, data in list(raw.items())[:top_k]:
            meta = self.products.get(pid, {})
            recs.append({
                "id": pid, "name": meta.get("name", pid),
                "category": meta.get("category", ""),
                "interest_score": data["score"],
                "pagerank": data["pagerank"],
                "authority": data["authority"],
                "hub": data["hub"],
                "eigenvector": data["eigenvector"],
                "adj_walk": data["adj_walk"],
                "strategy_multiplier": data["strategy_multiplier"],
                "signal_strength": data["signal"],
                "fusion_weights": data["fusion_weights"],
                "adjacency_matrix": data["adjacency_matrix"],
                "rank_reasons": self._explain(visitor_id, pid, data),
                "algorithm": (f"α·PR({data['pagerank']:.0f}) + β·Auth({data['authority']:.0f}) "
                              f"+ γ·Hub({data['hub']:.0f}) + δ·Eigen({data['eigenvector']:.0f}) "
                              f"+ ε·Walk({data['adj_walk']:.0f})"),
            })
        return recs

    def _explain(self, visitor_id, pid, data):
        reasons = []
        for i in self.interactions.get(visitor_id, []):
            if i[0] == pid:
                t = {"purchase":"You purchased this","cart":"In your cart",
                     "click":"You viewed this","search":"Matched your search"}.get(i[1])
                if t and t not in reasons:
                    reasons.append(t)
        if data["authority"] > 70:
            reasons.append("High authority: many interest signals converge")
        if data["eigenvector"] > 70:
            reasons.append("High influence in the product network")
        if data["adj_walk"] > 70:
            reasons.append("Multi-hop paths: connected via many product relationships")
        if data["strategy_multiplier"] > 1.1:
            reasons.append("Boosted by your strategy preferences")
        if data.get("adjacency_matrix", {}).get("confidence") == "high":
            reasons.append("High-confidence recommendation (strong spectral gap)")
        return reasons[:4] or ["Discovered via product network"]

    def _signal_label(self, score):
        if score >= 85: return "🔥 STRONG MATCH"
        elif score >= 65: return "⚡ HIGH INTEREST"
        elif score >= 40: return "📈 GROWING"
        elif score >= 20: return "💡 EMERGING"
        else: return "🔍 DISCOVERY"

    def clear_visitor_cache(self, visitor_id: str = None):
        """Invalidate cached scores (for one visitor or all)."""
        if visitor_id is None:
            self._cache.clear()
            self._cache_ts.clear()
        else:
            self._cache.pop(visitor_id, None)
            self._cache_ts.pop(visitor_id, None)

    def _cold_start_recommendations(self, top_k):
        auth, _ = compute_hits(self.base_graph)
        eigen = compute_eigenvector_centrality(self.base_graph)
        product_ids = list(self.products.keys())
        adj = AdjacencyMatrix(self.base_graph, product_ids)
        walk = adj.walk_scores()
        scores = {}
        for pid in self.products:
            a = auth.get(pid, 0)
            e = eigen.get(pid, 0)
            w = walk.get(pid, 0)
            scores[pid] = a * 35 + e * 35 + w * 30
        sorted_pids = sorted(scores, key=scores.get, reverse=True)
        mx = max(scores.values(), default=1)
        return [{
            "id": pid, "name": self.products[pid].get("name", pid),
            "category": self.products[pid].get("category", ""),
            "interest_score": round(min(100, scores[pid] / mx * 70 + 20), 2),
            "signal_strength": "🔍 DISCOVERY",
            "rank_reasons": ["Popular (cold start)"],
            "algorithm": "Cold start — HITS + EigenCentrality + AdjMatrix walks",
        } for pid in sorted_pids[:top_k]]

    def get_stats(self, visitor_id=None):
        product_ids = list(self.products.keys())
        adj = AdjacencyMatrix(self.base_graph, product_ids)
        adj_diag = adj.get_diagnostics()
        
        stats = {
            "algorithm": "InterestRank v4.0 — PageRank + HITS + EigenCentrality + AdjMatrix",
            "papers": ["Brin & Page 1998", "Kleinberg 1999", "Bonacich 1972"],
            "references": [
                "https://en.wikipedia.org/wiki/Adjacency_matrix",
                "https://en.wikipedia.org/wiki/Eigenvector_centrality",
                "https://en.wikipedia.org/wiki/HITS_algorithm",
            ],
            "fusion": "α·PR + β·Auth + γ·Hub + δ·Eigen + ε·AdjWalk (strategy-adjusted)",
            "defaults": {
                "alpha": DEFAULT_ALPHA, "beta": DEFAULT_BETA,
                "gamma": DEFAULT_GAMMA, "delta": DEFAULT_DELTA,
                "epsilon": DEFAULT_EPSILON,
            },
            "damping": DAMPING_FACTOR,
            "walk_depth": ADJ_WALK_DEPTH,
            "products": len(self.products),
            "adjacency_matrix": adj_diag,
            "strategy_presets": list(STRATEGY_PRESETS.keys()),
        }
        if visitor_id:
            ints = self.interactions.get(visitor_id, [])
            tc = defaultdict(int)
            for _, t, _, _ in ints:
                tc[t] += 1
            stats["visitor"] = {
                "interactions": len(ints), "types": dict(tc),
                "products_touched": len(set(i[0] for i in ints)),
                "strategy": self.get_strategy(visitor_id),
            }
        return stats
