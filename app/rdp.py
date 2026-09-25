"""Rényi Differential Privacy (RDP) accounting.

Each analysis request is declared as an (epsilon, delta)-DP guarantee.  We
convert every granted request to its RDP curve
``epsilon(alpha)`` for a fixed grid of Rényi orders, sum the per-order curves
(composition under RDP is additive), and convert the *composed* curve back to
an (epsilon, delta)-DP guarantee at the dataset's target delta:

    RDP(alpha) of an (eps, delta)-DP mechanism (a standard conservative bound):

        eps_rdp(alpha) = eps + log(1/delta) / (alpha - 1)

    pure eps-DP (delta = 0):

        eps_rdp(alpha) = eps

    composition of mechanisms M1, ..., Mk:

        eps_rdp_total(alpha) = sum_i eps_rdp_i(alpha)

    conversion back to (eps', delta')-DP:

        eps'(delta') = min_alpha [ eps_rdp_total(alpha)
                                   + log( (alpha - 1) / alpha
                                          * (1 - delta') / delta' ) / (alpha - 1) ]

The bound above is the standard textbook conversion
(Mironov, 2017; Proposition in the RDP paper).  It is deliberately a simple,
robust upper bound that works for arbitrary query types without any
mechanism-specific calibration: the ledger treats every request as an
approximate-DP guarantee rather than exploiting tighter Gaussian accounting.

Numerical note: ``log(1/delta)`` is evaluated in the log domain as
``-log(delta)``.  Forming the reciprocal ``1/delta`` first overflows to
``inf`` for ``delta`` below ~5.6e-309 (e.g. ``1e-309``), which would poison
the whole curve with ``inf`` and make every admission check reject a
request whose projection is in fact finite.  The negated logarithm stays
finite and accurate down to the smallest positive double (~5e-324).
"""
from __future__ import annotations

import math

# Dense around small orders (where the optimum sits for large composed
# mechanisms) and extended to large orders, where a single (eps, delta)-DP
# mechanism converts back tightly (the conversion overhead shrinks like
# log(1/delta)/(alpha-1)).  ~160 orders is cheap to sum per request.
RDP_ORDERS: tuple[float, ...] = (
    tuple(round((10 + i) / 10, 1) for i in range(1, 100))  # 1.1 .. 10.9
    + tuple(float(a) for a in range(11, 64))  # 11 .. 63
    + (128.0, 256.0, 512.0, 1024.0, 2048.0, 4096.0, 8192.0)
)

# Orders surfaced in human-readable budget output (nearest available order
# from RDP_ORDERS is used for each).
SAMPLE_ORDERS: tuple[float, ...] = (2.0, 4.0, 8.0, 16.0, 32.0, 64.0, 128.0, 512.0, 4096.0)


def single_rdp_curve(epsilon: float, delta: float) -> list[float]:
    """Return the RDP curve of one (epsilon, delta)-DP mechanism over RDP_ORDERS."""
    if delta > 0.0:
        # log(1/delta) as -log(delta): the reciprocal 1/delta overflows to
        # inf for delta < ~5.6e-309 (e.g. 1e-309), which would turn every
        # order of the curve into inf and wrongly reject the admission.
        log_inv_delta = -math.log(delta)
        return [
            epsilon + log_inv_delta / (alpha - 1.0)
            for alpha in RDP_ORDERS
        ]
    # Pure epsilon-DP implies RDP(alpha) = epsilon at every order.
    return [epsilon for _ in RDP_ORDERS]


def compose_rdp(curves: list[list[float]]) -> list[float]:
    """Sum RDP curves (parallel/sequential composition are both additive)."""
    if not curves:
        return [0.0 for _ in RDP_ORDERS]
    return [sum(curve[i] for curve in curves) for i in range(len(RDP_ORDERS))]


def rdp_to_approx_dp(rdp: list[float], target_delta: float) -> tuple[float, float]:
    """Convert a composed RDP curve to (epsilon, delta)-DP.

    Returns ``(epsilon, best_order)`` at ``delta = target_delta``.
    """
    if target_delta <= 0.0:
        # (eps, 0)-DP follows from RDP only via the minimum-order value,
        # i.e. pure DP composition: epsilon_total = sum of epsilons.
        best_idx = min(range(len(RDP_ORDERS)), key=lambda i: RDP_ORDERS[i])
        return rdp[best_idx], RDP_ORDERS[best_idx]

    log_term = math.log((1.0 - target_delta) / target_delta)
    best_epsilon = math.inf
    best_order = RDP_ORDERS[0]
    for alpha, eps_alpha in zip(RDP_ORDERS, rdp):
        eps = eps_alpha + (math.log((alpha - 1.0) / alpha) + log_term) / (alpha - 1.0)
        if eps < best_epsilon:
            best_epsilon = eps
            best_order = alpha
    return best_epsilon, best_order


def composed_epsilon(curves: list[list[float]], target_delta: float) -> tuple[float, float]:
    """Compose RDP curves and convert to a single (epsilon, delta)-DP value."""
    return rdp_to_approx_dp(compose_rdp(curves), target_delta)


def evaluate_entries(
    entries: list[tuple[float, float]], target_delta: float
) -> tuple[list[float], float, float]:
    """Compose granted mechanisms and return ``(curve, epsilon, best_order)``.

    Composition rule by entry type:

    * entries with ``delta == 0`` are pure epsilon-DP mechanisms, whose RDP
      curve is exactly ``epsilon`` at every order.  If *all* granted entries
      are pure-DP, composition is the exact sum of epsilons and no RDP
      conversion over-approximation is charged;
    * entries with ``delta > 0`` are converted with the conservative
      (eps, delta)-DP -> RDP bound and summed across orders.

    A mixture is handled by summing the flat pure-DP offsets onto the RDP
    curve (pure DP is a special constant RDP curve), then converting once.
    """
    pure_epsilon_sum = sum(eps for eps, d in entries if d == 0.0)
    approx = [(eps, d) for eps, d in entries if d > 0.0]

    composed = [
        pure_epsilon_sum + part
        for part in compose_rdp([single_rdp_curve(eps, d) for eps, d in approx])
    ]

    if not entries:
        return composed, 0.0, RDP_ORDERS[-1]
    if not approx:
        # Exact pure-DP basic composition.
        return composed, pure_epsilon_sum, RDP_ORDERS[0]

    epsilon, order = rdp_to_approx_dp(composed, target_delta)
    return composed, max(0.0, epsilon), order


def project_entries(
    entries: list[tuple[float, float]],
    request_epsilon: float,
    request_delta: float,
    target_delta: float,
) -> tuple[float, float]:
    """Composed (epsilon, best_order) *if* one more request were granted."""
    curve, epsilon, order = evaluate_entries(
        [*entries, (request_epsilon, request_delta)], target_delta
    )
    return epsilon, order


def sample_curve(rdp: list[float]) -> dict[str, float]:
    """Pick a representative subset of (order -> epsilon) for API responses."""
    out: dict[str, float] = {}
    for wanted in SAMPLE_ORDERS:
        idx = min(
            range(len(RDP_ORDERS)),
            key=lambda i: abs(RDP_ORDERS[i] - wanted),
        )
        out[str(wanted)] = round(rdp[idx], 12)
    return out
