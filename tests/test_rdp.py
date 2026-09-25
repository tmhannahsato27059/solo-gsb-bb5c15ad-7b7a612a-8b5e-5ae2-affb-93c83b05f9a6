"""Unit tests for the RDP accounting math."""
from __future__ import annotations

import math

import pytest

from app import rdp


def test_empty_consumes_nothing():
    curve, eps, _ = rdp.evaluate_entries([], 1e-6)
    assert eps == 0.0
    assert set(curve) == {0.0}


def test_pure_dp_composition_is_exact_addition():
    entries = [(0.1, 0.0), (0.2, 0.0), (0.3, 0.0)]
    _, eps, order = rdp.evaluate_entries(entries, 1e-9)
    assert eps == pytest.approx(0.6)
    assert order == rdp.RDP_ORDERS[0]


def test_single_approx_dp_is_conservative_but_tight_at_high_orders():
    # A single (0.2, 1e-8)-DP mechanism converted at delta=1e-6 must report
    # at least the mechanism's own epsilon, and approach it at high orders.
    _, eps, best_order = rdp.evaluate_entries([(0.2, 1e-8)], 1e-6)
    assert eps >= 0.2 - 1e-12
    assert best_order in (1024.0, 2048.0, 4096.0, 8192.0)


def test_composition_never_underestimates_parts():
    # Basic sanity: the composed guarantee must dominate each constituent
    # mechanism's own (eps, delta)-DP parameters.
    entries = [(0.1, 1e-8), (0.15, 1e-8), (0.05, 1e-7)]
    _, eps, _ = rdp.evaluate_entries(entries, 1e-6)
    for e, _ in entries:
        assert eps >= e


def test_mixed_pure_and_approx():
    # Pure-DP epsilon is added exactly on top of the approximate-DP curve.
    only_approx, eps_approx, _ = rdp.evaluate_entries([(0.1, 1e-8)], 1e-6)
    entries = [(0.3, 0.0), (0.1, 1e-8)]
    _, eps_mixed, _ = rdp.evaluate_entries(entries, 1e-6)
    assert eps_mixed == pytest.approx(eps_approx + 0.3, abs=1e-9)


def test_projection_is_consistent_with_evaluation():
    entries = [(0.1, 1e-8), (0.1, 1e-8)]
    projected, _ = rdp.project_entries(entries, 0.1, 1e-8, 1e-6)
    _, evaluated, _ = rdp.evaluate_entries(
        entries + [(0.1, 1e-8)], 1e-6
    )
    assert projected == pytest.approx(evaluated)


def test_rdp_curve_of_approx_dp():
    curve = rdp.single_rdp_curve(0.2, 1e-8)
    assert len(curve) == len(rdp.RDP_ORDERS)
    # eps_rdp(alpha) = eps + log(1/delta)/(alpha-1), decreasing in alpha.
    assert curve[0] > curve[-1]
    assert curve[0] == pytest.approx(0.2 + math.log(1e8) / (rdp.RDP_ORDERS[0] - 1))


def test_rdp_curve_of_pure_dp_is_flat():
    curve = rdp.single_rdp_curve(0.42, 0.0)
    assert set(curve) == {0.42}


def test_tiny_delta_curve_stays_finite():
    # 1/delta overflows float64 for delta < ~5.6e-309; the curve is built
    # from -log(delta) so it stays finite for any positive delta.
    delta = 1e-309
    curve = rdp.single_rdp_curve(0.1, delta)
    assert len(curve) == len(rdp.RDP_ORDERS)
    assert all(math.isfinite(value) for value in curve)
    log_inv_delta = -math.log(delta)
    assert curve[0] == pytest.approx(
        0.1 + log_inv_delta / (rdp.RDP_ORDERS[0] - 1)
    )
    assert curve[-1] == pytest.approx(
        0.1 + log_inv_delta / (rdp.RDP_ORDERS[-1] - 1)
    )


def test_tiny_delta_curve_down_to_smallest_denormal():
    curve = rdp.single_rdp_curve(0.2, 5e-324)
    assert all(math.isfinite(value) for value in curve)


def test_tiny_delta_projection_is_finite_and_within_cap():
    # The ledger scenario: cap 1.0 / delta_cap 1e-6, one (0.1, 1e-309)-DP
    # request.  The projection is finite and far below the cap, so the
    # request must not be rejected as over budget.
    _, eps, order = rdp.evaluate_entries([(0.1, 1e-309)], 1e-6)
    assert math.isfinite(eps)
    assert 0.1 <= eps < 1.0
    projected, proj_order = rdp.project_entries([], 0.1, 1e-309, 1e-6)
    assert projected == pytest.approx(eps)
    assert proj_order == order


def test_tiny_delta_mixed_with_pure_epsilon():
    # Pure-epsilon entries keep exact addition on top of a tiny-delta curve.
    _, approx_only, _ = rdp.evaluate_entries([(0.1, 1e-309)], 1e-6)
    _, mixed, _ = rdp.evaluate_entries([(0.3, 0.0), (0.1, 1e-309)], 1e-6)
    assert mixed == pytest.approx(approx_only + 0.3, abs=1e-9)


def test_tiny_delta_composes_with_ordinary_entries():
    # A tiny-delta entry composes additively per order with ordinary ones.
    _, combined, _ = rdp.evaluate_entries(
        [(0.2, 1e-8), (0.1, 1e-309)], 1e-6
    )
    _, ordinary, _ = rdp.evaluate_entries([(0.2, 1e-8)], 1e-6)
    _, tiny, _ = rdp.evaluate_entries([(0.1, 1e-309)], 1e-6)
    assert math.isfinite(combined)
    assert combined >= max(ordinary, tiny)
