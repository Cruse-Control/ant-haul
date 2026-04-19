"""Cost tracking and estimation for seed-storage API calls."""

from __future__ import annotations

import uuid

import psycopg2
import psycopg2.extras

from . import config
from .staging import _connect, summary as staging_summary

# Pricing per 1M tokens (as of April 2026)
PRICING = {
    "gemini": {"embed_text": 0.20, "embed_image": 0.00012},  # per image, not per 1M
    "anthropic": {
        "haiku_input": 1.00,
        "haiku_output": 5.00,
        "haiku_batch_input": 0.50,
        "haiku_batch_output": 2.50,
    },
}


def log_cost(
    *,
    operation: str,
    provider: str,
    model: str,
    input_tokens: int = 0,
    output_tokens: int = 0,
    cost_usd: float,
    source_id: str | None = None,
    batch_id: str | None = None,
):
    """Log a single API cost event."""
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO seed_costs (operation, provider, model, input_tokens, output_tokens,
                    cost_usd, source_id, batch_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (operation, provider, model, input_tokens, output_tokens, cost_usd, source_id, batch_id),
            )
        conn.commit()


def estimate() -> dict:
    """Estimate cost for all currently staged content."""
    s = staging_summary()
    total_tokens = s["total_tokens"]

    embed_cost = total_tokens * PRICING["gemini"]["embed_text"] / 1_000_000
    extract_input_cost = total_tokens * PRICING["anthropic"]["haiku_input"] / 1_000_000
    # Assume extraction output is ~15% of input length
    extract_output_tokens = int(total_tokens * 0.15)
    extract_output_cost = extract_output_tokens * PRICING["anthropic"]["haiku_output"] / 1_000_000

    return {
        "staged_items": s["total_items"],
        "total_tokens": total_tokens,
        "by_type": s["by_type"],
        "costs": {
            "gemini_embedding": round(embed_cost, 4),
            "haiku_extraction_input": round(extract_input_cost, 4),
            "haiku_extraction_output": round(extract_output_cost, 4),
            "total": round(embed_cost + extract_input_cost + extract_output_cost, 4),
        },
    }


def report() -> dict:
    """Report actual costs spent so far."""
    with _connect() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            # By provider
            cur.execute("""
                SELECT provider, operation,
                    count(*) AS calls,
                    sum(input_tokens) AS input_tokens,
                    sum(output_tokens) AS output_tokens,
                    sum(cost_usd)::float AS total_cost
                FROM seed_costs
                GROUP BY provider, operation
                ORDER BY total_cost DESC
            """)
            by_operation = [dict(r) for r in cur.fetchall()]

            # Total
            cur.execute("SELECT sum(cost_usd)::float AS total FROM seed_costs")
            total = cur.fetchone()["total"] or 0.0

            # By batch
            cur.execute("""
                SELECT batch_id,
                    count(*) AS calls,
                    sum(cost_usd)::float AS total_cost
                FROM seed_costs
                WHERE batch_id IS NOT NULL
                GROUP BY batch_id
                ORDER BY total_cost DESC
                LIMIT 10
            """)
            by_batch = [dict(r) for r in cur.fetchall()]

    return {
        "total_spent_usd": round(total, 4),
        "by_operation": by_operation,
        "recent_batches": by_batch,
    }


def print_estimate():
    """Print a human-readable cost estimate to stdout."""
    est = estimate()
    print("\nSeed Storage Cost Estimate")
    print("=" * 40)
    print(f"Staged: {est['staged_items']} items ({est['total_tokens']:,} tokens)\n")

    for row in est["by_type"]:
        print(f"  {row['source_type']:25s} {row['items']:5d} items  {row['total_tokens']:>10,} tokens")

    c = est["costs"]
    print(f"\nEstimated costs:")
    print(f"  Gemini embedding:      ${c['gemini_embedding']:.4f}")
    print(f"  Haiku extract (input): ${c['haiku_extraction_input']:.4f}")
    print(f"  Haiku extract (output):${c['haiku_extraction_output']:.4f}")
    print(f"  {'─' * 34}")
    print(f"  Total:                 ${c['total']:.4f}")
    print()


def print_report():
    """Print a human-readable cost report to stdout."""
    r = report()
    print("\nSeed Storage Cost Report")
    print("=" * 40)
    print(f"Total spent: ${r['total_spent_usd']:.4f}\n")

    for row in r["by_operation"]:
        print(f"  {row['provider']:10s} {row['operation']:10s} {row['calls']:5d} calls  ${row['total_cost']:.4f}")
    print()
