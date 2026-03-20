"""
================================================================================
  AFUNDMENTOR PRO — AI WEALTH ADVISOR ENGINE v3.0
  Author  : Built for Ankit | Bangalore
  Version : 3.0.0 (complete rewrite — all issues fixed)

  WHAT CHANGED FROM v2:
    1. Portfolio passed as a plain TEXT TABLE not JSON
       — far more token-efficient, all 14 funds always visible to AI
    2. AI cascade order: OpenAI -> Gemini 1.5 Flash -> Groq
       — Gemini 1.5 Flash has better free limits than flash-lite
    3. AI engine name shown in Telegram message and dashboard
    4. No JSON repair logic (was causing more harm than good)
    5. Output tokens fixed at 8192 across all engines
    6. Overlap analyser rebuilt using fund name patterns (not dummy ISINs)
    7. Strict fund count validation in report validator
    8. responseMimeType: application/json added for Gemini (forces valid JSON)
    9. Clean prompt design — AI instructed to use EXACT names + amounts from table
   10. Single _post() helper with one retry — simple and reliable
================================================================================
"""

import os
import json
import time
import datetime
import requests
import subprocess
import shutil
from typing import Optional, Tuple

# ── Global state ──────────────────────────────────────────────────────────────
_ai_engine_used: str = "N/A"


# ═══════════════════════════════════════════════════════════════════════════════
#  SECTION 1 — INVESTOR PROFILE
# ═══════════════════════════════════════════════════════════════════════════════

INVESTOR_PROFILE = {
    "name": "Ankit",
    "age": 34,
    "city": "Bangalore",
    "target_year": 2041,
    "monthly_salary_inr": 140000,
    "home_loan_emi_inr": 60000,
    "home_loan_months_remaining": 241,
    "home_loan_rate_pct": 8.5,
    "monthly_living_expenses_inr": 40000,
    "monthly_sip_inr": 20000,
    "free_cash_monthly_inr": 60000,
    "emergency_fund_inr": 350000,
    "emergency_fund_target_inr": 600000,
    "has_term_insurance": True,
    "has_health_insurance": True,
    "health_plan": "Niva Bupa Aspire Platinum+",
    "term_cover_inr": None,
    "role": "Technical Manager — EV Charging & Thermal Systems",
    "domain": "EV charging + battery thermal management",
    "ev_domain_advantage": True,
    "consulting_side_income_inr": 0,
    "planned_expenses": [
        {"label": "Car purchase", "estimated_inr": 800000,
         "months_away": 18, "likely_loan": True, "estimated_loan_emi": 17000},
        {"label": "International trip", "estimated_inr": 200000,
         "months_away": 12, "likely_loan": False},
    ],
    "income_tax_slab_pct": 30,
    "ltcg_booked_this_year_inr": 0,
    "risk_appetite": "aggressive",
    "equity_target_pct": 85,
    "gold_target_pct": 5,
    "international_target_pct": 10,
}


# ═══════════════════════════════════════════════════════════════════════════════
#  SECTION 2 — DUMMY PORTFOLIO (fallback if Zerodha unavailable)
# ═══════════════════════════════════════════════════════════════════════════════

DUMMY_PORTFOLIO = [
    {
        "fund_name": "Parag Parikh Flexi Cap Fund",
        "isin": "INF879O01019",
        "plan": "direct",
        "units": 142.85, "avg_nav": 52.50, "current_nav": 72.80,
        "invested_inr": 75000, "current_value_inr": 104000,
        "expense_ratio_pct": 0.58, "exit_load_pct": 1.0,
        "sip_start_date": "2023-04-01", "total_exit_cost_inr": 0,
    },
    {
        "fund_name": "Axis Bluechip Fund",
        "isin": "INF846K01EW2",
        "plan": "regular",
        "units": 98.10, "avg_nav": 45.00, "current_nav": 56.20,
        "invested_inr": 45000, "current_value_inr": 55150,
        "expense_ratio_pct": 1.62, "exit_load_pct": 1.0,
        "sip_start_date": "2023-06-01", "total_exit_cost_inr": 0,
    },
    {
        "fund_name": "SBI Small Cap Fund",
        "isin": "INF200K01RB2",
        "plan": "direct",
        "units": 112.30, "avg_nav": 89.00, "current_nav": 124.60,
        "invested_inr": 100000, "current_value_inr": 139930,
        "expense_ratio_pct": 0.72, "exit_load_pct": 1.0,
        "sip_start_date": "2023-02-01", "total_exit_cost_inr": 0,
    },
    {
        "fund_name": "Mirae Asset Large Cap Fund",
        "isin": "INF769K01010",
        "plan": "direct",
        "units": 75.40, "avg_nav": 66.00, "current_nav": 78.50,
        "invested_inr": 50000, "current_value_inr": 59210,
        "expense_ratio_pct": 1.68, "exit_load_pct": 1.0,
        "sip_start_date": "2023-09-01", "total_exit_cost_inr": 0,
    },
]


# ═══════════════════════════════════════════════════════════════════════════════
#  SECTION 3 — MARKET CONTEXT
# ═══════════════════════════════════════════════════════════════════════════════

def fetch_market_context() -> dict:
    return {
        "date": str(datetime.date.today()),
        "nifty50_pe": 22.4,
        "rbi_repo_rate_pct": 6.25,
        "rbi_stance": "neutral",
        "cpi_inflation_pct": 4.8,
        "fii_flow_monthly_cr": -4200,
        "dii_flow_monthly_cr": 6800,
        "india_vix": 15.4,
        "market_valuation_signal": "CAUTION",
        "ltcg_harvest_window": True,
        "ev_sector_sentiment": "BULLISH",
        "ev_hiring_trend": "SURGING",
    }


# ═══════════════════════════════════════════════════════════════════════════════
#  SECTION 4 — EXIT COST CALCULATOR
# ═══════════════════════════════════════════════════════════════════════════════

def calculate_exit_cost(fund: dict, investor: dict) -> dict:
    today = datetime.date.today()
    try:
        sip_start = datetime.date.fromisoformat(fund["sip_start_date"])
    except (ValueError, KeyError):
        sip_start = today - datetime.timedelta(days=730)

    months_invested = max(
        (today.year - sip_start.year) * 12 + (today.month - sip_start.month), 1
    )
    days_held = (today - sip_start).days

    units_in_window = min(12, months_invested)
    value_in_window = (units_in_window / months_invested) * fund["current_value_inr"]
    exit_load = value_in_window * (fund.get("exit_load_pct", 1.0) / 100)

    total_gain = fund["current_value_inr"] - fund["invested_inr"]
    if days_held >= 365:
        ltcg_exempt = max(0, 125000 - investor["ltcg_booked_this_year_inr"])
        tax = max(0, total_gain - ltcg_exempt) * 0.125
        tax_type = "LTCG 12.5%"
    else:
        tax = max(0, total_gain) * 0.20
        tax_type = "STCG 20%"

    stt = fund["current_value_inr"] * 0.00001
    stamp = fund["current_value_inr"] * 0.00005
    opp_cost = fund["current_value_inr"] * (0.12 / 252) * 5
    total = exit_load + tax + stt + stamp + opp_cost

    return {
        "fund_name": fund["fund_name"],
        "exit_load_inr": round(exit_load, 2),
        "tax_inr": round(tax, 2),
        "tax_type": tax_type,
        "total_exit_cost_inr": round(total, 2),
        "total_exit_cost_pct": round(total / max(fund["current_value_inr"], 1) * 100, 2),
        "days_held": days_held,
    }


# ═══════════════════════════════════════════════════════════════════════════════
#  SECTION 5 — PERSONAL RISK SCORER
# ═══════════════════════════════════════════════════════════════════════════════

def score_personal_risk(investor: dict, market: dict) -> dict:
    score = 10.0
    flags, positives = [], []

    emi_ratio = investor["home_loan_emi_inr"] / investor["monthly_salary_inr"]
    if emi_ratio > 0.40:
        score -= 1.5
        flags.append(f"EMI ratio {emi_ratio*100:.1f}% — above 40% threshold")
    elif emi_ratio > 0.35:
        score -= 0.5
        flags.append(f"EMI ratio {emi_ratio*100:.1f}% — watch if rate rises")
    else:
        positives.append("EMI ratio healthy")

    burn = investor["home_loan_emi_inr"] + investor["monthly_living_expenses_inr"]
    ef_months = investor["emergency_fund_inr"] / burn
    if ef_months < 3:
        score -= 2.0
        flags.append(f"Emergency fund only {ef_months:.1f}mo — critical")
    elif ef_months < 6:
        score -= 0.8
        flags.append(f"Emergency fund {ef_months:.1f}mo — target 6mo (Rs {investor['emergency_fund_target_inr']:,})")
    else:
        positives.append(f"Emergency fund {ef_months:.1f}mo — healthy")

    for exp in investor["planned_expenses"]:
        if exp.get("likely_loan") and exp["months_away"] <= 18:
            future_ratio = (investor["home_loan_emi_inr"] + exp["estimated_loan_emi"]) / investor["monthly_salary_inr"]
            if future_ratio > 0.50:
                score -= 1.0
                flags.append(f"{exp['label']}: total EMI hits {future_ratio*100:.0f}% in {exp['months_away']}mo")

    if investor["term_cover_inr"] is None:
        score -= 0.5
        flags.append("Term cover unverified")

    if investor["ev_domain_advantage"] and market.get("ev_sector_sentiment") == "BULLISH":
        score = min(10.0, score + 0.5)
        positives.append("EV sector bullish — income risk low")

    return {
        "personal_risk_score": round(max(0.0, min(10.0, score)), 1),
        "flags": flags,
        "positives": positives,
        "emi_ratio_pct": round(emi_ratio * 100, 1),
        "emergency_fund_months": round(ef_months, 1),
        "monthly_burn_inr": burn,
    }


# ═══════════════════════════════════════════════════════════════════════════════
#  SECTION 6 — SIP STEP-UP CALCULATOR
# ═══════════════════════════════════════════════════════════════════════════════

def calculate_stepup_guidance(investor: dict, personal_risk: dict, market: dict) -> dict:
    ef_months = personal_risk["emergency_fund_months"]
    career_boom = (
        market.get("ev_sector_sentiment") == "BULLISH"
        and market.get("ev_hiring_trend") == "SURGING"
    )

    def corpus15(sip: float, cagr: float = 0.12) -> float:
        n, r = 180, cagr / 12
        return sip * ((((1 + r) ** n) - 1) / r) * (1 + r)

    def real(nominal: float) -> float:
        return nominal / ((1.06) ** 15)

    sip = investor["monthly_sip_inr"]

    if ef_months < 4:
        phase, label, sip_inc, buf_add = 1, "Buffer Building", 0, 10000
        months_to_close = int((investor["emergency_fund_target_inr"] - investor["emergency_fund_inr"]) / 10000)
        reason = f"Build emergency fund first. Rs 10k/mo to savings. Done in ~{months_to_close}mo."
    elif ef_months < 6:
        phase, label, sip_inc, buf_add = 2, "Parallel Build", 5000, 5000
        gap = investor["emergency_fund_target_inr"] - investor["emergency_fund_inr"]
        reason = f"Split: Rs 5k SIP + Rs 5k buffer. Buffer closes ~{int(gap/5000)}mo."
    else:
        phase, label, buf_add = 3, "Full Acceleration", 0
        pct = 0.65 if career_boom else 0.50
        sip_inc = int(15000 * pct)
        reason = f"Buffer secured. Invest {int(pct*100)}% of April increment. EV career tailwind strong."

    return {
        "phase": phase,
        "phase_label": label,
        "sip_increase_recommended_inr": sip_inc,
        "buffer_monthly_addition_inr": buf_add,
        "reasoning": reason,
        "corpus_flat_sip_nominal": round(corpus15(sip)),
        "corpus_flat_sip_real": round(real(corpus15(sip))),
        "corpus_stepup10pct_nominal": round(corpus15(sip) * 2.1),
        "corpus_stepup10pct_real": round(real(corpus15(sip) * 2.1)),
        "ltcg_headroom_inr": max(0, 125000 - investor["ltcg_booked_this_year_inr"]),
        "ltcg_harvest_window": market.get("ltcg_harvest_window", False),
    }


# ═══════════════════════════════════════════════════════════════════════════════
#  SECTION 7 — HOME LOAN vs SIP
# ═══════════════════════════════════════════════════════════════════════════════

def analyse_loan_vs_sip(investor: dict) -> dict:
    loan_rate = investor["home_loan_rate_pct"]
    eq_post_tax = 12.0 * 0.875  # ~10.5%

    if eq_post_tax > loan_rate + 1.0:
        verdict = "INVEST"
        reason = f"Equity post-tax ({eq_post_tax:.1f}%) beats loan cost ({loan_rate}%) by {eq_post_tax-loan_rate:.1f}%. Keep investing."
    elif abs(eq_post_tax - loan_rate) <= 1.0:
        verdict = "BALANCED"
        reason = "Returns nearly equal. Prepay if psychological benefit matters."
    else:
        verdict = "PREPAY"
        reason = f"Loan rate ({loan_rate}%) exceeds equity post-tax ({eq_post_tax:.1f}%). Prepay."

    return {
        "verdict": verdict,
        "reasoning": reason,
        "loan_rate_pct": loan_rate,
        "equity_post_tax_pct": round(eq_post_tax, 1),
    }


# ═══════════════════════════════════════════════════════════════════════════════
#  SECTION 8 — PORTFOLIO OVERLAP (name-based heuristics)
# ═══════════════════════════════════════════════════════════════════════════════

def analyse_overlap(portfolio: list) -> dict:
    amc_count: dict = {}
    nifty50, large_cap, small_cap, mid_cap = [], [], [], []
    flags = []

    for f in portfolio:
        name = f.get("fund_name", "").upper()
        amc  = name.split(" ")[0]
        amc_count[amc] = amc_count.get(amc, 0) + 1
        if "NIFTY 50" in name and "NEXT" not in name and "100" not in name:
            nifty50.append(f["fund_name"])
        if any(x in name for x in ["LARGE CAP", "BLUECHIP", "SENSEX", "NIFTY 50"]):
            large_cap.append(f["fund_name"])
        if "SMALL CAP" in name:
            small_cap.append(f["fund_name"])
        if "MID CAP" in name or "MIDCAP" in name:
            mid_cap.append(f["fund_name"])

    for amc, cnt in amc_count.items():
        if cnt >= 3:
            flags.append(f"{cnt} funds from {amc} — single AMC concentration risk")

    if len(nifty50) > 1:
        flags.append(f"{len(nifty50)} Nifty 50 index funds — identical holdings, duplicate fees")

    if len(large_cap) >= 2:
        flags.append(f"{len(large_cap)} large cap/index funds — significant overlap expected")

    risk = "HIGH" if len(flags) >= 2 else "MODERATE" if flags else "LOW"

    return {
        "overlap_risk": risk,
        "flags": flags,
        "nifty50_count": len(nifty50),
        "large_cap_count": len(large_cap),
        "small_cap_count": len(small_cap),
        "mid_cap_count": len(mid_cap),
        "amc_distribution": amc_count,
    }


# ═══════════════════════════════════════════════════════════════════════════════
#  SECTION 9 — ZERODHA KITE FETCHER
# ═══════════════════════════════════════════════════════════════════════════════

def fetch_zerodha_portfolio() -> Optional[list]:
    api_key = os.environ.get("KITE_API_KEY")
    token   = os.environ.get("KITE_ACCESS_TOKEN")
    if not api_key or not token:
        print("[INFO] Kite credentials not set — using dummy portfolio")
        return None

    headers = {"X-Kite-Version": "3", "Authorization": f"token {api_key}:{token}"}

    try:
        resp = requests.get("https://api.kite.trade/mf/holdings", headers=headers, timeout=15)
        resp.raise_for_status()
        holdings = resp.json().get("data", [])
        if not holdings:
            print("[WARN] Zerodha returned empty holdings")
            return None

        # Fetch SIP dates
        sip_dates: dict = {}
        try:
            sr = requests.get("https://api.kite.trade/mf/sips", headers=headers, timeout=15)
            if sr.status_code == 200:
                for s in sr.json().get("data", []):
                    isin, created = s.get("isin", ""), s.get("created", "")
                    if isin and created:
                        d = created[:10]
                        if isin not in sip_dates or d < sip_dates[isin]:
                            sip_dates[isin] = d
        except Exception as e:
            print(f"[WARN] SIP dates: {e}")

        # Fetch order dates
        order_dates: dict = {}
        try:
            orr = requests.get("https://api.kite.trade/mf/orders", headers=headers, timeout=15)
            if orr.status_code == 200:
                for o in orr.json().get("data", []):
                    isin, ts = o.get("isin", ""), o.get("order_timestamp", "")
                    if isin and ts:
                        d = ts[:10]
                        if isin not in order_dates or d < order_dates[isin]:
                            order_dates[isin] = d
        except Exception as e:
            print(f"[WARN] Order dates: {e}")

        fallback = str(datetime.date.today() - datetime.timedelta(days=730))

        portfolio = []
        for h in holdings:
            name  = h.get("fund", "")
            isin  = h.get("isin", "")
            qty   = float(h.get("quantity", 0))
            price = float(h.get("last_price", 0))
            inv   = float(h.get("amount", 0))
            avg   = float(h.get("average_price", 0))
            plan  = "regular" if "REGULAR" in name.upper() else "direct"
            date  = sip_dates.get(isin) or order_dates.get(isin) or fallback

            portfolio.append({
                "fund_name":            name,
                "isin":                 isin,
                "plan":                 plan,
                "units":                qty,
                "avg_nav":              avg,
                "current_nav":          price,
                "invested_inr":         inv,
                "current_value_inr":    round(price * qty, 2),
                "expense_ratio_pct":    0.5 if plan == "direct" else 1.5,
                "exit_load_pct":        1.0,
                "sip_start_date":       date,
                "total_exit_cost_inr":  0,
            })

        print(f"[OK] Zerodha: {len(portfolio)} funds | {len(sip_dates)} SIP dates | {len(order_dates)} order dates")
        return portfolio if portfolio else None

    except Exception as e:
        print(f"[WARN] Zerodha fetch failed: {e} — using dummy")
        return None


# ═══════════════════════════════════════════════════════════════════════════════
#  SECTION 10 — AI PROMPT BUILDER
#  KEY: Portfolio as plain text table, not JSON.
#  ~80 chars/fund vs ~250 for JSON = 4x more funds fit in same token budget.
# ═══════════════════════════════════════════════════════════════════════════════

def build_portfolio_table(portfolio: list) -> str:
    total_inv  = sum(f.get("invested_inr", 0) for f in portfolio)
    total_cur  = sum(f.get("current_value_inr", 0) for f in portfolio)
    total_gain = total_cur - total_inv
    gain_pct   = (total_gain / total_inv * 100) if total_inv > 0 else 0

    lines = [
        f"PORTFOLIO TOTALS: {len(portfolio)} funds | "
        f"Invested Rs {total_inv:,.0f} | "
        f"Current Rs {total_cur:,.0f} | "
        f"Gain Rs {total_gain:,.0f} ({gain_pct:.1f}%)",
        "",
        f"{'No':<3} {'Fund Name':<50} {'Plan':<8} "
        f"{'Invested':>10} {'Current':>10} {'Gain%':>6} "
        f"{'ExitCost':>9} {'SIP-Start'}",
        "-" * 115,
    ]
    for i, f in enumerate(portfolio, 1):
        inv    = f.get("invested_inr", 0)
        cur    = f.get("current_value_inr", 0)
        gp     = ((cur - inv) / inv * 100) if inv > 0 else 0
        ec     = f.get("total_exit_cost_inr", 0)
        lines.append(
            f"{i:<3} {f.get('fund_name','')[:49]:<50} "
            f"{f.get('plan','?'):<8} "
            f"{inv:>10,.0f} "
            f"{cur:>10,.0f} "
            f"{gp:>6.1f}% "
            f"{ec:>9,.0f} "
            f"{f.get('sip_start_date','?')}"
        )
    lines.append("-" * 115)
    return "\n".join(lines)


def build_prompts(
    investor: dict,
    portfolio: list,
    market: dict,
    personal_risk: dict,
    step_up: dict,
    loan_vs_sip: dict,
    overlap: dict,
) -> Tuple[str, str]:
    today      = datetime.date.today().isoformat()
    n          = len(portfolio)
    table      = build_portfolio_table(portfolio)
    total_inv  = sum(f.get("invested_inr", 0) for f in portfolio)
    total_cur  = sum(f.get("current_value_inr", 0) for f in portfolio)

    # ── PROMPT 1: Fund analysis ───────────────────────────────────────────────
    p1 = f"""You are AFundMentor Pro — a brutally honest independent financial analyst.

INVESTOR PROFILE:
Name: Ankit | Age: 34 | City: Bangalore
Salary: Rs 1,40,000/mo | Home loan EMI: Rs 60,000/mo (241mo left at 8.5%)
Monthly SIP: Rs 20,000 | Emergency fund: Rs 3.5L (target Rs 6L) at 7% savings
Role: Technical Manager EV Charging + Thermal Systems (EV sector = career tailwind)
Risk: AGGRESSIVE | Target: 15-year wealth to 2041
Car purchase in 18mo (adds Rs 17,000 EMI) | Tax slab: 30% | LTCG headroom: Rs 1.25L
Personal risk score: {personal_risk['personal_risk_score']}/10
Risk flags: {' | '.join(personal_risk['flags']) if personal_risk['flags'] else 'None'}

MARKET ({today}):
Nifty P/E: {market['nifty50_pe']} ({market['market_valuation_signal']}) | RBI: {market['rbi_repo_rate_pct']}% ({market['rbi_stance']})
Inflation: {market['cpi_inflation_pct']}% | FII: Rs {market['fii_flow_monthly_cr']}Cr | EV sector: {market['ev_sector_sentiment']}

PORTFOLIO OVERLAP:
Risk level: {overlap['overlap_risk']}
Issues: {' | '.join(overlap['flags']) if overlap['flags'] else 'None detected'}
Nifty 50 funds: {overlap['nifty50_count']} | Large cap funds: {overlap['large_cap_count']}
Small cap: {overlap['small_cap_count']} | Mid cap: {overlap['mid_cap_count']}

COMPLETE PORTFOLIO TABLE ({n} funds — rate EVERY fund):
{table}

CRITICAL INSTRUCTIONS:
1. fund_ratings MUST have exactly {n} entries — one for each numbered fund above
2. Use the EXACT fund name from the table (copy it precisely)
3. Use the EXACT invested and current values from the table — do not estimate
4. A switch is only valid if net benefit > 1.5% annualised after ALL exit costs
5. Score mercilessly — you are paid to find problems, not to comfort

Return ONLY valid JSON, no prose, no markdown:

{{
  "master_score": <float 0-10>,
  "master_score_reasoning": "<2-3 sentences — honest, specific>",
  "pillar_scores": {{
    "portfolio_quality": <float 0-10>,
    "cost_intelligence": <float 0-10>,
    "portfolio_construction": <float 0-10>,
    "personal_risk": <float 0-10>,
    "behaviour_strategy": <float 0-10>
  }},
  "fund_ratings": [
    {{
      "fund_name": "<exact name from table>",
      "score": <float 0-10>,
      "verdict": "<HOLD or WATCH or SWITCH or EXIT>",
      "invested_inr": <exact value from table — Rs>,
      "current_value_inr": <exact value from table — Rs>,
      "gain_pct": <float>,
      "expense_ratio_verdict": "<cheap or fair or expensive>",
      "plan_type": "<direct or regular>",
      "regular_plan_annual_loss_inr": <float — 0 if direct>,
      "exit_cost_inr": <from table>,
      "switch_justified": <true or false>,
      "key_finding": "<one brutal honest sentence>",
      "recommended_action": "<specific action with deadline>"
    }}
  ],
  "returns_summary": {{
    "total_invested_inr": <sum from table>,
    "total_current_inr": <sum from table>,
    "total_gain_inr": <calculated>,
    "total_gain_pct": <calculated>,
    "estimated_xirr_pct": <float>,
    "real_return_post_inflation_pct": <xirr - inflation>,
    "vs_nifty50_assessment": "<outperforming/underperforming by X%>",
    "sip_consistency_score": <float 0-10>
  }},
  "cost_analysis": {{
    "total_annual_expense_drag_inr": <float>,
    "regular_plan_funds_count": <int>,
    "regular_plan_total_annual_loss_inr": <float>,
    "regular_plan_15yr_compounding_loss_inr": <float>,
    "top_cost_actions": ["<action 1 with Rs impact>", "<action 2>"]
  }},
  "brutal_honesty_block": [
    "<finding 1 — most important and uncomfortable>",
    "<finding 2>",
    "<finding 3>",
    "<finding 4>"
  ]
}}"""

    # ── PROMPT 2: Strategy & projections ─────────────────────────────────────
    p2 = f"""You are AFundMentor Pro — a brutally honest financial analyst.

INVESTOR: Ankit | Age 34 | Bangalore | Rs 1,40,000/mo salary
Home loan: Rs 60,000/mo EMI | SIP: Rs 20,000/mo | Free cash: Rs 60,000/mo
Emergency fund: Rs 3.5L / Rs 6L target | EV career tailwind
Portfolio: Rs {total_inv:,.0f} invested | Rs {total_cur:,.0f} current value
Horizon: 15 years to 2041 | Tax: 30% slab | LTCG headroom: Rs 1.25L
Today: {today}

STEP-UP ANALYSIS:
Phase {step_up['phase']} — {step_up['phase_label']}
Reasoning: {step_up['reasoning']}
Flat SIP corpus (nominal): Rs {step_up['corpus_flat_sip_nominal']:,}
Flat SIP corpus (real, 6% inflation): Rs {step_up['corpus_flat_sip_real']:,}
With 10% annual step-up (real): Rs {step_up['corpus_stepup10pct_real']:,}
Recommended SIP increase: Rs {step_up['sip_increase_recommended_inr']:,}/mo
Buffer addition: Rs {step_up['buffer_monthly_addition_inr']:,}/mo

LOAN vs SIP: {loan_vs_sip['verdict']} — {loan_vs_sip['reasoning']}
LTCG harvest window open: {'YES' if step_up['ltcg_harvest_window'] else 'NO'}

Return ONLY valid JSON, no prose, no markdown:

{{
  "projection": {{
    "corpus_flat_sip_nominal_inr": {step_up['corpus_flat_sip_nominal']},
    "corpus_flat_sip_real_inr": {step_up['corpus_flat_sip_real']},
    "corpus_stepup10pct_nominal_inr": {step_up['corpus_stepup10pct_nominal']},
    "corpus_stepup10pct_real_inr": {step_up['corpus_stepup10pct_real']},
    "sip_needed_for_1cr_real_inr": <float>,
    "sip_needed_for_2cr_real_inr": <float>,
    "wealth_gap_assessment": "<2 sentence honest assessment with specific Rs numbers>"
  }},
  "stepup_guidance": {{
    "phase": {step_up['phase']},
    "phase_label": "{step_up['phase_label']}",
    "recommended_sip_increase_inr": {step_up['sip_increase_recommended_inr']},
    "buffer_addition_inr": {step_up['buffer_monthly_addition_inr']},
    "april_action": "<specific instruction for April increment — Rs amounts>",
    "reasoning": "{step_up['reasoning']}"
  }},
  "loan_vs_sip_verdict": {{
    "verdict": "{loan_vs_sip['verdict']}",
    "reasoning": "{loan_vs_sip['reasoning']}"
  }},
  "tax_harvest_alert": {{
    "opportunity_exists": {"true" if step_up['ltcg_harvest_window'] else "false"},
    "ltcg_headroom_inr": {step_up['ltcg_headroom_inr']},
    "action": "<specific steps to book LTCG before 31 March>"
  }},
  "career_signal": {{
    "signal": "POSITIVE",
    "ev_status": "<one sentence current EV sector outlook>",
    "income_risk": "low",
    "sip_implication": "<one sentence how this supports investment strategy>"
  }},
  "action_items": [
    {{
      "priority": "<URGENT or ADVISORY or FYI>",
      "action": "<specific action — what, exactly how, by when>",
      "impact_inr": <estimated Rs impact or null>,
      "deadline": "<specific deadline>"
    }}
  ],
  "no_action_needed": false,
  "no_action_reason": "",
  "telegram_summary": "<100 words max. Start with X/10 score. List all fund verdicts briefly. Top 2 actions with Rs impact. 15yr real corpus. No emojis. Brutally direct.>"
}}"""

    return p1, p2


# ═══════════════════════════════════════════════════════════════════════════════
#  SECTION 11 — AI CALLERS
#  CASCADE: OpenAI GPT-4o-mini → Gemini 1.5 Flash → Groq Llama 3.3 70B
# ═══════════════════════════════════════════════════════════════════════════════

SYSTEM_MSG = (
    "You are AFundMentor Pro — a brutally honest financial analyst. "
    "Return ONLY valid JSON. No markdown fences. No prose. "
    "All monetary figures in Indian Rupees. "
    "Mission: maximum wealth through honest, specific advice."
)


def _post(url: str, headers: dict, payload: dict, label: str) -> Optional[requests.Response]:
    """Single HTTP POST with one retry on 429."""
    for attempt in range(2):
        try:
            print(f"      [{label}] attempt {attempt+1}/2...")
            r = requests.post(url, headers=headers, json=payload, timeout=120)
            if r.status_code == 429:
                wait = 45 * (attempt + 1)
                print(f"      [{label}] rate limited — waiting {wait}s...")
                time.sleep(wait)
                continue
            r.raise_for_status()
            return r
        except requests.exceptions.Timeout:
            print(f"      [{label}] timeout")
        except requests.exceptions.RequestException as e:
            print(f"      [{label}] error: {e}")
        if attempt == 0:
            time.sleep(15)
    return None


def _parse(raw: str) -> Optional[dict]:
    """Simple JSON parser — no repair logic."""
    if not raw:
        return None
    text = raw.strip()
    if "```" in text:
        for part in text.split("```"):
            p = part.strip()
            if p.startswith("json"):
                text = p[4:].strip()
                break
            elif p.startswith("{"):
                text = p
                break
    s = text.find("{")
    e = text.rfind("}") + 1
    if s == -1 or e <= s:
        return None
    try:
        return json.loads(text[s:e])
    except json.JSONDecodeError as ex:
        print(f"      [WARN] JSON parse error: {ex}")
        return None


def call_openai(p1: str, p2: str) -> Optional[dict]:
    key = os.environ.get("OPENAI_API_KEY")
    if not key:
        print("      [OpenAI] key not set — skipping")
        return None

    url = "https://api.openai.com/v1/chat/completions"
    hdrs = {"Authorization": f"Bearer {key}", "Content-Type": "application/json"}

    def call(prompt: str, label: str) -> Optional[dict]:
        r = _post(url, hdrs, {
            "model": "gpt-4o-mini",
            "messages": [
                {"role": "system", "content": SYSTEM_MSG},
                {"role": "user",   "content": prompt},
            ],
            "temperature": 0.1,
            "max_tokens": 8192,
            "response_format": {"type": "json_object"},
        }, f"OpenAI {label}")
        if r is None:
            return None
        try:
            return _parse(r.json()["choices"][0]["message"]["content"])
        except Exception as ex:
            print(f"      [OpenAI {label}] parse error: {ex}")
            return None

    print("      Call 1 (fund analysis) — OpenAI...")
    r1 = call(p1, "C1")
    if not r1:
        return None
    print("      Waiting 12s...")
    time.sleep(12)
    print("      Call 2 (strategy) — OpenAI...")
    r2 = call(p2, "C2")
    if not r2:
        return None
    merged = {**r1, **r2}
    print(f"      [OK] OpenAI: {len(merged.get('fund_ratings',[]))} funds rated")
    return merged


def call_gemini(p1: str, p2: str) -> Optional[dict]:
    key = os.environ.get("GEMINI_API_KEY")
    if not key:
        print("      [Gemini] key not set — skipping")
        return None

    # gemini-1.5-flash: better free tier limits than flash-lite
    url  = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key={key}"
    hdrs = {"Content-Type": "application/json"}

    def call(prompt: str, label: str) -> Optional[dict]:
        r = _post(url, hdrs, {
            "contents": [{"parts": [{"text": prompt}]}],
            "generationConfig": {
                "temperature": 0.1,
                "maxOutputTokens": 8192,
                "responseMimeType": "application/json",
            },
            "systemInstruction": {"parts": [{"text": SYSTEM_MSG}]},
        }, f"Gemini {label}")
        if r is None:
            return None
        try:
            text = r.json()["candidates"][0]["content"]["parts"][0]["text"].strip()
            return _parse(text)
        except Exception as ex:
            print(f"      [Gemini {label}] parse error: {ex}")
            return None

    print("      Call 1 (fund analysis) — Gemini 1.5 Flash...")
    r1 = call(p1, "C1")
    if not r1:
        return None
    print("      Waiting 15s...")
    time.sleep(15)
    print("      Call 2 (strategy) — Gemini 1.5 Flash...")
    r2 = call(p2, "C2")
    if not r2:
        return None
    merged = {**r1, **r2}
    print(f"      [OK] Gemini: {len(merged.get('fund_ratings',[]))} funds rated")
    return merged


def call_groq(p1: str, p2: str) -> Optional[dict]:
    key = os.environ.get("GROQ_API_KEY")
    if not key:
        print("      [Groq] key not set — skipping")
        return None

    url  = "https://api.groq.com/openai/v1/chat/completions"
    hdrs = {"Authorization": f"Bearer {key}", "Content-Type": "application/json"}

    def call(prompt: str, label: str) -> Optional[dict]:
        r = _post(url, hdrs, {
            "model": "llama-3.3-70b-versatile",
            "messages": [
                {"role": "system", "content": SYSTEM_MSG},
                {"role": "user",   "content": prompt},
            ],
            "temperature": 0.1,
            "max_tokens": 8192,
        }, f"Groq {label}")
        if r is None:
            return None
        try:
            return _parse(r.json()["choices"][0]["message"]["content"])
        except Exception as ex:
            print(f"      [Groq {label}] parse error: {ex}")
            return None

    print("      Call 1 (fund analysis) — Groq Llama 3.3 70B...")
    r1 = call(p1, "C1")
    if not r1:
        return None
    print("      Waiting 15s...")
    time.sleep(15)
    print("      Call 2 (strategy) — Groq Llama 3.3 70B...")
    r2 = call(p2, "C2")
    if not r2:
        return None
    merged = {**r1, **r2}
    print(f"      [OK] Groq: {len(merged.get('fund_ratings',[]))} funds rated")
    return merged


# ═══════════════════════════════════════════════════════════════════════════════
#  SECTION 12 — REPORT VALIDATOR
# ═══════════════════════════════════════════════════════════════════════════════

def validate_report(report: dict, expected_funds: int) -> Tuple[bool, list]:
    issues = []
    for key in ["master_score", "pillar_scores", "fund_ratings",
                "returns_summary", "cost_analysis", "brutal_honesty_block",
                "projection", "stepup_guidance", "action_items"]:
        if key not in report:
            issues.append(f"Missing key: {key}")

    if "fund_ratings" in report:
        actual = len(report["fund_ratings"])
        if actual < expected_funds:
            issues.append(f"Only {actual}/{expected_funds} funds rated")

    if "master_score" in report:
        try:
            s = float(report["master_score"])
            if not 0 <= s <= 10:
                issues.append(f"master_score {s} out of range")
        except (ValueError, TypeError):
            issues.append("master_score not numeric")

    return len(issues) == 0, issues


# ═══════════════════════════════════════════════════════════════════════════════
#  SECTION 13 — TELEGRAM FORMATTER
# ═══════════════════════════════════════════════════════════════════════════════

def format_telegram(report: dict, ai_engine: str, dashboard_url: str = "") -> str:
    today     = datetime.date.today().strftime("%d %b %Y")
    score     = report.get("master_score", 0)
    no_action = report.get("no_action_needed", False)

    def bar(s) -> str:
        try:
            f = int(round(float(s)))
            return "█" * f + "░" * (10 - f)
        except Exception:
            return "░" * 10

    def grade(s) -> str:
        s = float(s) if s else 0
        if s >= 8: return "STRONG"
        if s >= 6: return "ADEQUATE"
        if s >= 4: return "WEAK"
        return "CRITICAL"

    def vicon(v) -> str:
        return {"HOLD": "✅", "WATCH": "⚠️", "SWITCH": "🔄", "EXIT": "❌"}.get(v, "•")

    def picon(p) -> str:
        return {"URGENT": "🔴", "ADVISORY": "🟡", "FYI": "🟢"}.get(p, "•")

    def wrap(text: str, width: int = 44, pad: str = "    ") -> str:
        words, lines, line = str(text).split(), [], ""
        for w in words:
            if len(line) + len(w) + 1 <= width:
                line = (line + " " + w).strip()
            else:
                if line:
                    lines.append(pad + line)
                line = w
        if line:
            lines.append(pad + line)
        return "\n".join(lines)

    L = [
        "━━━━━━━━━━━━━━━━━━━━━━━━━",
        "  💼 AFundMentor Pro",
        f"  {today}  |  Monthly Report",
        f"  AI: {ai_engine}",
        "━━━━━━━━━━━━━━━━━━━━━━━━━",
        "",
        "📊  PORTFOLIO SCORE",
        f"    {score}/10  {bar(score)}",
        f"    {grade(score)}",
        "",
        wrap(str(report.get("master_score_reasoning", ""))[:140], pad="    "),
        "",
    ]

    if no_action:
        L += ["─────────────────────────", "✅  NO ACTION NEEDED", "",
              wrap(str(report.get("no_action_reason", ""))[:200], pad="    "), ""]
    else:
        # Pillars
        p = report.get("pillar_scores", {})
        if p:
            L += [
                "─────────────────────────", "📐  PILLARS",
                f"    Quality    {p.get('portfolio_quality','-')}/10  {bar(p.get('portfolio_quality',0))}",
                f"    Cost       {p.get('cost_intelligence','-')}/10  {bar(p.get('cost_intelligence',0))}",
                f"    Structure  {p.get('portfolio_construction','-')}/10  {bar(p.get('portfolio_construction',0))}",
                f"    Risk       {p.get('personal_risk','-')}/10  {bar(p.get('personal_risk',0))}",
                f"    Strategy   {p.get('behaviour_strategy','-')}/10  {bar(p.get('behaviour_strategy',0))}",
                "",
            ]

        # Funds
        funds = report.get("fund_ratings", [])
        if funds:
            L += ["─────────────────────────", f"🏦  FUNDS ({len(funds)} rated)"]
            for f in funds:
                inv = f.get("invested_inr", 0)
                cur = f.get("current_value_inr", 0)
                L += [
                    "",
                    f"  {vicon(f.get('verdict',''))}  {f.get('fund_name','')[:32]}",
                    f"      {f.get('score','-')}/10 {f.get('verdict','')} | "
                    f"Inv Rs {inv:,.0f} | Cur Rs {cur:,.0f}",
                ]
                if f.get("key_finding"):
                    L.append(wrap(f["key_finding"], width=44, pad="      "))
            L.append("")

        # Returns
        ret = report.get("returns_summary", {})
        if ret:
            L += [
                "─────────────────────────", "📈  YOUR RETURNS",
                f"    Invested    : Rs {ret.get('total_invested_inr', 0):,.0f}",
                f"    Current     : Rs {ret.get('total_current_inr', 0):,.0f}",
                f"    Gain        : Rs {ret.get('total_gain_inr', 0):,.0f} ({ret.get('total_gain_pct',0):.1f}%)",
                f"    XIRR (est.) : {ret.get('estimated_xirr_pct', '-')}%",
                f"    Real return : {ret.get('real_return_post_inflation_pct', '-')}%",
                f"    vs Nifty 50 : {ret.get('vs_nifty50_assessment', '-')}",
                "",
            ]

        # Brutal findings
        brutal = report.get("brutal_honesty_block", [])
        if brutal:
            L += ["─────────────────────────", "🔍  BRUTAL FINDINGS", ""]
            for i, f in enumerate(brutal[:4], 1):
                L.append(f"  {i}.")
                L.append(wrap(f, width=44, pad="     "))
                L.append("")

        # Actions
        actions = report.get("action_items", [])
        if actions:
            L += ["─────────────────────────", "⚡  ACTIONS", ""]
            for item in actions:
                pri = item.get("priority", "FYI")
                L.append(f"  {picon(pri)}  [{pri}]")
                L.append(wrap(item.get("action", ""), width=44, pad="     "))
                if item.get("impact_inr"):
                    L.append(f"     Impact : Rs {item['impact_inr']:,.0f}")
                if item.get("deadline"):
                    L.append(f"     By     : {item['deadline']}")
                L.append("")

        # Projection
        proj = report.get("projection", {})
        if proj:
            L += [
                "─────────────────────────",
                "🎯  15-YEAR PROJECTION (real Rs)",
                "",
                f"    Flat Rs 20k SIP : Rs {proj.get('corpus_flat_sip_real_inr',0):,.0f}",
                f"    10% annual stpup: Rs {proj.get('corpus_stepup10pct_real_inr',0):,.0f}",
                "",
                wrap(proj.get("wealth_gap_assessment", ""), width=44, pad="    "),
                "",
            ]

        # Step-up
        su = report.get("stepup_guidance", {})
        if su and su.get("recommended_sip_increase_inr", 0) > 0:
            L += [
                "─────────────────────────",
                f"📅  APRIL STEP-UP — Ph{su.get('phase','-')}: {su.get('phase_label','')}",
                f"    SIP +   : Rs {su.get('recommended_sip_increase_inr',0):,.0f}/mo",
                f"    Buffer +: Rs {su.get('buffer_addition_inr',0):,.0f}/mo",
                "",
                wrap(su.get("april_action", ""), width=44, pad="    "),
                "",
            ]

        # Loan vs SIP
        lsip = report.get("loan_vs_sip_verdict", {})
        if lsip:
            L += [
                "─────────────────────────",
                f"🏠  LOAN vs SIP : {lsip.get('verdict','')}",
                "",
                wrap(lsip.get("reasoning", ""), width=44, pad="    "),
                "",
            ]

        # Tax harvest
        tax = report.get("tax_harvest_alert", {})
        if tax.get("opportunity_exists"):
            L += [
                "─────────────────────────",
                f"💰  TAX HARVEST — Rs {tax.get('ltcg_headroom_inr',0):,.0f} tax-free",
                "",
                wrap(tax.get("action", ""), width=44, pad="    "),
                "",
            ]

        # Career
        career = report.get("career_signal", {})
        if career:
            sig  = career.get("signal", "")
            sico = {"POSITIVE": "🟢", "NEUTRAL": "🟡", "NEGATIVE": "🔴"}.get(sig, "•")
            L += [
                "─────────────────────────",
                f"{sico}  CAREER : {sig}",
                "",
                wrap(career.get("ev_status", ""), width=44, pad="    "),
                f"    Income risk : {career.get('income_risk', '')}",
                "",
            ]

        # Cost
        cost = report.get("cost_analysis", {})
        if cost:
            L += [
                "─────────────────────────",
                "💸  COST DRAIN",
                f"    Annual drag       : Rs {cost.get('total_annual_expense_drag_inr',0):,.0f}",
                f"    Regular plan loss : Rs {cost.get('regular_plan_total_annual_loss_inr',0):,.0f}/yr",
                f"    15yr impact       : Rs {cost.get('regular_plan_15yr_compounding_loss_inr',0):,.0f}",
                "",
            ]

    # Footer
    next_month = (
        datetime.date.today().replace(day=1) + datetime.timedelta(days=32)
    ).replace(day=1) - datetime.timedelta(days=1)

    L.append("━━━━━━━━━━━━━━━━━━━━━━━━━")
    if dashboard_url:
        L += ["📱  Full Dashboard", f"    {dashboard_url}", ""]
    L += [
        f"    Next report : {next_month.strftime('%d %b %Y')}",
        f"    AFundMentor Pro v3.0 | {ai_engine}",
        "━━━━━━━━━━━━━━━━━━━━━━━━━",
    ]

    msg = "\n".join(L)
    if len(msg) > 4000:
        msg = msg[:3950] + "\n\n    ... (see full dashboard)"
    return msg


# ═══════════════════════════════════════════════════════════════════════════════
#  SECTION 14 — TELEGRAM SENDER
# ═══════════════════════════════════════════════════════════════════════════════

def send_telegram(message: str) -> bool:
    token   = os.environ.get("TELEGRAM_BOT_TOKEN")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        print("[WARN] Telegram not configured — printing to console")
        print("\n" + "=" * 60)
        print(message)
        print("=" * 60 + "\n")
        return False
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat_id, "text": message},
            timeout=15,
        )
        r.raise_for_status()
        print("[OK] Telegram sent")
        return True
    except Exception as e:
        print(f"[ERROR] Telegram failed: {e}")
        return False


# ═══════════════════════════════════════════════════════════════════════════════
#  SECTION 15 — REPORT SAVER
# ═══════════════════════════════════════════════════════════════════════════════

def save_report(report: dict, portfolio: list, market: dict, ai_engine: str) -> str:
    os.makedirs("reports", exist_ok=True)
    fname = f"reports/report_{datetime.date.today().strftime('%Y_%m')}.json"
    with open(fname, "w", encoding="utf-8") as f:
        json.dump({
            "meta": {
                "generated_at":   str(datetime.datetime.now()),
                "engine_version": "3.0.0",
                "ai_engine":      ai_engine,
                "investor":       "Ankit",
                "fund_count":     len(portfolio),
            },
            "report":             report,
            "portfolio_snapshot": portfolio,
            "market_snapshot":    market,
        }, f, indent=2, ensure_ascii=False)
    print(f"[OK] Saved: {fname}")
    return fname


def load_score_history() -> list:
    if not os.path.exists("reports"):
        return []
    history = []
    for fname in sorted(os.listdir("reports")):
        if fname.startswith("report_") and fname.endswith(".json"):
            try:
                with open(os.path.join("reports", fname), encoding="utf-8") as f:
                    d = json.load(f)
                history.append({
                    "month":     fname.replace("report_", "").replace(".json", ""),
                    "score":     d["report"].get("master_score"),
                    "ai_engine": d.get("meta", {}).get("ai_engine", "N/A"),
                })
            except Exception:
                continue
    return history[-12:]


# ═══════════════════════════════════════════════════════════════════════════════
#  SECTION 16 — GITHUB PAGES PUBLISHER
# ═══════════════════════════════════════════════════════════════════════════════

def publish_dashboard(
    report: dict, portfolio: list, market: dict, ai_engine: str
) -> str:
    today    = datetime.date.today()
    docs_dir = "docs"
    os.makedirs(docs_dir, exist_ok=True)

    if not os.path.exists("dashboard.html"):
        print("[WARN] dashboard.html missing — skipping Pages publish")
        return os.environ.get("DASHBOARD_URL", "")

    with open("dashboard.html", encoding="utf-8") as f:
        template = f.read()

    payload = {
        "report":             report,
        "portfolio_snapshot": portfolio,
        "market_snapshot":    market,
        "ai_engine":          ai_engine,
        "generated_at":       str(datetime.datetime.now()),
        "fund_count":         len(portfolio),
    }

    injection = (
        f"<script>\nwindow.REPORT_DATA = "
        f"{json.dumps(payload, ensure_ascii=False)};\n</script>\n"
    )
    html = template.replace("</body>", injection + "\n</body>")

    date_str = today.strftime("%Y_%m")
    index_path = os.path.join(docs_dir, "index.html")
    with open(index_path, "w", encoding="utf-8") as f:
        f.write(html)
    shutil.copy(index_path, os.path.join(docs_dir, f"report_{date_str}.html"))

    # Archive index
    archives = sorted(
        [x for x in os.listdir(docs_dir) if x.startswith("report_") and x.endswith(".html")],
        reverse=True,
    )
    with open(os.path.join(docs_dir, "archive.html"), "w", encoding="utf-8") as f:
        f.write(
            "<!DOCTYPE html><html><head><meta charset='UTF-8'>"
            "<title>AFundMentor Pro</title>"
            "<style>body{font-family:monospace;background:#0b0b10;color:#dcdce8;"
            "padding:40px;max-width:500px;margin:0 auto;}"
            "h1{color:#b8ff4e;}a{color:#4eb8ff;text-decoration:none;"
            "display:block;padding:10px 0;border-bottom:1px solid rgba(255,255,255,.06);}"
            "a:hover{color:#fff;}.l{color:#b8ff4e;}</style></head><body>"
            "<h1>AFundMentor Pro</h1><p>Monthly Reports</p>"
            + "".join(
                f'<a href="{x}" class="{"l" if i==0 else ""}">'
                f'{"★ " if i==0 else ""}'
                f'{x.replace("report_","").replace(".html","").replace("_"," ")}</a>'
                for i, x in enumerate(archives)
            )
            + "</body></html>"
        )

    github_user  = os.environ.get("GITHUB_USERNAME", "akiyadav")
    github_repo  = os.environ.get("GITHUB_REPO", "mf-advisor")
    github_token = os.environ.get("GITHUB_TOKEN", "")

    try:
        subprocess.run(["git", "config", "user.email", "bot@afundmentor.com"], check=True)
        subprocess.run(["git", "config", "user.name",  "AFundMentor Bot"],     check=True)
        if github_token:
            subprocess.run([
                "git", "remote", "set-url", "origin",
                f"https://{github_token}@github.com/{github_user}/{github_repo}.git",
            ], check=True)
        subprocess.run(["git", "add", "docs/"], check=True)
        subprocess.run(["git", "commit", "-m", f"report {today} ({ai_engine})"], check=True)
        subprocess.run(["git", "push", "origin", "main"], check=True)
        url = f"https://{github_user}.github.io/{github_repo}/"
        print(f"[OK] Dashboard: {url}")
        return url
    except subprocess.CalledProcessError as e:
        print(f"[WARN] Git push failed: {e}")
        return f"https://{github_user}.github.io/{github_repo}/"


# ═══════════════════════════════════════════════════════════════════════════════
#  SECTION 17 — MAIN ORCHESTRATOR
# ═══════════════════════════════════════════════════════════════════════════════

def main() -> None:
    global _ai_engine_used

    print("\n" + "=" * 62)
    print("  AFUNDMENTOR PRO v3.0 — MONTHLY RUN")
    print(f"  {datetime.datetime.now().strftime('%d %B %Y, %H:%M IST')}")
    print("=" * 62 + "\n")

    # 1. Portfolio
    print("[1/8] Loading portfolio...")
    portfolio = fetch_zerodha_portfolio() or DUMMY_PORTFOLIO
    print(f"      {len(portfolio)} funds loaded")

    # 2. Market
    print("[2/8] Market context...")
    market = fetch_market_context()
    print(f"      Nifty P/E: {market['nifty50_pe']} | RBI: {market['rbi_stance']} | EV: {market['ev_sector_sentiment']}")

    # 3. Exit costs
    print("[3/8] Exit costs...")
    for fund in portfolio:
        cost = calculate_exit_cost(fund, INVESTOR_PROFILE)
        fund["total_exit_cost_inr"] = cost["total_exit_cost_inr"]
        print(f"      {fund['fund_name'][:42]}: Rs {cost['total_exit_cost_inr']:,.0f} ({cost['tax_type']})")

    # 4. Personal risk
    print("[4/8] Personal risk...")
    personal_risk = score_personal_risk(INVESTOR_PROFILE, market)
    print(f"      Score: {personal_risk['personal_risk_score']}/10 | Flags: {len(personal_risk['flags'])}")

    # 5. Step-up
    print("[5/8] Step-up guidance...")
    step_up = calculate_stepup_guidance(INVESTOR_PROFILE, personal_risk, market)
    print(f"      Phase {step_up['phase']} ({step_up['phase_label']}) | +Rs {step_up['sip_increase_recommended_inr']:,}/mo recommended")

    # 6. Loan vs SIP
    print("[6/8] Loan vs SIP...")
    loan_vs_sip = analyse_loan_vs_sip(INVESTOR_PROFILE)
    print(f"      Verdict: {loan_vs_sip['verdict']}")

    # 7. Overlap
    print("[7/8] Overlap analysis...")
    overlap = analyse_overlap(portfolio)
    print(f"      Risk: {overlap['overlap_risk']} | Flags: {len(overlap['flags'])}")
    for flag in overlap["flags"]:
        print(f"      > {flag}")

    # 8. Build prompts
    print("[8/8] Running AI analysis...")
    p1, p2 = build_prompts(INVESTOR_PROFILE, portfolio, market, personal_risk, step_up, loan_vs_sip, overlap)
    print(f"      Prompt 1: {len(p1):,} chars | Prompt 2: {len(p2):,} chars")
    print(f"      Portfolio table covers all {len(portfolio)} funds")

    # AI cascade
    report    = None
    ai_engine = "N/A"

    print("\n      [1/3] OpenAI GPT-4o-mini...")
    report = call_openai(p1, p2)
    if report:
        ai_engine = "OpenAI GPT-4o-mini"

    if not report:
        print("\n      [2/3] Gemini 1.5 Flash...")
        report = call_gemini(p1, p2)
        if report:
            ai_engine = "Gemini 1.5 Flash"

    if not report:
        print("\n      [3/3] Groq Llama 3.3 70B...")
        report = call_groq(p1, p2)
        if report:
            ai_engine = "Groq Llama 3.3 70B"

    if not report:
        print("[ERROR] All AI engines failed — aborting")
        return

    _ai_engine_used = ai_engine

    # Validate
    valid, issues = validate_report(report, len(portfolio))
    funds_rated = len(report.get("fund_ratings", []))
    print(f"\n      Score     : {report.get('master_score')}/10")
    print(f"      Funds     : {funds_rated}/{len(portfolio)} rated")
    print(f"      Actions   : {len(report.get('action_items', []))}")
    print(f"      AI engine : {ai_engine}")
    if not valid:
        print(f"      WARNINGS  : {issues}")

    # Save
    save_report(report, portfolio, market, ai_engine)

    # Publish
    print("\n[Publishing dashboard...]")
    dashboard_url = publish_dashboard(report, portfolio, market, ai_engine)

    # Telegram
    print("[Sending Telegram...]")
    msg = format_telegram(report, ai_engine, dashboard_url)
    send_telegram(msg)

    # Trend
    history = load_score_history()
    if len(history) > 1:
        prev  = history[-2].get("score", 0)
        curr  = report.get("master_score", 0)
        trend = "UP" if curr > prev else "DOWN" if curr < prev else "FLAT"
        print(f"\n  Score trend: {prev} -> {curr} ({trend})")

    print("\n" + "=" * 62)
    print(f"  COMPLETE | Engine: {ai_engine} | Funds: {funds_rated}/{len(portfolio)}")
    print("=" * 62 + "\n")


if __name__ == "__main__":
    main()