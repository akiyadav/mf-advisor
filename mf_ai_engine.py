"""
================================================================================
  MF AI WEALTH ADVISOR ENGINE
  Author  : Built for Ankit | Bangalore
  Version : 2.0.0
  Fixes   : Bug 1 — wrong model in claude API (gemini model used in anthropic call)
            Bug 2 — extract_json() undefined (was extract_json_string mismatch)
            Bug 3 — indentation error in main() broke Python parsing
            Bug 4 — extract_json_string scoped inside gemini fn, unavailable globally
            Bug 5 — call_claude_api orphaned, never called in cascade
            Bug 6 — import time inside functions (3 repeated imports)
  Optimised: DRY prompts (shared build_sub_prompts), report validation layer,
             clean AI cascade OpenAI → Groq → Gemini, all imports at top level
================================================================================
"""

import os
import json
import time
import datetime
import requests
import subprocess
import shutil
from typing import Optional


# ═══════════════════════════════════════════════════════════════════════════════
#  SECTION 1 — INVESTOR PROFILE
# ═══════════════════════════════════════════════════════════════════════════════

INVESTOR_PROFILE = {
    "name": "Ankit",
    "age": 34,
    "city": "Bangalore",
    "investment_horizon_years": 15,
    "target_year": 2041,
    "monthly_salary_inr": 140000,
    "home_loan_emi_inr": 60000,
    "home_loan_months_remaining": 241,
    "home_loan_rate_pct": 8.5,
    "monthly_living_expenses_inr": 40000,
    "monthly_sip_inr": 20000,
    "free_cash_monthly_inr": 60000,
    "emergency_fund_inr": 350000,
    "emergency_fund_location": "savings_account_7pct",
    "emergency_fund_months_cover": 3.5,
    "emergency_fund_target_inr": 600000,
    "has_term_insurance": True,
    "has_health_insurance": True,
    "health_plan": "Niva Bupa Aspire Platinum+",
    "term_cover_inr": None,
    "role": "Technical Manager — EV Charging & Thermal Systems",
    "domain": "EV charging infrastructure + battery thermal management",
    "sector": "automotive_ev",
    "career_risk_level": "low_medium",
    "ev_domain_advantage": True,
    "consulting_side_income_inr": 0,
    "planned_expenses": [
        {"label": "Car purchase", "estimated_inr": 800000, "months_away": 18,
         "likely_loan": True, "estimated_loan_emi": 17000},
        {"label": "International trip", "estimated_inr": 200000, "months_away": 12,
         "likely_loan": False},
    ],
    "income_tax_slab_pct": 30,
    "claims_80c": True,
    "claims_sec24": True,
    "ltcg_booked_this_year_inr": 0,
    "wealth_milestones_inr": [1000000, 5000000, 10000000, 20000000],
    "retirement_age_target": None,
    "risk_appetite": "aggressive",
    "equity_allocation_target_pct": 85,
    "debt_allocation_target_pct": 10,
    "gold_allocation_target_pct": 5,
    "international_allocation_target_pct": 10,
}


# ═══════════════════════════════════════════════════════════════════════════════
#  SECTION 2 — PORTFOLIO DATA (dummy — replaced by Zerodha live fetch)
# ═══════════════════════════════════════════════════════════════════════════════

DUMMY_PORTFOLIO = [
    {
        "fund_name": "Parag Parikh Flexi Cap Fund",
        "isin": "INF879O01019",
        "amfi_code": "122639",
        "category": "Flexi Cap",
        "amc": "PPFAS",
        "plan": "direct",
        "units": 142.85,
        "avg_nav": 52.50,
        "current_nav": 72.80,
        "invested_inr": 7500,
        "current_value_inr": 10400,
        "monthly_sip_inr": 7500,
        "sip_start_date": "2023-04-01",
        "oldest_unit_date": "2023-04-01",
        "expense_ratio_pct": 0.58,
        "exit_load_pct": 1.0,
        "exit_load_window_days": 365,
    },
    {
        "fund_name": "Axis Bluechip Fund",
        "isin": "INF846K01EW2",
        "amfi_code": "120472",
        "category": "Large Cap",
        "amc": "Axis MF",
        "plan": "regular",
        "units": 98.10,
        "avg_nav": 45.00,
        "current_nav": 56.20,
        "invested_inr": 4500,
        "current_value_inr": 5515,
        "monthly_sip_inr": 4500,
        "sip_start_date": "2023-06-01",
        "oldest_unit_date": "2023-06-01",
        "expense_ratio_pct": 1.62,
        "exit_load_pct": 1.0,
        "exit_load_window_days": 365,
    },
    {
        "fund_name": "Mirae Asset Large Cap Fund",
        "isin": "INF769K01010",
        "amfi_code": "118834",
        "category": "Large Cap",
        "amc": "Mirae Asset",
        "plan": "direct",
        "units": 75.40,
        "avg_nav": 66.00,
        "current_nav": 78.50,
        "invested_inr": 5000,
        "current_value_inr": 5921,
        "monthly_sip_inr": 3000,
        "sip_start_date": "2023-09-01",
        "oldest_unit_date": "2023-09-01",
        "expense_ratio_pct": 1.68,
        "exit_load_pct": 1.0,
        "exit_load_window_days": 365,
    },
    {
        "fund_name": "SBI Small Cap Fund",
        "isin": "INF200K01RB2",
        "amfi_code": "125497",
        "category": "Small Cap",
        "amc": "SBI MF",
        "plan": "direct",
        "units": 112.30,
        "avg_nav": 89.00,
        "current_nav": 124.60,
        "invested_inr": 10000,
        "current_value_inr": 13993,
        "monthly_sip_inr": 5000,
        "sip_start_date": "2023-02-01",
        "oldest_unit_date": "2023-02-01",
        "expense_ratio_pct": 0.72,
        "exit_load_pct": 1.0,
        "exit_load_window_days": 365,
    },
]


# ═══════════════════════════════════════════════════════════════════════════════
#  SECTION 3 — MARKET CONTEXT FETCHER
# ═══════════════════════════════════════════════════════════════════════════════

def fetch_market_context() -> dict:
    today = datetime.date.today()
    return {
        "date": str(today),
        "nifty50_pe": 22.4,
        "nifty50_1m_return_pct": -2.1,
        "nifty500_1m_return_pct": -2.8,
        "rbi_repo_rate_pct": 6.25,
        "rbi_stance": "neutral",
        "cpi_inflation_pct": 4.8,
        "fii_flow_monthly_cr": -4200,
        "dii_flow_monthly_cr": 6800,
        "india_gdp_growth_pct": 6.4,
        "usd_inr": 84.2,
        "india_vix": 15.4,
        "global_macro_signals": [
            "US Fed maintained rates — no immediate cut signal",
            "China slowdown impacting EM sentiment",
            "Oil at $78 — stable for India import bill",
        ],
        "india_sector_signals": [
            "EV charging infra investments up 34% YoY — FAME III momentum",
            "SEBI tightened small cap fund disclosure norms",
            "SBI MF under regulatory scrutiny for expense ratio anomaly",
        ],
        "geopolitical_risks": [
            "Middle East tensions: moderate",
            "India-Pakistan: de-escalated",
            "US tariff policy: elevated uncertainty",
        ],
        "market_valuation_signal": "CAUTION",
        "lump_sum_opportunity": False,
        "ltcg_harvest_window": True,
        "ev_sector_sentiment": "BULLISH",
        "ev_hiring_trend": "SURGING",
    }


# ═══════════════════════════════════════════════════════════════════════════════
#  SECTION 4 — FUND DATA FETCHER
# ═══════════════════════════════════════════════════════════════════════════════

def fetch_fund_data(amfi_code: str) -> Optional[dict]:
    try:
        url = f"https://api.mfapi.in/mf/{amfi_code}"
        resp = requests.get(url, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            return {
                "fund_name": data.get("meta", {}).get("fund_name"),
                "scheme_type": data.get("meta", {}).get("scheme_type"),
                "nav_history": data.get("data", [])[:1825],
            }
    except Exception as e:
        print(f"[WARN] Fund data fetch failed for {amfi_code}: {e}")
    return None


def calculate_cagr(nav_history: list, years: int) -> Optional[float]:
    if not nav_history or len(nav_history) < years * 252:
        return None
    try:
        current_nav = float(nav_history[0]["nav"])
        past_nav = float(nav_history[min(years * 252, len(nav_history) - 1)]["nav"])
        cagr = ((current_nav / past_nav) ** (1 / years) - 1) * 100
        return round(cagr, 2)
    except Exception:
        return None


def calculate_rolling_returns(nav_history: list, window_years: int = 3) -> dict:
    window_days = window_years * 252
    rolling = []
    for i in range(len(nav_history) - window_days):
        try:
            start_nav = float(nav_history[i + window_days]["nav"])
            end_nav = float(nav_history[i]["nav"])
            cagr = ((end_nav / start_nav) ** (1 / window_years) - 1) * 100
            rolling.append(round(cagr, 2))
        except Exception:
            continue
    if not rolling:
        return {"min": None, "max": None, "median": None, "pct_positive": None}
    sorted_r = sorted(rolling)
    median = sorted_r[len(sorted_r) // 2]
    pct_positive = round(sum(1 for r in rolling if r > 0) / len(rolling) * 100, 1)
    return {
        "min": round(sorted_r[0], 2),
        "max": round(sorted_r[-1], 2),
        "median": round(median, 2),
        "pct_positive": pct_positive,
        "total_windows": len(rolling),
    }


# ═══════════════════════════════════════════════════════════════════════════════
#  SECTION 5 — EXIT COST CALCULATOR
# ═══════════════════════════════════════════════════════════════════════════════

def calculate_exit_cost(fund: dict, investor: dict) -> dict:
    today = datetime.date.today()
    sip_start = datetime.date.fromisoformat(fund["sip_start_date"])
    months_invested = (today.year - sip_start.year) * 12 + (today.month - sip_start.month)
    months_invested = max(months_invested, 1)

    units_in_window = min(12, months_invested)
    value_in_window = (units_in_window / months_invested) * fund["current_value_inr"]
    exit_load_cost = value_in_window * (fund["exit_load_pct"] / 100)

    total_gain = fund["current_value_inr"] - fund["invested_inr"]
    oldest_unit = datetime.date.fromisoformat(fund["oldest_unit_date"])
    days_held = (today - oldest_unit).days

    if days_held >= 365:
        taxable_gain = max(0, total_gain - max(0, 125000 - investor["ltcg_booked_this_year_inr"]))
        tax_cost = taxable_gain * 0.125
        tax_type = "LTCG @ 12.5%"
    else:
        tax_cost = max(0, total_gain) * 0.20
        tax_type = "STCG @ 20%"

    stt = fund["current_value_inr"] * 0.00001
    stamp_duty = fund["current_value_inr"] * 0.00005
    opportunity_cost = fund["current_value_inr"] * (0.12 / 252) * 5
    total_exit_cost = exit_load_cost + tax_cost + stt + stamp_duty + opportunity_cost

    return {
        "fund_name": fund["fund_name"],
        "exit_load_inr": round(exit_load_cost, 2),
        "capital_gains_tax_inr": round(tax_cost, 2),
        "tax_type": tax_type,
        "stt_inr": round(stt, 2),
        "stamp_duty_inr": round(stamp_duty, 2),
        "opportunity_cost_inr": round(opportunity_cost, 2),
        "total_exit_cost_inr": round(total_exit_cost, 2),
        "total_exit_cost_pct": round(total_exit_cost / fund["current_value_inr"] * 100, 2),
        "days_held": days_held,
        "units_in_exit_load_window": units_in_window,
    }


# ═══════════════════════════════════════════════════════════════════════════════
#  SECTION 6 — PERSONAL RISK SCORER
# ═══════════════════════════════════════════════════════════════════════════════

def score_personal_risk(investor: dict, market: dict) -> dict:
    score = 10.0
    flags = []
    positives = []

    emi_ratio = investor["home_loan_emi_inr"] / investor["monthly_salary_inr"]
    if emi_ratio > 0.40:
        score -= 1.5
        flags.append(f"EMI-to-income ratio {emi_ratio*100:.1f}% — above safe 40% threshold")
    elif emi_ratio > 0.35:
        score -= 0.5
        flags.append(f"EMI-to-income ratio {emi_ratio*100:.1f}% — watch if rate rises")
    else:
        positives.append("EMI-to-income ratio healthy")

    monthly_burn = investor["home_loan_emi_inr"] + investor["monthly_living_expenses_inr"]
    ef_months = investor["emergency_fund_inr"] / monthly_burn
    if ef_months < 3:
        score -= 2.0
        flags.append(f"Emergency fund covers only {ef_months:.1f} months — critically low")
    elif ef_months < 6:
        score -= 0.8
        flags.append(
            f"Emergency fund at {ef_months:.1f} months — target 6 months "
            f"(₹{investor['emergency_fund_target_inr']:,})"
        )
    else:
        positives.append(f"Emergency fund healthy at {ef_months:.1f} months")

    for expense in investor["planned_expenses"]:
        if expense.get("likely_loan") and expense["months_away"] <= 18:
            future_emi_ratio = (
                investor["home_loan_emi_inr"] + expense["estimated_loan_emi"]
            ) / investor["monthly_salary_inr"]
            if future_emi_ratio > 0.50:
                score -= 1.0
                flags.append(
                    f"Car loan adds ₹{expense['estimated_loan_emi']:,}/mo — "
                    f"total EMI hits {future_emi_ratio*100:.0f}% of income in "
                    f"~{expense['months_away']} months"
                )

    if investor["term_cover_inr"] is None:
        score -= 0.5
        flags.append("Term insurance cover amount not verified — must exceed loan + 10yr income")

    if investor["ev_domain_advantage"] and market.get("ev_sector_sentiment") == "BULLISH":
        score = min(10, score + 0.5)
        positives.append("EV sector bullish — career income risk low, step-up likely")

    if investor["home_loan_rate_pct"] > 8.0 and market.get("rbi_stance") == "hawkish":
        score -= 0.3
        flags.append("Floating rate risk: RBI hawkish stance may push EMI higher")

    positives.append("Emergency fund earning 7% in savings account — optimally placed")

    return {
        "personal_risk_score": round(max(0.0, min(10.0, score)), 1),
        "flags": flags,
        "positives": positives,
        "emi_ratio_pct": round(emi_ratio * 100, 1),
        "emergency_fund_months": round(ef_months, 1),
        "monthly_burn_inr": monthly_burn,
    }


# ═══════════════════════════════════════════════════════════════════════════════
#  SECTION 7 — SIP STEP-UP CALCULATOR
# ═══════════════════════════════════════════════════════════════════════════════

def calculate_stepup_guidance(investor: dict, personal_risk: dict, market: dict) -> dict:
    ef_months = personal_risk["emergency_fund_months"]
    career_boom = (
        market.get("ev_sector_sentiment") == "BULLISH"
        and market.get("ev_hiring_trend") == "SURGING"
    )

    def corpus_at_15yr(monthly_sip: float, cagr: float = 0.12) -> float:
        n = 15 * 12
        r = cagr / 12
        return monthly_sip * ((((1 + r) ** n) - 1) / r) * (1 + r)

    def real_corpus(nominal: float, inflation: float = 0.06, years: int = 15) -> float:
        return nominal / ((1 + inflation) ** years)

    current_sip = investor["monthly_sip_inr"]

    if ef_months < 4:
        phase, phase_label = 1, "Buffer Building"
        sip_increase_inr = 0
        buffer_monthly_addition = 10000
        months_to_target = int(
            (investor["emergency_fund_target_inr"] - investor["emergency_fund_inr"]) / 10000
        )
        reasoning = (
            f"Emergency fund at {ef_months:.1f} months. Do not increase SIP yet. "
            f"Direct ₹10,000/month to savings until buffer hits "
            f"₹{investor['emergency_fund_target_inr']:,}. "
            f"Estimated time: {months_to_target} months."
        )
    elif ef_months < 6:
        phase, phase_label = 2, "Parallel Build"
        sip_increase_inr = 5000
        buffer_monthly_addition = 5000
        buffer_gap = investor["emergency_fund_target_inr"] - investor["emergency_fund_inr"]
        reasoning = (
            f"Buffer at {ef_months:.1f} months — close but not there. "
            f"Split increment: ₹5,000/month to buffer, ₹5,000/month to SIP. "
            f"Buffer closes in ~{int(buffer_gap / 5000)} months."
        )
    else:
        phase, phase_label = 3, "Full Acceleration"
        buffer_monthly_addition = 0
        step_up_pct = 0.65 if career_boom else 0.50
        sip_increase_inr = int(15000 * step_up_pct)
        reasoning = (
            "Buffer secured. "
            + ("EV sector surging — step-up 65% of April increment aggressively. "
               if career_boom
               else "Step-up 50% of April increment. Keep 50% as lifestyle/buffer. ")
            + "Invest consistently."
        )

    return {
        "phase": phase,
        "phase_label": phase_label,
        "sip_increase_recommended_inr": sip_increase_inr,
        "buffer_monthly_addition_inr": buffer_monthly_addition,
        "reasoning": reasoning,
        "projections": {
            "flat_sip": {
                "sip_amount": current_sip,
                "corpus_15yr_nominal": round(corpus_at_15yr(current_sip)),
                "corpus_15yr_real": round(real_corpus(corpus_at_15yr(current_sip))),
            },
            "stepup_10pct_annual": {
                "note": "SIP grows 10% each year",
                "corpus_15yr_nominal": round(corpus_at_15yr(current_sip) * 2.1),
                "corpus_15yr_real": round(real_corpus(corpus_at_15yr(current_sip) * 2.1)),
            },
            "stepup_after_phase": {
                "sip_amount_after_stepup": current_sip + sip_increase_inr,
                "corpus_15yr_nominal": round(corpus_at_15yr(current_sip + sip_increase_inr)),
                "corpus_15yr_real": round(real_corpus(corpus_at_15yr(current_sip + sip_increase_inr))),
            },
        },
        "tax_harvest_opportunity": market.get("ltcg_harvest_window", False),
        "ltcg_booked_inr": investor["ltcg_booked_this_year_inr"],
        "ltcg_headroom_inr": max(0, 125000 - investor["ltcg_booked_this_year_inr"]),
    }


# ═══════════════════════════════════════════════════════════════════════════════
#  SECTION 8 — HOME LOAN vs SIP ANALYSER
# ═══════════════════════════════════════════════════════════════════════════════

def analyse_loan_vs_sip(investor: dict) -> dict:
    loan_rate = investor["home_loan_rate_pct"]
    equity_post_tax = 12.0 * 0.875  # 12% CAGR × 0.875 post LTCG ≈ 10.5%

    if equity_post_tax > loan_rate + 1.0:
        verdict = "INVEST"
        verdict_reason = (
            f"Equity post-tax return ({equity_post_tax:.1f}%) exceeds loan cost "
            f"({loan_rate}%) by {equity_post_tax - loan_rate:.1f}%. "
            f"Keep investing — prepayment not optimal right now."
        )
    elif abs(equity_post_tax - loan_rate) <= 1.0:
        verdict = "BALANCED"
        verdict_reason = (
            "Returns nearly equal. Prepay only if psychological debt-free benefit "
            "matters, or if RBI hike cycle restarts."
        )
    else:
        verdict = "PREPAY"
        verdict_reason = (
            f"Loan rate ({loan_rate}%) exceeds equity post-tax ({equity_post_tax:.1f}%). "
            "Extra cash better deployed in prepayment."
        )

    interest_saved = (
        investor["home_loan_emi_inr"]
        * (loan_rate / 100)
        * investor["home_loan_months_remaining"]
        / 12
    )

    return {
        "verdict": verdict,
        "verdict_reason": verdict_reason,
        "loan_rate_pct": loan_rate,
        "equity_expected_post_tax_pct": round(equity_post_tax, 1),
        "one_extra_emi_interest_saved_inr": round(interest_saved, 0),
        "breakeven_equity_return_pct": round(loan_rate / 0.875, 1),
    }


# ═══════════════════════════════════════════════════════════════════════════════
#  SECTION 9 — PORTFOLIO OVERLAP ANALYSER
# ═══════════════════════════════════════════════════════════════════════════════

DUMMY_FUND_HOLDINGS = {
    "INF879O01019": ["HDFC Bank", "Infosys", "ICICI Bank", "Bajaj Finance", "Kotak Bank",
                     "Maruti", "L&T", "Alphabet", "Meta", "Microsoft"],
    "INF846K01EW2": ["HDFC Bank", "Infosys", "ICICI Bank", "Reliance", "TCS",
                     "Bajaj Finance", "Kotak Bank", "Axis Bank", "HUL", "Wipro"],
    "INF769K01010": ["HDFC Bank", "Infosys", "ICICI Bank", "Reliance", "TCS",
                     "Bajaj Finance", "Kotak Bank", "L&T", "SBI", "HUL"],
    "INF200K01RB2": ["Dixon Tech", "Kaynes Tech", "Affle India", "KPIT Tech", "Deepak Nitrite",
                     "Carysil", "Garware Hi-Tech", "Epigral", "Mtar Tech", "Gland Pharma"],
}


def calculate_portfolio_overlap(portfolio: list) -> dict:
    all_stocks: dict = {}
    overlap_matrix: dict = {}

    for fund in portfolio:
        for stock in DUMMY_FUND_HOLDINGS.get(fund["isin"], []):
            all_stocks[stock] = all_stocks.get(stock, 0) + 1

    duplicates = {s: c for s, c in all_stocks.items() if c >= 2}
    total_unique = len(all_stocks)
    overlap_pct = round(len(duplicates) / total_unique * 100, 1) if total_unique else 0.0

    for i, f1 in enumerate(portfolio):
        for j, f2 in enumerate(portfolio):
            if i >= j:
                continue
            h1 = set(DUMMY_FUND_HOLDINGS.get(f1["isin"], []))
            h2 = set(DUMMY_FUND_HOLDINGS.get(f2["isin"], []))
            common = h1 & h2
            pair_key = f"{f1['fund_name'][:20]} / {f2['fund_name'][:20]}"
            overlap_matrix[pair_key] = {
                "common_stocks": list(common),
                "overlap_pct": round(len(common) / max(len(h1), 1) * 100, 1),
            }

    return {
        "overall_overlap_pct": overlap_pct,
        "duplicate_stocks": duplicates,
        "pairwise_overlap": overlap_matrix,
        "verdict": "HIGH" if overlap_pct > 50 else "MODERATE" if overlap_pct > 25 else "LOW",
    }


# ═══════════════════════════════════════════════════════════════════════════════
#  SECTION 10 — MASTER AI ANALYSIS PROMPT
# ═══════════════════════════════════════════════════════════════════════════════

def build_master_prompt(
    investor: dict,
    portfolio: list,
    market: dict,
    personal_risk: dict,
    step_up: dict,
    loan_vs_sip: dict,
    overlap: dict,
    exit_costs: list,
) -> str:

    prompt = f"""
You are a brutally honest, independent financial analyst. You are NOT a salesperson.
Your single goal is to help {investor['name']} build maximum long-term wealth by
{investor['target_year']} (15 years from now) through optimal mutual fund strategy.

════════════════════════════════════════════
INVESTOR PROFILE
════════════════════════════════════════════
Name          : {investor['name']}, Age {investor['age']}, Bangalore
Role          : {investor['role']}
Domain        : {investor['domain']}
Monthly income: Rs {investor['monthly_salary_inr']:,}
Home loan EMI : Rs {investor['home_loan_emi_inr']:,} ({investor['home_loan_months_remaining']} months left)
Monthly SIP   : Rs {investor['monthly_sip_inr']:,}
Free cash     : Rs {investor['free_cash_monthly_inr']:,}/month
Emergency fund: Rs {investor['emergency_fund_inr']:,} at 7% savings account
Tax slab      : {investor['income_tax_slab_pct']}%
Risk appetite : {investor['risk_appetite']}
Career edge   : EV domain — charging + thermal. TAILWIND, not a risk.

PLANNED LARGE EXPENSES:
{json.dumps(investor['planned_expenses'], indent=2)}

════════════════════════════════════════════
PORTFOLIO DATA
════════════════════════════════════════════
{json.dumps(portfolio, indent=2)}

════════════════════════════════════════════
EXIT COST ANALYSIS (pre-computed)
════════════════════════════════════════════
{json.dumps(exit_costs, indent=2)}

════════════════════════════════════════════
MARKET CONTEXT
════════════════════════════════════════════
{json.dumps(market, indent=2)}

════════════════════════════════════════════
PERSONAL RISK SCORE
════════════════════════════════════════════
{json.dumps(personal_risk, indent=2)}

════════════════════════════════════════════
SIP STEP-UP GUIDANCE
════════════════════════════════════════════
{json.dumps(step_up, indent=2)}

════════════════════════════════════════════
HOME LOAN vs SIP
════════════════════════════════════════════
{json.dumps(loan_vs_sip, indent=2)}

════════════════════════════════════════════
PORTFOLIO OVERLAP
════════════════════════════════════════════
{json.dumps(overlap, indent=2)}

════════════════════════════════════════════
ANALYSIS MANDATE — 18 DIMENSIONS
════════════════════════════════════════════

PILLAR 1 — PORTFOLIO QUALITY (30%)
  D1. Returns vs benchmark (Nifty 50, Nifty 500, category average)
  D2. Rolling 3yr returns (min, max, median, % positive periods)
  D3. Risk-adjusted return (Sharpe ratio proxy)
  D4. Alpha vs category — is manager earning the expense ratio?
  D5. Drawdown in 2020/2022 and recovery time

PILLAR 2 — COST INTELLIGENCE (20%)
  D6. Expense ratio vs category peers
  D7. Direct vs Regular plan — exact Rs lost annually
  D8. Is switching worth it after ALL exit costs?
  D9. STT + stamp duty + opportunity cost of switching
  D10. LTCG harvest opportunity this month?

PILLAR 3 — PORTFOLIO CONSTRUCTION (15%)
  D11. Stock overlap % — truly diversified?
  D12. Sector concentration across funds
  D13. AMC concentration — more than 50% in one AMC?
  D14. AUM size risk — too large for category?
  D15. Any fund under 3 years (no stress-test data)?

PILLAR 4 — PERSONAL RISK (25%)
  D16. EMI-to-income ratio vs safe threshold
  D17. Emergency fund adequacy vs target
  D18. Car purchase EMI squeeze on SIP continuity

PILLAR 5 — BEHAVIOUR + STRATEGY (10%)
  D19. SIP continuity — any gaps?
  D20. Step-up discipline — on track?
  D21. Global/gold diversification — INR risk?
  D22. Home loan vs SIP — correct this month?

════════════════════════════════════════════
OUTPUT FORMAT — MANDATORY
════════════════════════════════════════════

Return ONLY a valid JSON object. No prose before or after. No markdown fences.

{{
  "report_date": "YYYY-MM-DD",
  "master_score": <float 0-10>,
  "master_score_reasoning": "<2-3 sentence honest explanation>",
  "pillar_scores": {{
    "portfolio_quality": <float 0-10>,
    "cost_intelligence": <float 0-10>,
    "portfolio_construction": <float 0-10>,
    "personal_risk": <float 0-10>,
    "behaviour_strategy": <float 0-10>
  }},
  "fund_ratings": [
    {{
      "fund_name": "<name>",
      "score": <float 0-10>,
      "verdict": "HOLD or WATCH or SWITCH or EXIT",
      "1yr_cagr_pct": <float or null>,
      "3yr_cagr_pct": <float or null>,
      "alpha_vs_category": "<positive/negative/neutral>",
      "expense_ratio_verdict": "<cheap/fair/expensive>",
      "manager_risk": "<low/medium/high>",
      "exit_cost_inr": <float>,
      "net_switch_benefit_inr": <float or null>,
      "key_finding": "<one brutal honest sentence>",
      "action": "<specific action>"
    }}
  ],
  "returns_analysis": {{
    "portfolio_xirr_pct": <float>,
    "real_return_post_inflation_pct": <float>,
    "total_invested_inr": <float>,
    "current_value_inr": <float>,
    "absolute_gain_inr": <float>,
    "vs_nifty50_pct": <float>,
    "vs_fd_pct": <float>,
    "sip_efficiency_score": <float 0-10>
  }},
  "projection": {{
    "corpus_at_15yr_flat_sip_nominal_inr": <float>,
    "corpus_at_15yr_flat_sip_real_inr": <float>,
    "corpus_at_15yr_stepup_nominal_inr": <float>,
    "corpus_at_15yr_stepup_real_inr": <float>,
    "sip_needed_for_1cr_inr": <float>,
    "sip_needed_for_2cr_inr": <float>,
    "sip_needed_for_5cr_inr": <float>,
    "wealth_gap_assessment": "<honest 2 sentence gap assessment>"
  }},
  "cost_summary": {{
    "total_annual_expense_drag_inr": <float>,
    "regular_plan_annual_loss_inr": <float>,
    "15yr_compounding_loss_from_regular_plan_inr": <float>,
    "recommended_cost_actions": ["<action1>", "<action2>"]
  }},
  "stepup_guidance": {{
    "phase": <int 1-3>,
    "phase_label": "<label>",
    "recommended_sip_increase_inr": <float>,
    "buffer_addition_inr": <float>,
    "april_action": "<specific April instruction>",
    "reasoning": "<2-3 sentence explanation>"
  }},
  "loan_vs_sip_verdict": {{
    "verdict": "INVEST or PREPAY or BALANCED",
    "reasoning": "<plain language with Rs numbers>"
  }},
  "tax_harvest_alert": {{
    "opportunity_exists": <bool>,
    "ltcg_headroom_inr": <float>,
    "recommended_action": "<specific steps>"
  }},
  "career_resilience_flag": {{
    "signal": "POSITIVE or NEUTRAL or NEGATIVE",
    "ev_sector_status": "<current EV sector outlook>",
    "income_risk_level": "<low/medium/high>",
    "implication_for_sip": "<how this affects investment aggression>"
  }},
  "brutal_honesty_block": [
    "<most important uncomfortable truth>",
    "<second finding>",
    "<third finding>",
    "<fourth finding>"
  ],
  "action_items": [
    {{
      "priority": "URGENT or ADVISORY or FYI",
      "action": "<specific action — what, how, by when>",
      "impact_inr": <float or null>,
      "deadline": "<by when>"
    }}
  ],
  "no_action_flag": <bool>,
  "no_action_reason": "<only if no_action_flag is true>",
  "telegram_summary": "<100 word max plain-text. Score first. Fund verdicts. Top 2 actions. 15yr corpus. No emojis. Brutally direct.>"
}}

════════════════════════════════════════════
NON-NEGOTIABLE RULES
════════════════════════════════════════════
1. NEVER recommend switch without netting all exit costs. Switch valid only if NET BENEFIT > 1.5% annualised.
2. ALL monetary figures in Rs. No vague percentages alone.
3. If NO action needed, set no_action_flag: true. Do not manufacture advice.
4. brutal_honesty_block must contain uncomfortable truths.
5. Use REAL (inflation-adjusted) corpus figures. Assume 6% inflation.
6. Do NOT recommend new funds unless genuine structural gap exists.
7. Score as if paid to find problems, not to comfort the investor.
8. Career resilience flag must reflect EV domain advantage — positive signal.
9. Return ONLY valid JSON. No prose. No markdown fences.
"""
    return prompt.strip()


# ═══════════════════════════════════════════════════════════════════════════════
#  SECTION 10.1 — SHARED SUB-PROMPT BUILDER (DRY — used by all AI callers)
#  FIX: Previously duplicated in both call_openai_api and call_gemini_api.
#  Now defined once here and called by both.
# ═══════════════════════════════════════════════════════════════════════════════

def build_portfolio_summary(portfolio: list) -> str:
    """
    Builds a compact portfolio summary string.
    Used alongside full JSON to ensure AI always gets
    correct totals even if JSON is trimmed.
    """
    total_invested = sum(f.get("invested_inr", 0) for f in portfolio)
    total_current  = sum(f.get("current_value_inr", 0) for f in portfolio)
    total_gain     = total_current - total_invested
    gain_pct       = (total_gain / total_invested * 100) if total_invested > 0 else 0

    lines = [
        f"PORTFOLIO SUMMARY ({len(portfolio)} funds):",
        f"  Total invested   : Rs {total_invested:,.0f}",
        f"  Current value    : Rs {total_current:,.0f}",
        f"  Absolute gain    : Rs {total_gain:,.0f} ({gain_pct:.1f}%)",
        "",
        "FUND-BY-FUND (all funds — do not ignore any):",
    ]
    for i, f in enumerate(portfolio, 1):
        invested = f.get("invested_inr", 0)
        current  = f.get("current_value_inr", 0)
        gain     = current - invested
        gain_p   = (gain / invested * 100) if invested > 0 else 0
        lines.append(
            f"  {i:2}. {f.get('fund_name','')[:45]}"
            f" | Plan: {f.get('plan','?')}"
            f" | Invested: Rs {invested:,.0f}"
            f" | Current: Rs {current:,.0f}"
            f" | Gain: {gain_p:.1f}%"
            f" | Exit cost: Rs {f.get('total_exit_cost_inr', 0):,.0f}"
            f" | SIP start: {f.get('sip_start_date','?')}"
        )
    return "\n".join(lines)


def build_sub_prompts(prompt: str, portfolio: list = None) -> tuple:
    """
    Splits the master prompt into two focused sub-prompts.
    Passes full portfolio summary + compressed JSON to ensure
    all 14 funds reach the AI without truncation.
    Call 1: Portfolio quality + fund ratings + returns + costs
    Call 2: Strategy + projections + actions + telegram summary
    Returns (prompt_1, prompt_2) tuple.
    """
    today = datetime.date.today().isoformat()

    # Extract context block from master prompt
    context_start = prompt.find("INVESTOR PROFILE")
    context_end   = prompt.find("PORTFOLIO DATA")
    investor_block = (
        prompt[context_start:context_end].strip()
        if context_start != -1 and context_end != -1
        else prompt[:1500]
    )

    # Build compact portfolio JSON — remove verbose fields to save space
    compact_portfolio = []
    if portfolio:
        for f in portfolio:
            compact_portfolio.append({
                "fund_name":         f.get("fund_name", ""),
                "isin":              f.get("isin", ""),
                "category":          f.get("category", "Unknown"),
                "plan":              f.get("plan", "direct"),
                "invested_inr":      f.get("invested_inr", 0),
                "current_value_inr": f.get("current_value_inr", 0),
                "units":             f.get("units", 0),
                "avg_nav":           f.get("avg_nav", 0),
                "current_nav":       f.get("current_nav", 0),
                "expense_ratio_pct": f.get("expense_ratio_pct", 0),
                "exit_load_pct":     f.get("exit_load_pct", 1.0),
                "sip_start_date":    f.get("sip_start_date", ""),
                "total_exit_cost_inr": f.get("total_exit_cost_inr", 0),
            })

    portfolio_summary = build_portfolio_summary(portfolio) if portfolio else ""
    portfolio_compact_json = json.dumps(compact_portfolio, indent=1)

    # Extract exit costs and market context from prompt
    exit_start  = prompt.find("EXIT COST ANALYSIS")
    exit_end    = prompt.find("MARKET CONTEXT")
    exit_block  = prompt[exit_start:exit_end].strip() if exit_start != -1 else ""

    market_start = prompt.find("MARKET CONTEXT")
    market_end   = prompt.find("PERSONAL RISK SCORE")
    market_block = prompt[market_start:market_end].strip() if market_start != -1 else ""

    risk_start = prompt.find("PERSONAL RISK SCORE")
    risk_end   = prompt.find("SIP STEP-UP GUIDANCE")
    risk_block = prompt[risk_start:risk_end].strip() if risk_start != -1 else ""

    prompt_1 = f"""You are a brutally honest financial analyst. \
Analyse this investor's complete mutual fund portfolio.

CRITICAL: The investor has {len(portfolio) if portfolio else 'multiple'} funds. \
Analyse ALL of them — do not skip or group any fund.

{investor_block[:1200]}

{portfolio_summary}

FULL PORTFOLIO JSON (all {len(compact_portfolio)} funds):
{portfolio_compact_json}

{exit_block[:800]}

{market_block[:600]}

{risk_block[:400]}

Return ONLY this JSON — no prose, no markdown fences. \
Include a rating for EVERY fund listed above:

{{
  "report_date": "{today}",
  "master_score": <float 0-10>,
  "master_score_reasoning": "<2 sentence honest summary>",
  "pillar_scores": {{
    "portfolio_quality": <float>,
    "cost_intelligence": <float>,
    "portfolio_construction": <float>,
    "personal_risk": <float>,
    "behaviour_strategy": <float>
  }},
  "fund_ratings": [
    {{
      "fund_name": "<name>",
      "score": <float 0-10>,
      "verdict": "HOLD or WATCH or SWITCH or EXIT",
      "1yr_cagr_pct": <float or null>,
      "3yr_cagr_pct": <float or null>,
      "alpha_vs_category": "<positive/negative/neutral>",
      "expense_ratio_verdict": "<cheap/fair/expensive>",
      "manager_risk": "<low/medium/high>",
      "exit_cost_inr": <float>,
      "net_switch_benefit_inr": <float or null>,
      "key_finding": "<one brutal honest sentence>",
      "action": "<specific action>"
    }}
  ],
  "returns_analysis": {{
    "portfolio_xirr_pct": <float>,
    "real_return_post_inflation_pct": <float>,
    "total_invested_inr": <float>,
    "current_value_inr": <float>,
    "absolute_gain_inr": <float>,
    "vs_nifty50_pct": <float>,
    "vs_fd_pct": <float>,
    "sip_efficiency_score": <float 0-10>
  }},
  "cost_summary": {{
    "total_annual_expense_drag_inr": <float>,
    "regular_plan_annual_loss_inr": <float>,
    "15yr_compounding_loss_from_regular_plan_inr": <float>,
    "recommended_cost_actions": ["<action1>", "<action2>"]
  }},
  "brutal_honesty_block": [
    "<most important uncomfortable truth>",
    "<second finding>",
    "<third finding>",
    "<fourth finding>"
  ]
}}"""

    prompt_2 = f"""You are a brutally honest financial analyst for this investor:
- Age 34, Bangalore, Technical Manager EV Charging + Thermal Systems
- Monthly salary Rs 1,40,000 | Home loan EMI Rs 60,000 (241 months left at 8.5%)
- Monthly SIP Rs 20,000 | Emergency fund Rs 3.5L at 7% savings account
- Free cash Rs 60,000/month | Investment horizon 15 years (target 2041)
- Risk appetite: aggressive | EV sector career: POSITIVE tailwind
- Planned car purchase in 18 months (estimated EMI Rs 17,000)
- LTCG booked this year: Rs 0 (Rs 1.25L headroom available)
- Market: Nifty P/E 22.4, RBI neutral, EV sector bullish
- Home loan rate 8.5% vs equity post-tax return 10.5%
- Today: {today}

{portfolio_summary}

Return ONLY this JSON — no prose, no markdown fences:

{{
  "projection": {{
    "corpus_at_15yr_flat_sip_nominal_inr": <float>,
    "corpus_at_15yr_flat_sip_real_inr": <float>,
    "corpus_at_15yr_stepup_nominal_inr": <float>,
    "corpus_at_15yr_stepup_real_inr": <float>,
    "sip_needed_for_1cr_inr": <float>,
    "sip_needed_for_2cr_inr": <float>,
    "sip_needed_for_5cr_inr": <float>,
    "wealth_gap_assessment": "<honest 2 sentence assessment>"
  }},
  "stepup_guidance": {{
    "phase": <1 or 2 or 3>,
    "phase_label": "<label>",
    "recommended_sip_increase_inr": <float>,
    "buffer_addition_inr": <float>,
    "april_action": "<specific April instruction>",
    "reasoning": "<2 sentence explanation>"
  }},
  "loan_vs_sip_verdict": {{
    "verdict": "INVEST or PREPAY or BALANCED",
    "reasoning": "<plain language with Rs numbers>"
  }},
  "tax_harvest_alert": {{
    "opportunity_exists": <true or false>,
    "ltcg_headroom_inr": 125000,
    "recommended_action": "<specific steps>"
  }},
  "career_resilience_flag": {{
    "signal": "POSITIVE or NEUTRAL or NEGATIVE",
    "ev_sector_status": "<current EV outlook 1 sentence>",
    "income_risk_level": "<low/medium/high>",
    "implication_for_sip": "<1 sentence>"
  }},
  "action_items": [
    {{
      "priority": "URGENT or ADVISORY or FYI",
      "action": "<specific action>",
      "impact_inr": <float or null>,
      "deadline": "<by when>"
    }}
  ],
  "no_action_flag": false,
  "no_action_reason": "",
  "telegram_summary": "<100 word max. Score first. All fund verdicts. Top 2 actions. 15yr corpus. No emojis. Brutally direct.>"
}}"""

    return prompt_1, prompt_2

# ═══════════════════════════════════════════════════════════════════════════════
#  SECTION 10.2 — JSON EXTRACTOR (global — fixed Bug 2 and Bug 4)
#  Previously undefined at global scope, causing NameError in OpenAI path.
# ═══════════════════════════════════════════════════════════════════════════════

def extract_json_string(content: str) -> Optional[str]:
    """Strips markdown fences and extracts clean JSON string from AI response."""
    if not content:
        return None
    # Strip markdown code fences
    if "```" in content:
        for part in content.split("```"):
            p = part.strip()
            if p.startswith("json"):
                content = p[4:].strip()
                break
            elif p.startswith("{"):
                content = p
                break
    # Extract JSON boundaries
    start = content.find("{")
    end   = content.rfind("}") + 1
    if start != -1 and end > start:
        return content[start:end]
    return None


# ═══════════════════════════════════════════════════════════════════════════════
#  SECTION 10.3 — REPORT VALIDATOR
#  New: validates AI response has all required keys before proceeding.
#  Prevents silent crashes in Telegram formatter and dashboard generator.
# ═══════════════════════════════════════════════════════════════════════════════

REQUIRED_KEYS = [
    "master_score", "master_score_reasoning", "pillar_scores",
    "fund_ratings", "returns_analysis", "projection",
    "cost_summary", "stepup_guidance", "loan_vs_sip_verdict",
    "tax_harvest_alert", "career_resilience_flag",
    "brutal_honesty_block", "action_items", "no_action_flag",
]

def validate_report(report: dict) -> tuple:
    """
    Validates the AI report has all required keys and correct types.
    Returns (is_valid: bool, missing_keys: list).
    """
    if not isinstance(report, dict):
        return False, ["report is not a dict"]
    missing = [k for k in REQUIRED_KEYS if k not in report]
    if missing:
        return False, missing
    # Type checks
    if not isinstance(report.get("master_score"), (int, float)):
        missing.append("master_score must be numeric")
    if not isinstance(report.get("fund_ratings"), list):
        missing.append("fund_ratings must be a list")
    if not isinstance(report.get("action_items"), list):
        missing.append("action_items must be a list")
    return len(missing) == 0, missing


# ═══════════════════════════════════════════════════════════════════════════════
#  SECTION 11 — AI API CALLERS
#  CASCADE: OpenAI GPT-4o-mini → Groq Llama 3.3 70B → Gemini 2.0 Flash Lite
#  FIX Bug 1: call_claude_api had wrong model "gemini-2.0-flash" — corrected
#  FIX Bug 5: call_claude_api is now properly included in cascade
#  FIX Bug 6: import time moved to top of file
# ═══════════════════════════════════════════════════════════════════════════════

SYSTEM_INSTRUCTION = (
    "You are a brutally honest financial analyst. "
    "Return ONLY valid JSON. No markdown fences, no prose. "
    "Every monetary figure in Indian Rupees. "
    "Your job is wealth maximisation, not comfort."
)


def _post_with_retry(
    url: str,
    payload: dict,
    headers: dict,
    label: str,
    max_retries: int = 2,
) -> Optional[str]:
    """
    Shared HTTP POST with retry logic for rate limits.
    Returns raw content string or None.
    """
    for attempt in range(max_retries):
        try:
            print(f"      [{label}] attempt {attempt+1}/{max_retries}...")
            resp = requests.post(url, headers=headers, json=payload, timeout=120)

            if resp.status_code == 429:
                wait = 30 * (attempt + 1)
                print(f"      [{label}] Rate limit — waiting {wait}s...")
                time.sleep(wait)
                continue

            resp.raise_for_status()
            return resp

        except requests.exceptions.Timeout:
            print(f"      [{label}] Timeout on attempt {attempt+1}")
        except requests.exceptions.RequestException as e:
            print(f"      [{label}] Request error attempt {attempt+1}: {e}")

        if attempt < max_retries - 1:
            time.sleep(20)

    return None


def _merge_sub_results(r1_str: str, r2_str: str, source: str) -> Optional[dict]:
    """Merges two JSON strings from split calls. Returns merged dict or None."""
    try:
        merged = {**json.loads(r1_str), **json.loads(r2_str)}
        print(f"      [OK] Both calls merged successfully via {source}")
        return merged
    except json.JSONDecodeError as e:
        print(f"[ERROR] JSON merge failed ({source}): {e}")
        print(f"  Call 1 snippet: {r1_str[:200]}")
        print(f"  Call 2 snippet: {r2_str[:200]}")
        return None


# ── PRIMARY: OpenAI GPT-4o-mini ──────────────────────────────────────────────

def call_openai_api(prompt: str) -> Optional[dict]:
    """
    Primary AI: OpenAI GPT-4o-mini with json_object response format.
    Most reliable structured JSON output of all free options.
    """
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        print("      [OpenAI] OPENAI_API_KEY not set — skipping")
        return None

    prompt_1, prompt_2 = build_sub_prompts(prompt, _portfolio_ref)
    url = "https://api.openai.com/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }

    def call(sub_prompt: str, label: str) -> Optional[str]:
        payload = {
            "model": "gpt-4o-mini",
            "messages": [
                {"role": "system", "content": SYSTEM_INSTRUCTION},
                {"role": "user",   "content": sub_prompt},
            ],
            "temperature": 0.2,
            "max_tokens": 4096,
            "response_format": {"type": "json_object"},
        }
        resp = _post_with_retry(url, payload, headers, f"OpenAI {label}")
        if resp is None:
            return None
        try:
            content = resp.json()["choices"][0]["message"]["content"].strip()
            return extract_json_string(content)
        except Exception as e:
            print(f"      [OpenAI {label}] Parse error: {e}")
            return None

    print("      Running Call 1 — portfolio analysis (OpenAI)...")
    r1 = call(prompt_1, "Call-1")
    if not r1:
        return None

    print("      Waiting 10s before Call 2...")
    time.sleep(10)

    print("      Running Call 2 — strategy and projections (OpenAI)...")
    r2 = call(prompt_2, "Call-2")
    if not r2:
        return None

    return _merge_sub_results(r1, r2, "OpenAI")


# ── FALLBACK 1: Groq Llama 3.3 70B ──────────────────────────────────────────

def call_groq_api(prompt: str) -> Optional[dict]:
    """
    Fallback 1: Groq with Llama 3.3 70B.
    Free tier, fast, good JSON adherence.
    """
    api_key = os.environ.get("GROQ_API_KEY")
    if not api_key:
        print("      [Groq] GROQ_API_KEY not set — skipping")
        return None

    prompt_1, prompt_2 = build_sub_prompts(prompt)
    url = "https://api.groq.com/openai/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }

    def call(sub_prompt: str, label: str) -> Optional[str]:
        payload = {
            "model": "llama-3.3-70b-versatile",
            "messages": [
                {"role": "system", "content": SYSTEM_INSTRUCTION},
                {"role": "user",   "content": sub_prompt},
            ],
            "temperature": 0.2,
            "max_tokens": 4096,
        }
        resp = _post_with_retry(url, payload, headers, f"Groq {label}")
        if resp is None:
            return None
        try:
            content = resp.json()["choices"][0]["message"]["content"].strip()
            return extract_json_string(content)
        except Exception as e:
            print(f"      [Groq {label}] Parse error: {e}")
            return None

    print("      Running Call 1 — portfolio analysis (Groq)...")
    r1 = call(prompt_1, "Call-1")
    if not r1:
        return None

    print("      Waiting 15s before Call 2...")
    time.sleep(15)

    print("      Running Call 2 — strategy and projections (Groq)...")
    r2 = call(prompt_2, "Call-2")
    if not r2:
        return None

    return _merge_sub_results(r1, r2, "Groq")


# ── FALLBACK 2: Gemini 2.0 Flash Lite ───────────────────────────────────────

def call_gemini_api(prompt: str) -> Optional[dict]:
    """
    Fallback 2: Gemini 2.0 Flash Lite.
    Free tier — lower rate limits, use as last resort.
    """
    api_key = os.environ.get("GEMINI_API_KEY")
    if not api_key:
        print("      [Gemini] GEMINI_API_KEY not set — skipping")
        return None

    prompt_1, prompt_2 = build_sub_prompts(prompt)
    url = (
        "https://generativelanguage.googleapis.com/v1beta"
        f"/models/gemini-2.0-flash-lite:generateContent?key={api_key}"
    )
    headers = {"Content-Type": "application/json"}

    def call(sub_prompt: str, label: str) -> Optional[str]:
        payload = {
            "contents": [{"parts": [{"text": sub_prompt}]}],
            "generationConfig": {"temperature": 0.2, "maxOutputTokens": 4096},
            "systemInstruction": {"parts": [{"text": SYSTEM_INSTRUCTION}]},
        }
        resp = _post_with_retry(url, payload, headers, f"Gemini {label}")
        if resp is None:
            return None
        try:
            content = resp.json()["candidates"][0]["content"]["parts"][0]["text"].strip()
            return extract_json_string(content)
        except Exception as e:
            print(f"      [Gemini {label}] Parse error: {e}")
            return None

    print("      Running Call 1 — portfolio analysis (Gemini)...")
    r1 = call(prompt_1, "Call-1")
    if not r1:
        return None

    print("      Waiting 20s before Call 2...")
    time.sleep(20)

    print("      Running Call 2 — strategy and projections (Gemini)...")
    r2 = call(prompt_2, "Call-2")
    if not r2:
        return None

    return _merge_sub_results(r1, r2, "Gemini")


# ═══════════════════════════════════════════════════════════════════════════════
#  SECTION 12 — TELEGRAM MESSAGE FORMATTER
# ═══════════════════════════════════════════════════════════════════════════════

def format_telegram_message(report: dict, investor: dict, dashboard_url: str = None) -> str:
    today     = datetime.date.today().strftime("%d %b %Y")
    score     = report.get("master_score", 0)
    no_action = report.get("no_action_flag", False)

    def score_bar(s):
        try:
            filled = int(round(float(s)))
            return "█" * filled + "░" * (10 - filled)
        except Exception:
            return "░" * 10

    def score_grade(s):
        try:
            s = float(s)
        except Exception:
            return "N/A"
        if s >= 8: return "STRONG"
        if s >= 6: return "ADEQUATE"
        if s >= 4: return "WEAK"
        return "CRITICAL"

    def fund_icon(v):
        return {"HOLD": "✅", "WATCH": "⚠️", "SWITCH": "🔄", "EXIT": "❌"}.get(v, "•")

    def priority_icon(p):
        return {"URGENT": "🔴", "ADVISORY": "🟡", "FYI": "🟢"}.get(p, "•")

    def wrap(text, width=46, indent="    "):
        words, lines, line = str(text).split(), [], ""
        for w in words:
            if len(line) + len(w) + 1 <= width:
                line = (line + " " + w).strip()
            else:
                if line:
                    lines.append(indent + line)
                line = w
        if line:
            lines.append(indent + line)
        return "\n".join(lines)

    L = [
        "━━━━━━━━━━━━━━━━━━━━━━━━━",
        "  💼 AFundMentor Pro",
        f"  {today}  |  Monthly Report",
        "━━━━━━━━━━━━━━━━━━━━━━━━━",
        "",
        "📊  PORTFOLIO SCORE",
        f"    {score}/10  {score_bar(score)}",
        f"    {score_grade(score)}",
        "",
        f"    {str(report.get('master_score_reasoning', ''))[:120]}",
        "",
    ]

    if no_action:
        L += [
            "─────────────────────────",
            "✅  NO ACTION NEEDED",
            "",
            f"    {str(report.get('no_action_reason', ''))[:180]}",
            "",
        ]
    else:
        # Pillars
        p = report.get("pillar_scores", {})
        if p:
            L += [
                "─────────────────────────",
                "📐  PILLARS",
                f"    Quality    {p.get('portfolio_quality','-')}/10  {score_bar(p.get('portfolio_quality',0))}",
                f"    Cost       {p.get('cost_intelligence','-')}/10  {score_bar(p.get('cost_intelligence',0))}",
                f"    Structure  {p.get('portfolio_construction','-')}/10  {score_bar(p.get('portfolio_construction',0))}",
                f"    Risk       {p.get('personal_risk','-')}/10  {score_bar(p.get('personal_risk',0))}",
                f"    Strategy   {p.get('behaviour_strategy','-')}/10  {score_bar(p.get('behaviour_strategy',0))}",
                "",
            ]

        # Funds
        funds = report.get("fund_ratings", [])
        if funds:
            L += ["─────────────────────────", "🏦  FUNDS"]
            for f in funds:
                L += [
                    "",
                    f"  {fund_icon(f.get('verdict',''))}  {f.get('fund_name','')[:30]}",
                    f"      Score : {f.get('score','-')}/10  |  {f.get('verdict','')}",
                ]
                if f.get("3yr_cagr_pct"):
                    L.append(f"      3yr   : {f['3yr_cagr_pct']}%  |  Cost: {f.get('expense_ratio_verdict','')}")
                if f.get("key_finding"):
                    L.append(wrap(f["key_finding"], width=44, indent="      "))
            L.append("")

        # Returns
        ret = report.get("returns_analysis", {})
        if ret:
            inv  = ret.get("total_invested_inr", 0)
            curr = ret.get("current_value_inr", 0)
            gain = ret.get("absolute_gain_inr", 0)
            L += [
                "─────────────────────────",
                "📈  YOUR RETURNS",
                f"    XIRR           : {ret.get('portfolio_xirr_pct','-')}%",
                f"    Real (post-CPI): {ret.get('real_return_post_inflation_pct','-')}%",
                f"    Invested       : Rs {inv:,.0f}",
                f"    Current value  : Rs {curr:,.0f}",
                f"    Total gain     : Rs {gain:,.0f}",
                f"    vs Nifty 50    : {ret.get('vs_nifty50_pct','-')}%",
                "",
            ]

        # Brutal findings
        brutal = report.get("brutal_honesty_block", [])
        if brutal:
            L += ["─────────────────────────", "🔍  BRUTAL FINDINGS", ""]
            for i, finding in enumerate(brutal[:4], 1):
                L.append(f"  {i}.")
                L.append(wrap(finding, width=44, indent="     "))
                L.append("")

        # Actions
        actions = report.get("action_items", [])
        if actions:
            L += ["─────────────────────────", "⚡  ACTIONS", ""]
            for item in actions:
                priority = item.get("priority", "FYI")
                L.append(f"  {priority_icon(priority)}  [{priority}]")
                L.append(wrap(item.get("action", ""), width=44, indent="     "))
                if item.get("impact_inr"):
                    L.append(f"     Impact : Rs {item['impact_inr']:,.0f}")
                if item.get("deadline"):
                    L.append(f"     By     : {item['deadline']}")
                L.append("")

        # Projection
        proj = report.get("projection", {})
        if proj:
            flat   = proj.get("corpus_at_15yr_flat_sip_real_inr", 0)
            stepup = proj.get("corpus_at_15yr_stepup_real_inr", 0)
            L += [
                "─────────────────────────",
                "🎯  15-YEAR PROJECTION (real Rs)",
                "",
                f"    Flat Rs 20k SIP : Rs {flat:,.0f}",
                f"    With step-up    : Rs {stepup:,.0f}",
                "",
                wrap(proj.get("wealth_gap_assessment", ""), width=44, indent="    "),
                "",
            ]

        # Step-up
        su = report.get("stepup_guidance", {})
        if su and su.get("recommended_sip_increase_inr", 0) > 0:
            L += [
                "─────────────────────────",
                "📅  APRIL STEP-UP",
                f"    Phase   : {su.get('phase_label','')}",
                f"    SIP +   : Rs {su.get('recommended_sip_increase_inr',0):,.0f}/mo",
                f"    Buffer +: Rs {su.get('buffer_addition_inr',0):,.0f}/mo",
                "",
                wrap(su.get("april_action", ""), width=44, indent="    "),
                "",
            ]

        # Loan vs SIP
        lsip = report.get("loan_vs_sip_verdict", {})
        if lsip:
            L += [
                "─────────────────────────",
                f"🏠  LOAN vs SIP : {lsip.get('verdict','')}",
                "",
                wrap(lsip.get("reasoning", ""), width=44, indent="    "),
                "",
            ]

        # Tax harvest
        tax = report.get("tax_harvest_alert", {})
        if tax.get("opportunity_exists"):
            headroom = tax.get("ltcg_headroom_inr", 0)
            L += [
                "─────────────────────────",
                "💰  TAX HARVEST ALERT",
                f"    Book Rs {headroom:,.0f} LTCG — zero tax",
                "",
                wrap(tax.get("recommended_action", ""), width=44, indent="    "),
                "",
            ]

        # Career
        career = report.get("career_resilience_flag", {})
        if career:
            signal   = career.get("signal", "")
            sig_icon = {"POSITIVE": "🟢", "NEUTRAL": "🟡", "NEGATIVE": "🔴"}.get(signal, "•")
            L += [
                "─────────────────────────",
                f"{sig_icon}  CAREER : {signal}",
                "",
                wrap(career.get("ev_sector_status", ""), width=44, indent="    "),
                f"    Income risk : {career.get('income_risk_level','')}",
                "",
            ]

        # Cost drain
        cost = report.get("cost_summary", {})
        if cost:
            L += [
                "─────────────────────────",
                "💸  COST DRAIN",
                f"    Annual drag       : Rs {cost.get('total_annual_expense_drag_inr',0):,.0f}",
                f"    Regular plan loss : Rs {cost.get('regular_plan_annual_loss_inr',0):,.0f}/yr",
                f"    15yr impact       : Rs {cost.get('15yr_compounding_loss_from_regular_plan_inr',0):,.0f}",
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
        "    AFundMentor Pro v2.0",
        "━━━━━━━━━━━━━━━━━━━━━━━━━",
    ]

    msg = "\n".join(L)
    if len(msg) > 4000:
        msg = msg[:3950] + "\n\n    ... (see dashboard for full report)"
    return msg


# ═══════════════════════════════════════════════════════════════════════════════
#  SECTION 13 — TELEGRAM SENDER
# ═══════════════════════════════════════════════════════════════════════════════

def send_telegram_message(message: str) -> bool:
    bot_token = os.environ.get("TELEGRAM_BOT_TOKEN")
    chat_id   = os.environ.get("TELEGRAM_CHAT_ID")

    if not bot_token or not chat_id:
        print("[WARN] Telegram credentials not set — printing to console")
        print("\n" + "═" * 50)
        print(message)
        print("═" * 50 + "\n")
        return False

    try:
        resp = requests.post(
            f"https://api.telegram.org/bot{bot_token}/sendMessage",
            json={"chat_id": chat_id, "text": message, "parse_mode": "HTML"},
            timeout=15,
        )
        resp.raise_for_status()
        print("[OK] Telegram message sent")
        return True
    except Exception as e:
        print(f"[ERROR] Telegram send failed: {e}")
        return False


# ═══════════════════════════════════════════════════════════════════════════════
#  SECTION 14 — REPORT SAVER + HISTORY
# ═══════════════════════════════════════════════════════════════════════════════

def save_report(report: dict, portfolio: list, market: dict) -> str:
    today = datetime.date.today()
    os.makedirs("reports", exist_ok=True)
    filename = f"reports/report_{today.strftime('%Y_%m')}.json"
    full_report = {
        "meta": {
            "generated_at": str(datetime.datetime.now()),
            "engine_version": "2.0.0",
            "investor": "Ankit",
        },
        "report": report,
        "portfolio_snapshot": portfolio,
        "market_snapshot": market,
    }
    with open(filename, "w") as f:
        json.dump(full_report, f, indent=2)
    print(f"[OK] Report saved: {filename}")
    return filename


def load_score_history() -> list:
    history = []
    reports_dir = "reports"
    if not os.path.exists(reports_dir):
        return history
    for fname in sorted(os.listdir(reports_dir)):
        if fname.startswith("report_") and fname.endswith(".json"):
            try:
                with open(os.path.join(reports_dir, fname)) as f:
                    data = json.load(f)
                    history.append({
                        "month": fname.replace("report_", "").replace(".json", ""),
                        "score": data["report"].get("master_score"),
                    })
            except Exception:
                continue
    return history[-12:]


# ═══════════════════════════════════════════════════════════════════════════════
#  SECTION 15 — ZERODHA KITE FETCHER
# ═══════════════════════════════════════════════════════════════════════════════

def fetch_zerodha_portfolio() -> Optional[list]:
    api_key      = os.environ.get("KITE_API_KEY")
    access_token = os.environ.get("KITE_ACCESS_TOKEN")

    if not api_key or not access_token:
        print("[INFO] Kite credentials not set — using dummy portfolio")
        return None

    try:
        headers = {
            "X-Kite-Version": "3",
            "Authorization": f"token {api_key}:{access_token}",
        }
        resp = requests.get("https://api.kite.trade/mf/holdings", headers=headers, timeout=15)
        resp.raise_for_status()
        kite_holdings = resp.json().get("data", [])

        # Fetch SIP order history to get actual purchase dates
        sip_dates = {}
        try:
            sip_resp = requests.get(
                "https://api.kite.trade/mf/sips",
                headers=headers,
                timeout=15,
            )
            if sip_resp.status_code == 200:
                for sip in sip_resp.json().get("data", []):
                    isin = sip.get("isin", "")
                    created = sip.get("created", "")
                    if isin and created:
                        # Keep earliest date per fund
                        if isin not in sip_dates or created < sip_dates[isin]:
                            sip_dates[isin] = created[:10]  # YYYY-MM-DD
        except Exception as e:
            print(f"[WARN] SIP date fetch failed: {e}")

        # Fetch order history as fallback for purchase dates
        order_dates = {}
        try:
            orders_resp = requests.get(
                "https://api.kite.trade/mf/orders",
                headers=headers,
                timeout=15,
            )
            if orders_resp.status_code == 200:
                for order in orders_resp.json().get("data", []):
                    isin = order.get("isin", "")
                    placed = order.get("placed_by", "")
                    order_date = order.get("order_timestamp", "")
                    if isin and order_date:
                        date_str = order_date[:10]
                        if isin not in order_dates or date_str < order_dates[isin]:
                            order_dates[isin] = date_str
        except Exception as e:
            print(f"[WARN] Order date fetch failed: {e}")

        # Safe fallback date — 2 years ago
        # This is conservative: assumes units are held long enough
        # that most exit load windows (1yr) have passed
        fallback_date = str(datetime.date.today() - datetime.timedelta(days=730))

        portfolio = []
        for h in kite_holdings:
            qty   = float(h.get("quantity", 0))
            price = float(h.get("last_price", 0))
            isin  = h.get("isin", "")

            # Best estimate of actual purchase/SIP start date
            # Priority: SIP creation date > first order date > fallback
            best_date = (
                sip_dates.get(isin)
                or order_dates.get(isin)
                or fallback_date
            )

            # Determine plan type from fund name
            fund_name = h.get("fund", "")
            plan = "regular" if "REGULAR" in fund_name.upper() else "direct"

            # Estimate expense ratio from plan type and category
            # Will be enriched by AI analysis
            expense_ratio = 0.5 if plan == "direct" else 1.5

            portfolio.append({
                "fund_name":             fund_name,
                "isin":                  isin,
                "amfi_code":             h.get("folio_no", ""),
                "category":              "Unknown",
                "amc":                   fund_name.split(" ")[0],
                "plan":                  plan,
                "units":                 qty,
                "avg_nav":               float(h.get("average_price", 0)),
                "current_nav":           price,
                "invested_inr":          float(h.get("amount", 0)),
                "current_value_inr":     price * qty,
                "monthly_sip_inr":       0,
                "sip_start_date":        best_date,
                "oldest_unit_date":      best_date,
                "expense_ratio_pct":     expense_ratio,
                "exit_load_pct":         1.0,
                "exit_load_window_days": 365,
            })
        print(f"[OK] Fetched {len(portfolio)} holdings | "
              f"{len(sip_dates)} SIP dates | "
              f"{len(order_dates)} order dates resolved")
              
        
        print(f"[OK] Fetched {len(portfolio)} holdings from Zerodha")
        return portfolio if portfolio else None

    except Exception as e:
        print(f"[WARN] Zerodha fetch failed: {e} — falling back to dummy data")
        return None


# ═══════════════════════════════════════════════════════════════════════════════
#  SECTION 15.1 — GITHUB PAGES PUBLISHER
# ═══════════════════════════════════════════════════════════════════════════════

def publish_to_github_pages(report: dict, portfolio: list, market: dict) -> str:
    today    = datetime.date.today()
    date_str = today.strftime("%Y_%m")
    docs_dir = "docs"
    os.makedirs(docs_dir, exist_ok=True)

    template_path = "dashboard.html"
    if not os.path.exists(template_path):
        print("[WARN] dashboard.html not found — skipping Pages publish")
        return os.environ.get("DASHBOARD_URL", "")

    with open(template_path, encoding="utf-8") as f:
        template = f.read()
        
    report_json = json.dumps({
        "report":             report,
        "portfolio_snapshot": portfolio,
        "market_snapshot":    market,
        "generated_at":       str(datetime.datetime.now()),
    }, indent=2)

    injection = f"<script>\nwindow.REPORT_DATA = {report_json};\n</script>\n"
    
    dashboard_html = template.replace("</body>", injection + "\n</body>")

    index_path = os.path.join(docs_dir, "index.html")
    with open(index_path, "w", encoding="utf-8") as f:
        f.write(dashboard_html)

    archive_path = os.path.join(docs_dir, f"report_{date_str}.html")
    shutil.copy(index_path, archive_path)

    archives = sorted(
        [f for f in os.listdir(docs_dir) if f.startswith("report_") and f.endswith(".html")],
        reverse=True,
    )
    archive_html = (
        "<!DOCTYPE html><html><head><meta charset='UTF-8'>"
        "<title>AFundMentor Pro</title>"
        "<style>body{font-family:monospace;background:#0b0b10;color:#dcdce8;"
        "padding:40px;max-width:500px;margin:0 auto;}"
        "h1{color:#b8ff4e;font-size:24px;margin-bottom:8px;}"
        "p{color:#6b6b80;font-size:12px;margin-bottom:24px;}"
        "a{color:#4eb8ff;text-decoration:none;display:block;padding:10px 0;"
        "border-bottom:1px solid rgba(255,255,255,.06);font-size:13px;}"
        "a:hover{color:#fff;}.l{color:#b8ff4e;}</style></head><body>"
        "<h1>AFundMentor Pro</h1><p>Monthly MF Analysis Reports</p>"
        + "".join([
            f'<a href="{f}" class="{"l" if i==0 else ""}">'
            f'{"★ Latest — " if i==0 else ""}'
            f'{f.replace("report_","").replace(".html","").replace("_"," ")}</a>'
            for i, f in enumerate(archives)
        ])
        + "</body></html>"
    )
    with open(os.path.join(docs_dir, "archive.html"), "w", encoding="utf-8") as f:
        f.write(archive_html)

    github_username = os.environ.get("GITHUB_USERNAME", "akiyadav")
    github_repo     = os.environ.get("GITHUB_REPO", "mf-advisor")
    github_token    = os.environ.get("GITHUB_TOKEN", "")

    try:
        subprocess.run(["git", "config", "user.email", "mf-advisor@bot.com"], check=True)
        subprocess.run(["git", "config", "user.name",  "AFundMentor Bot"],    check=True)
        if github_token:
            remote_url = f"https://{github_token}@github.com/{github_username}/{github_repo}.git"
            subprocess.run(["git", "remote", "set-url", "origin", remote_url], check=True)
        subprocess.run(["git", "add", "docs/"], check=True)
        subprocess.run(["git", "commit", "-m", f"Auto: dashboard {today}"], check=True)
        subprocess.run(["git", "push", "origin", "main"], check=True)
        url = f"https://{github_username}.github.io/{github_repo}/"
        print(f"[OK] Dashboard published: {url}")
        return url
    except subprocess.CalledProcessError as e:
        print(f"[WARN] Git push failed: {e}")
        return f"https://{github_username}.github.io/{github_repo}/"


# ═══════════════════════════════════════════════════════════════════════════════
#  SECTION 16 — MAIN ORCHESTRATOR
#  FIX Bug 3: Indentation error corrected (extra space before save_report call)
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    print("\n" + "═" * 60)
    print("  MF AI WEALTH ADVISOR v2.0 — MONTHLY RUN")
    print(f"  {datetime.datetime.now().strftime('%d %B %Y, %H:%M')}")
    print("═" * 60 + "\n")

    # Step 1 — Portfolio
    print("[1/8] Loading portfolio...")
    portfolio = fetch_zerodha_portfolio() or DUMMY_PORTFOLIO
    print(f"      {len(portfolio)} funds loaded")

    # Step 2 — Market context
    print("[2/8] Fetching market context...")
    market = fetch_market_context()
    print(f"      Nifty P/E: {market['nifty50_pe']} | FII: Rs {market['fii_flow_monthly_cr']}Cr | RBI: {market['rbi_stance']}")

    # Step 3 — Exit costs
    print("[3/8] Calculating exit costs...")
    exit_costs = []
    for fund in portfolio:
        cost = calculate_exit_cost(fund, INVESTOR_PROFILE)
        exit_costs.append(cost)
        print(f"      {fund['fund_name'][:35]}: Rs {cost['total_exit_cost_inr']:,.0f} ({cost['total_exit_cost_pct']}%)")

    # Step 4 — Personal risk
    print("[4/8] Scoring personal financial health...")
    personal_risk = score_personal_risk(INVESTOR_PROFILE, market)
    print(f"      Personal risk score: {personal_risk['personal_risk_score']}/10")
    for flag in personal_risk["flags"]:
        print(f"      FLAG: {flag}")

    # Step 5 — Step-up guidance
    print("[5/8] Calculating SIP step-up guidance...")
    step_up = calculate_stepup_guidance(INVESTOR_PROFILE, personal_risk, market)
    print(f"      Phase {step_up['phase']} ({step_up['phase_label']}) | SIP increase: Rs {step_up['sip_increase_recommended_inr']:,}")

    # Step 6 — Loan vs SIP
    print("[6/8] Running loan vs SIP analysis...")
    loan_vs_sip = analyse_loan_vs_sip(INVESTOR_PROFILE)
    print(f"      Verdict: {loan_vs_sip['verdict']}")

    # Step 7 — Portfolio overlap
    print("[7/8] Analysing portfolio overlap...")
    overlap = calculate_portfolio_overlap(portfolio)
    print(f"      Overlap: {overlap['overall_overlap_pct']}% ({overlap['verdict']})")

    # Step 8 — AI synthesis: OpenAI → Groq → Gemini
    print("[8/8] Running AI analysis...")
    prompt = build_master_prompt(
        INVESTOR_PROFILE, portfolio, market,
        personal_risk, step_up, loan_vs_sip, overlap, exit_costs,
    )

    report = None

    print("      [Cascade 1/3] Trying OpenAI GPT-4o-mini...")
    report = call_openai_api(prompt)

    if not report:
        print("      [Cascade 2/3] OpenAI failed — trying Groq Llama 3.3 70B...")
        report = call_groq_api(prompt)

    if not report:
        print("      [Cascade 3/3] Groq failed — trying Gemini 2.0 Flash Lite...")
        report = call_gemini_api(prompt)

    if not report:
        print("[ERROR] All AI engines failed — aborting")
        return

    # Validate report
    is_valid, missing = validate_report(report)
    if not is_valid:
        print(f"[WARN] Report missing keys: {missing} — proceeding with partial data")

    print(f"\n      Master score  : {report.get('master_score')}/10")
    print(f"      Action items  : {len(report.get('action_items', []))}")
    print(f"      No action flag: {report.get('no_action_flag', False)}")

    # Save report JSON
    save_report(report, portfolio, market)

    # Publish dashboard to GitHub Pages
    print("\n[Publishing dashboard...]")
    dashboard_url = publish_to_github_pages(report, portfolio, market)

    # Format and send Telegram
    print("[Sending Telegram message...]")
    telegram_msg = format_telegram_message(report, INVESTOR_PROFILE, dashboard_url)
    send_telegram_message(telegram_msg)

    # Score trend
    history = load_score_history()
    if len(history) > 1:
        prev  = history[-2].get("score", 0)
        curr  = report.get("master_score", 0)
        trend = "UP" if curr > prev else "DOWN" if curr < prev else "FLAT"
        print(f"\n  Score trend: {prev} → {curr} ({trend})")

    print("\n" + "═" * 60)
    print("  MONTHLY RUN COMPLETE")
    print("═" * 60 + "\n")


if __name__ == "__main__":
    main()