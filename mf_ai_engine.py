"""
================================================================================
  MF AI WEALTH ADVISOR ENGINE
  Author  : Built for Ankit | Bangalore
  Version : 1.0.0
  Purpose : Monthly mutual fund analysis engine with brutal honesty scoring,
            full cost awareness, personal risk profiling, and step-up guidance.
================================================================================

ARCHITECTURE
  1. investor_profile        — static personal context (update when life changes)
  2. portfolio_data          — MF holdings (from Zerodha Kite API or manual input)
  3. market_context_fetcher  — live macro + news signals via web search
  4. fund_data_fetcher        — NAV, returns, AUM, manager data via MFapi.in
  5. analysis_engine         — 18-dimension scoring across 5 pillars
  6. ai_analyst              — Claude/Gemini prompt that synthesises everything
  7. report_builder          — formats Telegram message + HTML dashboard data
  8. main()                  — orchestrator; run this monthly via cron / GitHub Actions

DEPENDENCIES
  pip install requests anthropic python-telegram-bot schedule

API KEYS NEEDED (set as environment variables)
  ANTHROPIC_API_KEY     — for Claude AI analysis
  TELEGRAM_BOT_TOKEN    — from @BotFather on Telegram
  TELEGRAM_CHAT_ID      — your personal chat ID
  KITE_API_KEY          — Zerodha Kite (optional — can use manual portfolio)
  KITE_ACCESS_TOKEN     — refreshed monthly via kite_auth_helper.py
================================================================================
"""

import os
import json
import datetime
import requests
from typing import Optional


# ═══════════════════════════════════════════════════════════════════════════════
#  SECTION 1 — INVESTOR PROFILE
#  Update this when your life situation changes.
#  This is the personalisation core — every analysis is anchored to this.
# ═══════════════════════════════════════════════════════════════════════════════

INVESTOR_PROFILE = {
    "name": "Ankit",
    "age": 34,
    "city": "Bangalore",
    "investment_horizon_years": 15,
    "target_year": 2041,

    # Income & liabilities
    "monthly_salary_inr": 140000,
    "home_loan_emi_inr": 60000,
    "home_loan_months_remaining": 241,
    "home_loan_rate_pct": 8.5,           # update if floating rate changes
    "monthly_living_expenses_inr": 40000,
    "monthly_sip_inr": 20000,
    "free_cash_monthly_inr": 60000,      # salary - EMI - SIP

    # Emergency fund
    "emergency_fund_inr": 350000,        # ₹3.5L (midpoint of 3-4L)
    "emergency_fund_location": "savings_account_7pct",
    "emergency_fund_months_cover": 3.5,  # vs expenses+EMI of ~₹1L/month
    "emergency_fund_target_inr": 600000, # 6-month target

    # Insurance
    "has_term_insurance": True,
    "has_health_insurance": True,
    "health_plan": "Niva Bupa Aspire Platinum+",
    "term_cover_inr": None,              # FILL IN — must exceed loan balance + 10yr income

    # Career
    "role": "Technical Manager — EV Charging & Thermal Systems",
    "domain": "EV charging infrastructure + battery thermal management",
    "sector": "automotive_ev",
    "career_risk_level": "low_medium",   # EV is a tailwind, not headwind
    "ev_domain_advantage": True,
    "consulting_side_income_inr": 0,     # 0 now; model scenario at ₹15k-20k/mo

    # Planned large expenses (next 24 months)
    "planned_expenses": [
        {"label": "Car purchase", "estimated_inr": 800000, "months_away": 18,
         "likely_loan": True, "estimated_loan_emi": 17000},
        {"label": "International trip", "estimated_inr": 200000, "months_away": 12,
         "likely_loan": False}
    ],

    # Tax
    "income_tax_slab_pct": 30,           # assumed — update for new regime
    "claims_80c": True,                  # home loan principal
    "claims_sec24": True,                # home loan interest ₹2L deduction
    "ltcg_booked_this_year_inr": 0,      # reset every April; track for harvesting

    # Goals
    "wealth_milestones_inr": [1000000, 5000000, 10000000, 20000000],  # 10L, 50L, 1Cr, 2Cr
    "retirement_age_target": None,       # not defined yet — engine flags this

    # Risk appetite
    "risk_appetite": "aggressive",
    "equity_allocation_target_pct": 85,
    "debt_allocation_target_pct": 10,
    "gold_allocation_target_pct": 5,
    "international_allocation_target_pct": 10,  # within equity
}


# ═══════════════════════════════════════════════════════════════════════════════
#  SECTION 2 — PORTFOLIO DATA
#  Replace dummy data with live Zerodha fetch (see kite_fetcher below).
#  Each fund entry must have all fields — engine will flag missing data.
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
        "oldest_unit_date": "2023-04-01",  # for exit load calculation
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
        "plan": "regular",                 # ⚠ FLAG: Regular plan
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
#  Pulls macro signals, news, RBI/SEBI updates, sector trends.
#  Uses Claude's web search or Perplexity API (free).
# ═══════════════════════════════════════════════════════════════════════════════

def fetch_market_context() -> dict:
    """
    Returns structured market context for the current month.
    In production: use web search API (Perplexity / Serper / Claude with search).
    Dummy data shown here for structure reference.
    """
    today = datetime.date.today()
    return {
        "date": str(today),
        "nifty50_pe": 22.4,              # Current P/E — above 24 = caution
        "nifty50_1m_return_pct": -2.1,   # Monthly return
        "nifty500_1m_return_pct": -2.8,
        "rbi_repo_rate_pct": 6.25,
        "rbi_stance": "neutral",         # hawkish / neutral / dovish
        "cpi_inflation_pct": 4.8,
        "fii_flow_monthly_cr": -4200,    # Negative = selling
        "dii_flow_monthly_cr": 6800,     # Positive = buying
        "india_gdp_growth_pct": 6.4,
        "usd_inr": 84.2,
        "india_vix": 15.4,               # Below 15 = calm; above 20 = fear
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
        "market_valuation_signal": "CAUTION",  # CHEAP / FAIR / CAUTION / EXPENSIVE
        "lump_sum_opportunity": False,   # True when market dips >15%
        "ltcg_harvest_window": True,     # Jan–Mar = ideal harvest window
        "ev_sector_sentiment": "BULLISH",  # Career resilience signal for Ankit
        "ev_hiring_trend": "SURGING",
    }


# ═══════════════════════════════════════════════════════════════════════════════
#  SECTION 4 — FUND DATA FETCHER
#  Fetches rolling returns, AUM, manager data, category rank from MFapi.in.
#  MFapi.in is free, no API key needed.
# ═══════════════════════════════════════════════════════════════════════════════

def fetch_fund_data(amfi_code: str) -> Optional[dict]:
    """
    Fetches NAV history from MFapi.in.
    Returns last 5 years of NAV data for rolling return calculation.
    """
    try:
        url = f"https://api.mfapi.in/mf/{amfi_code}"
        resp = requests.get(url, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            return {
                "fund_name": data.get("meta", {}).get("fund_name"),
                "scheme_type": data.get("meta", {}).get("scheme_type"),
                "nav_history": data.get("data", [])[:1825],  # 5 years
            }
    except Exception as e:
        print(f"[WARN] Fund data fetch failed for {amfi_code}: {e}")
    return None


def calculate_cagr(nav_history: list, years: int) -> Optional[float]:
    """
    Calculates CAGR over specified years from NAV history.
    nav_history is sorted newest-first from MFapi.in.
    """
    if not nav_history or len(nav_history) < years * 252:
        return None
    try:
        current_nav = float(nav_history[0]["nav"])
        past_nav = float(nav_history[min(years * 252, len(nav_history) - 1)]["nav"])
        cagr = ((current_nav / past_nav) ** (1 / years) - 1) * 100
        return round(cagr, 2)
    except:
        return None


def calculate_rolling_returns(nav_history: list, window_years: int = 3) -> dict:
    """
    Calculates rolling returns over the full available history.
    Returns min, max, median, and % of positive rolling periods.
    This is far more honest than point-to-point returns.
    """
    window_days = window_years * 252
    rolling = []
    for i in range(len(nav_history) - window_days):
        try:
            start_nav = float(nav_history[i + window_days]["nav"])
            end_nav = float(nav_history[i]["nav"])
            cagr = ((end_nav / start_nav) ** (1 / window_years) - 1) * 100
            rolling.append(round(cagr, 2))
        except:
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
#  Calculates true cost of exiting each fund considering:
#  exit load per SIP tranche, capital gains tax, opportunity cost, re-entry.
# ═══════════════════════════════════════════════════════════════════════════════

def calculate_exit_cost(fund: dict, investor: dict) -> dict:
    """
    Returns full exit cost breakdown for a fund.
    Accounts for per-unit exit load windows (each SIP has own 1-year clock).
    """
    today = datetime.date.today()
    sip_start = datetime.date.fromisoformat(fund["sip_start_date"])
    months_invested = (today.year - sip_start.year) * 12 + (today.month - sip_start.month)

    # Units still within exit load window (approx: last 12 months of SIPs)
    monthly_sip = fund["monthly_sip_inr"]
    units_in_window = min(12, months_invested)  # months of SIP within 1yr lock
    value_in_window = (units_in_window / max(months_invested, 1)) * fund["current_value_inr"]
    exit_load_cost = value_in_window * (fund["exit_load_pct"] / 100)

    # Capital gains
    total_gain = fund["current_value_inr"] - fund["invested_inr"]
    oldest_unit = datetime.date.fromisoformat(fund["oldest_unit_date"])
    days_held = (today - oldest_unit).days
    if days_held >= 365:
        # LTCG: 12.5% above ₹1.25L exemption
        taxable_gain = max(0, total_gain - (125000 - investor["ltcg_booked_this_year_inr"]))
        tax_cost = taxable_gain * 0.125
        tax_type = "LTCG @ 12.5%"
    else:
        # STCG: 20% flat
        tax_cost = total_gain * 0.20
        tax_type = "STCG @ 20%"

    # STT on redemption
    stt = fund["current_value_inr"] * 0.00001  # 0.001%

    # Stamp duty on reinvestment
    stamp_duty = fund["current_value_inr"] * 0.00005  # 0.005%

    # Opportunity cost (5 trading days out of market)
    daily_market_return = 0.12 / 252  # assuming 12% annual
    opportunity_cost = fund["current_value_inr"] * daily_market_return * 5

    total_exit_cost = exit_load_cost + tax_cost + stt + stamp_duty + opportunity_cost

    return {
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
#  Scores Ankit's personal financial health before even looking at funds.
#  Feeds directly into how aggressively the engine recommends changes.
# ═══════════════════════════════════════════════════════════════════════════════

def score_personal_risk(investor: dict, market: dict) -> dict:
    """
    Calculates personal financial health score with flags.
    Score: 0–10. Below 6 = engine takes defensive stance on recommendations.
    """
    score = 10.0
    flags = []
    positives = []

    # EMI-to-income ratio
    emi_ratio = investor["home_loan_emi_inr"] / investor["monthly_salary_inr"]
    if emi_ratio > 0.40:
        score -= 1.5
        flags.append(f"EMI-to-income ratio {emi_ratio*100:.1f}% — above safe 40% threshold")
    elif emi_ratio > 0.35:
        score -= 0.5
        flags.append(f"EMI-to-income ratio {emi_ratio*100:.1f}% — watch if rate rises")
    else:
        positives.append("EMI-to-income ratio healthy")

    # Emergency fund coverage
    monthly_burn = investor["home_loan_emi_inr"] + investor["monthly_living_expenses_inr"]
    ef_months = investor["emergency_fund_inr"] / monthly_burn
    if ef_months < 3:
        score -= 2.0
        flags.append(f"Emergency fund covers only {ef_months:.1f} months — critically low")
    elif ef_months < 6:
        score -= 0.8
        flags.append(f"Emergency fund at {ef_months:.1f} months — target is 6 months (₹{investor['emergency_fund_target_inr']:,})")
    else:
        positives.append(f"Emergency fund healthy at {ef_months:.1f} months")

    # Planned large expenses (car loan impact)
    for expense in investor["planned_expenses"]:
        if expense.get("likely_loan") and expense["months_away"] <= 18:
            future_emi_ratio = (investor["home_loan_emi_inr"] + expense["estimated_loan_emi"]) / investor["monthly_salary_inr"]
            if future_emi_ratio > 0.50:
                score -= 1.0
                flags.append(
                    f"Car loan adds ₹{expense['estimated_loan_emi']:,}/mo — "
                    f"total EMI will hit {future_emi_ratio*100:.0f}% of income in ~{expense['months_away']} months"
                )

    # Insurance adequacy
    if investor["term_cover_inr"] is None:
        score -= 0.5
        flags.append("Term insurance cover amount not verified — must exceed outstanding loan + 10yr income")

    # Career resilience (positive factor for Ankit)
    if investor["ev_domain_advantage"] and market["ev_sector_sentiment"] == "BULLISH":
        score = min(10, score + 0.5)
        positives.append("EV sector bullish — career income risk low, step-up likely")

    # RBI rate risk on floating loan
    if investor["home_loan_rate_pct"] > 8.0 and market["rbi_stance"] == "hawkish":
        score -= 0.3
        flags.append("Floating rate risk: RBI hawkish stance may push EMI higher")

    # Savings interest (positive — 7% is good)
    positives.append("Emergency fund earning 7% in savings account — optimally placed")

    return {
        "personal_risk_score": round(max(0, min(10, score)), 1),
        "flags": flags,
        "positives": positives,
        "emi_ratio_pct": round(emi_ratio * 100, 1),
        "emergency_fund_months": round(ef_months, 1),
        "monthly_burn_inr": monthly_burn,
    }


# ═══════════════════════════════════════════════════════════════════════════════
#  SECTION 7 — SIP STEP-UP CALCULATOR
#  Calculates personalised step-up guidance for April every year.
#  Not a flat 10% — tied to actual cash flow, career phase, buffer status.
# ═══════════════════════════════════════════════════════════════════════════════

def calculate_stepup_guidance(investor: dict, personal_risk: dict, market: dict) -> dict:
    """
    Returns personalised SIP step-up recommendation for the year.
    Phase 1: Buffer building — no step-up yet
    Phase 2: Buffer secured — 50% of net increment to SIP
    Phase 3: Career boom + hike >15% — step-up 60-70%
    """
    ef_months = personal_risk["emergency_fund_months"]
    career_boom = (market["ev_sector_sentiment"] == "BULLISH" and
                   market["ev_hiring_trend"] == "SURGING")

    # Phase determination
    if ef_months < 4:
        phase = 1
        phase_label = "Buffer Building"
        sip_increase_inr = 0
        buffer_monthly_addition = 10000
        reasoning = (
            f"Emergency fund at {ef_months:.1f} months. "
            f"Do not increase SIP yet. Direct ₹10,000/month to savings account "
            f"until buffer reaches ₹{investor['emergency_fund_target_inr']:,} "
            f"({6} months cover). Estimated time to complete: "
            f"{int((investor['emergency_fund_target_inr'] - investor['emergency_fund_inr']) / 10000)} months."
        )
    elif ef_months < 6:
        phase = 2
        phase_label = "Parallel Build"
        buffer_gap = investor["emergency_fund_target_inr"] - investor["emergency_fund_inr"]
        buffer_monthly_addition = 5000
        sip_increase_inr = 5000  # modest step-up while building buffer
        reasoning = (
            f"Buffer at {ef_months:.1f} months — close but not there. "
            f"Split increment: ₹5,000/month to buffer, ₹5,000/month to SIP. "
            f"Buffer closes in ~{int(buffer_gap / 5000)} months."
        )
    else:
        phase = 3
        phase_label = "Full Acceleration"
        buffer_monthly_addition = 0
        if career_boom:
            step_up_pct = 0.65
            reasoning = (
                "Buffer secured. EV sector surging — high probability of above-average "
                "increment this April. Step-up 65% of net salary increment to SIP. "
                "Reduce buffer addition to zero — deploy aggressively."
            )
        else:
            step_up_pct = 0.50
            reasoning = (
                "Buffer secured. Step-up 50% of net salary increment to SIP. "
                "Keep 50% as lifestyle/buffer top-up."
            )
        # Example: if ₹15k increment
        example_increment = 15000
        sip_increase_inr = int(example_increment * step_up_pct)

    # 15-year corpus projections at different SIP levels
    def corpus_at_15yr(monthly_sip, cagr=0.12):
        n = 15 * 12
        r = cagr / 12
        return monthly_sip * ((((1 + r) ** n) - 1) / r) * (1 + r)

    def real_corpus(nominal, inflation=0.06, years=15):
        return nominal / ((1 + inflation) ** years)

    current_sip = investor["monthly_sip_inr"]
    projections = {
        "flat_sip": {
            "sip_amount": current_sip,
            "corpus_15yr_nominal": round(corpus_at_15yr(current_sip)),
            "corpus_15yr_real": round(real_corpus(corpus_at_15yr(current_sip))),
        },
        "stepup_10pct_annual": {
            "note": "SIP grows 10% each year",
            "corpus_15yr_nominal": round(corpus_at_15yr(current_sip) * 2.1),  # approx multiplier
            "corpus_15yr_real": round(real_corpus(corpus_at_15yr(current_sip) * 2.1)),
        },
        "stepup_after_phase": {
            "sip_amount_after_stepup": current_sip + sip_increase_inr,
            "corpus_15yr_nominal": round(corpus_at_15yr(current_sip + sip_increase_inr)),
            "corpus_15yr_real": round(real_corpus(corpus_at_15yr(current_sip + sip_increase_inr))),
        },
    }

    return {
        "phase": phase,
        "phase_label": phase_label,
        "sip_increase_recommended_inr": sip_increase_inr,
        "buffer_monthly_addition_inr": buffer_monthly_addition,
        "reasoning": reasoning,
        "projections": projections,
        "tax_harvest_opportunity": market.get("ltcg_harvest_window", False),
        "ltcg_booked_inr": investor["ltcg_booked_this_year_inr"],
        "ltcg_headroom_inr": max(0, 125000 - investor["ltcg_booked_this_year_inr"]),
    }


# ═══════════════════════════════════════════════════════════════════════════════
#  SECTION 8 — HOME LOAN vs SIP ANALYSER
#  Monthly verdict: does prepaying one EMI beat investing that same amount?
#  This is the most underrated financial decision — now automated.
# ═══════════════════════════════════════════════════════════════════════════════

def analyse_loan_vs_sip(investor: dict) -> dict:
    """
    Compares benefit of ₹X prepayment vs ₹X investment at current market conditions.
    Returns monthly verdict with ₹ impact numbers.
    """
    loan_rate = investor["home_loan_rate_pct"] / 100
    months_remaining = investor["home_loan_months_remaining"]
    equity_expected_cagr = 0.12
    safe_return = 0.07  # savings account

    # Guaranteed return on prepayment = loan rate saved
    prepay_guaranteed_return = investor["home_loan_rate_pct"]

    # Expected equity return (uncertain, 12% assumption)
    equity_expected_return = equity_expected_cagr * 100

    # Post-tax equity return (LTCG 12.5% on gains above ₹1.25L)
    equity_post_tax = equity_expected_return * 0.875  # approximate

    # Verdict
    if equity_post_tax > prepay_guaranteed_return + 1.0:
        verdict = "INVEST"
        verdict_reason = (
            f"Equity post-tax return ({equity_post_tax:.1f}%) exceeds loan cost "
            f"({prepay_guaranteed_return}%) by {equity_post_tax - prepay_guaranteed_return:.1f}%. "
            f"Keep investing — prepayment not optimal right now."
        )
    elif abs(equity_post_tax - prepay_guaranteed_return) <= 1.0:
        verdict = "BALANCED"
        verdict_reason = (
            f"Returns nearly equal. Prepay only if psychological debt-free benefit "
            f"matters to you, or if RBI hike cycle restarts."
        )
    else:
        verdict = "PREPAY"
        verdict_reason = (
            f"Loan rate ({prepay_guaranteed_return}%) exceeds expected post-tax equity "
            f"({equity_post_tax:.1f}%). Extra cash better used in prepayment."
        )

    # Impact of one extra EMI prepayment
    extra_emi = investor["home_loan_emi_inr"]
    months_saved_approx = int(extra_emi / (investor["home_loan_emi_inr"] * 0.08))
    interest_saved = extra_emi * loan_rate * months_remaining / 12

    return {
        "verdict": verdict,
        "verdict_reason": verdict_reason,
        "loan_rate_pct": prepay_guaranteed_return,
        "equity_expected_post_tax_pct": round(equity_post_tax, 1),
        "one_extra_emi_interest_saved_inr": round(interest_saved, 0),
        "breakeven_equity_return_pct": round(prepay_guaranteed_return / 0.875, 1),
    }


# ═══════════════════════════════════════════════════════════════════════════════
#  SECTION 9 — PORTFOLIO OVERLAP ANALYSER
#  Detects stock-level overlap across funds.
#  Uses top holdings data — in production fetch from AMFI monthly disclosures.
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
    """
    Calculates pairwise and overall stock overlap across all funds.
    High overlap = you are not as diversified as you think.
    """
    overlap_matrix = {}
    all_stocks = {}

    for fund in portfolio:
        isin = fund["isin"]
        holdings = DUMMY_FUND_HOLDINGS.get(isin, [])
        for stock in holdings:
            all_stocks[stock] = all_stocks.get(stock, 0) + 1

    # Stocks held in 2+ funds
    duplicates = {s: c for s, c in all_stocks.items() if c >= 2}
    total_unique = len(all_stocks)
    overlap_pct = round(len(duplicates) / total_unique * 100, 1) if total_unique else 0

    # Pairwise
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
                "overlap_pct": round(len(common) / max(len(h1), 1) * 100, 1)
            }

    return {
        "overall_overlap_pct": overlap_pct,
        "duplicate_stocks": duplicates,
        "pairwise_overlap": overlap_matrix,
        "verdict": "HIGH" if overlap_pct > 50 else "MODERATE" if overlap_pct > 25 else "LOW",
    }


# ═══════════════════════════════════════════════════════════════════════════════
#  SECTION 10 — MASTER AI ANALYSIS PROMPT
#  This is the most critical function in the engine.
#  Sends ALL computed data to Claude for final synthesis and brutal honest verdict.
#  Prompt is engineered to prevent hallucination, enforce ₹-first outputs,
#  and mandate brutal honesty over feel-good advice.
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

    portfolio_json = json.dumps(portfolio, indent=2)
    market_json = json.dumps(market, indent=2)
    personal_risk_json = json.dumps(personal_risk, indent=2)
    step_up_json = json.dumps(step_up, indent=2)
    loan_sip_json = json.dumps(loan_vs_sip, indent=2)
    overlap_json = json.dumps(overlap, indent=2)
    exit_costs_json = json.dumps(exit_costs, indent=2)

    prompt = f"""
You are a brutally honest, independent financial analyst. You are NOT a salesperson.
Your single goal is to help {investor['name']} build maximum long-term wealth by
{investor['target_year']} (15 years from now) through optimal mutual fund strategy.

You will now receive structured data from a quantitative analysis engine.
Your job is to synthesise this into a FINAL ADVISORY REPORT.

════════════════════════════════════════════
INVESTOR PROFILE
════════════════════════════════════════════
Name          : {investor['name']}, Age {investor['age']}, Bangalore
Role          : {investor['role']}
Domain        : {investor['domain']}
Monthly income: ₹{investor['monthly_salary_inr']:,}
Home loan EMI : ₹{investor['home_loan_emi_inr']:,} ({investor['home_loan_months_remaining']} months left)
Monthly SIP   : ₹{investor['monthly_sip_inr']:,}
Free cash     : ₹{investor['free_cash_monthly_inr']:,}/month
Emergency fund: ₹{investor['emergency_fund_inr']:,} at 7% savings account
Tax slab      : {investor['income_tax_slab_pct']}%
Risk appetite : {investor['risk_appetite']}
Career edge   : EV domain — charging + thermal. This is a TAILWIND, not a risk.

PLANNED LARGE EXPENSES:
{json.dumps(investor['planned_expenses'], indent=2)}

════════════════════════════════════════════
PORTFOLIO DATA (from Zerodha / manual input)
════════════════════════════════════════════
{portfolio_json}

════════════════════════════════════════════
EXIT COST ANALYSIS (pre-computed per fund)
════════════════════════════════════════════
{exit_costs_json}

════════════════════════════════════════════
MARKET CONTEXT (current month)
════════════════════════════════════════════
{market_json}

════════════════════════════════════════════
PERSONAL RISK SCORE
════════════════════════════════════════════
{personal_risk_json}

════════════════════════════════════════════
SIP STEP-UP GUIDANCE (pre-calculated)
════════════════════════════════════════════
{step_up_json}

════════════════════════════════════════════
HOME LOAN vs SIP VERDICT (this month)
════════════════════════════════════════════
{loan_sip_json}

════════════════════════════════════════════
PORTFOLIO OVERLAP ANALYSIS
════════════════════════════════════════════
{overlap_json}

════════════════════════════════════════════
YOUR ANALYSIS MANDATE — 18 DIMENSIONS
════════════════════════════════════════════

Score EACH of the following and provide findings. Be specific, use ₹ figures not just %.

PILLAR 1 — PORTFOLIO QUALITY (weight: 30%)
  D1. Point-to-point returns vs benchmark (Nifty 50, Nifty 500, category average)
  D2. Rolling 3-year returns (min, max, median, % positive periods)
  D3. Risk-adjusted return (Sharpe ratio proxy: return / volatility)
  D4. Alpha generated vs category — is the manager earning their expense ratio?
  D5. Drawdown behaviour — max fall in 2020 / 2022 and recovery time

PILLAR 2 — COST INTELLIGENCE (weight: 20%)
  D6. Expense ratio vs category peers — flag if above median
  D7. Direct vs Regular plan — exact ₹ lost annually per fund
  D8. Exit cost reality — is switching worth it after all costs?
  D9. STT, stamp duty, opportunity cost — total real cost to exit/switch
  D10. Tax efficiency — LTCG harvest opportunity this month?

PILLAR 3 — PORTFOLIO CONSTRUCTION (weight: 15%)
  D11. Stock overlap % across funds — are you really diversified?
  D12. Sector concentration — same sector overweighted across multiple funds?
  D13. AMC concentration — more than 50% AUM in one AMC?
  D14. AUM size risk — is any fund too large for its category?
  D15. NFO / new fund trap — any fund under 3 years old with no stress-test data?

PILLAR 4 — PERSONAL RISK (weight: 25%)
  D16. EMI-to-income ratio vs safe threshold
  D17. Emergency fund adequacy — months of cover vs target
  D18. Car purchase impact — future EMI squeeze on SIP continuity

PILLAR 5 — BEHAVIOUR + STRATEGY (weight: 10%)
  D19. SIP continuity — any gaps or missed SIPs?
  D20. Step-up discipline — on track for wealth goal?
  D21. Global / gold diversification — INR concentration risk?
  D22. Home loan vs SIP — correct allocation this month?

════════════════════════════════════════════
OUTPUT FORMAT — MANDATORY STRUCTURE
════════════════════════════════════════════

Return your response as a valid JSON object with EXACTLY this structure:

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
      "verdict": "HOLD" | "WATCH" | "SWITCH" | "EXIT",
      "1yr_cagr_pct": <float or null>,
      "3yr_cagr_pct": <float or null>,
      "alpha_vs_category": "<positive/negative/neutral>",
      "expense_ratio_verdict": "<cheap/fair/expensive>",
      "manager_risk": "<low/medium/high>",
      "exit_cost_inr": <float>,
      "net_switch_benefit_inr": <float or null>,
      "key_finding": "<one brutal honest sentence>",
      "action": "<specific action in plain language>"
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
    "wealth_gap_assessment": "<honest gap assessment in plain language>"
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
    "april_action": "<specific instruction for April>",
    "reasoning": "<2-3 sentence explanation>"
  }},

  "loan_vs_sip_verdict": {{
    "verdict": "INVEST" | "PREPAY" | "BALANCED",
    "reasoning": "<plain language explanation with ₹ numbers>"
  }},

  "tax_harvest_alert": {{
    "opportunity_exists": <bool>,
    "ltcg_headroom_inr": <float>,
    "recommended_action": "<specific steps if opportunity exists>"
  }},

  "career_resilience_flag": {{
    "signal": "POSITIVE" | "NEUTRAL" | "NEGATIVE",
    "ev_sector_status": "<current EV sector outlook>",
    "income_risk_level": "<low/medium/high>",
    "implication_for_sip": "<how this affects investment aggression>"
  }},

  "brutal_honesty_block": [
    "<Finding 1 — most important, most uncomfortable truth>",
    "<Finding 2>",
    "<Finding 3>",
    "<Finding 4>",
    "<Finding 5 — only include if genuinely material>"
  ],

  "action_items": [
    {{
      "priority": "URGENT" | "ADVISORY" | "FYI",
      "action": "<specific action — what to do, how, by when>",
      "impact_inr": <estimated ₹ impact if acted upon>,
      "deadline": "<by when>"
    }}
  ],

  "no_action_flag": <bool>,
  "no_action_reason": "<only populate if no_action_flag is true>",

  "telegram_summary": "<60-word max plain-text summary for Telegram message. Start with score. One action line. No emojis. Brutally direct.>"
}}

════════════════════════════════════════════
RULES — NON-NEGOTIABLE
════════════════════════════════════════════

1. NEVER recommend a switch without netting exit costs first.
   A switch is only valid if NET BENEFIT > 1.5% annualised.

2. ALL monetary figures must be in ₹. No vague percentages alone.

3. If NO action is genuinely needed this month, set no_action_flag: true
   and say exactly that. Do not manufacture advice.

4. brutal_honesty_block must contain uncomfortable truths.
   If everything is fine, say so. If not, say exactly what is wrong.

5. projection must use REAL (inflation-adjusted) corpus figures
   alongside nominal. Use 6% inflation assumption.

6. Do NOT recommend adding new funds unless current portfolio has
   a genuine structural gap (e.g., zero international, zero gold).

7. Score the portfolio as if you are being paid to find problems,
   not to make the investor feel good.

8. The career resilience flag must reflect {investor['name']}'s specific
   EV domain advantage — this is a real positive signal.

9. Return ONLY valid JSON. No prose before or after. No markdown fences.
"""
    return prompt.strip()


# ═══════════════════════════════════════════════════════════════════════════════
#  SECTION 11 — AI API CALLER
#  Calls Claude API with the master prompt.
#  Falls back to Gemini free tier if Claude quota exceeded.
# ═══════════════════════════════════════════════════════════════════════════════

def call_claude_api(prompt: str) -> Optional[dict]:
    """
    Calls Claude gemini-2.0-flash with the master analysis prompt.
    Returns parsed JSON report or None on failure.
    """
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        print("[ERROR] ANTHROPIC_API_KEY not set")
        return None

    try:
        response = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": api_key,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            json={
                "model": "gemini-2.0-flash",
                "max_tokens": 4096,
                "messages": [{"role": "user", "content": prompt}],
                "system": (
                    "You are a brutally honest financial analyst. "
                    "You return ONLY valid JSON. No markdown, no prose. "
                    "Every monetary figure in Indian Rupees. "
                    "Your job is wealth maximisation, not comfort."
                ),
            },
            timeout=60,
        )
        response.raise_for_status()
        content = response.json()["content"][0]["text"].strip()
        # Strip markdown fences if present
        if content.startswith("```"):
            content = content.split("```")[1]
            if content.startswith("json"):
                content = content[4:]
        return json.loads(content)
    except Exception as e:
        print(f"[ERROR] Claude API call failed: {e}")
        return None

def call_gemini_api(prompt: str) -> Optional[dict]:
    """
    Calls Gemini 2.0 Flash with retry logic for rate limits.
    Free tier: 15 RPM, 1M TPM. Our prompt is large so we retry with backoff.
    """
    import time
    api_key = os.environ.get("GEMINI_API_KEY")
    if not api_key:
        return None

    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key={api_key}"

    payload = {
        "contents": [{
            "parts": [{"text": prompt}]
        }],
        "generationConfig": {
            "temperature": 0.2,
            "maxOutputTokens": 8192,
        },
        "systemInstruction": {
            "parts": [{"text": (
                "You are a brutally honest financial analyst. "
                "Return ONLY valid JSON. No markdown fences, no prose. "
                "Every monetary figure in Indian Rupees. "
                "Your job is wealth maximisation, not comfort."
            )}]
        }
    }

    max_retries = 3
    for attempt in range(max_retries):
        try:
            print(f"      Gemini attempt {attempt + 1}/{max_retries}...")
            resp = requests.post(url, json=payload, timeout=120)

            if resp.status_code == 429:
                wait = 30 * (attempt + 1)  # 30s, 60s, 90s
                print(f"      Rate limit hit — waiting {wait}s before retry...")
                time.sleep(wait)
                continue

            resp.raise_for_status()
            content = resp.json()["candidates"][0]["content"]["parts"][0]["text"].strip()

            # Strip markdown fences if present
            if "```" in content:
                parts = content.split("```")
                for part in parts:
                    if part.startswith("json"):
                        content = part[4:].strip()
                        break
                    elif "{" in part:
                        content = part.strip()
                        break

            # Find JSON object boundaries
            start = content.find("{")
            end = content.rfind("}") + 1
            if start != -1 and end > start:
                content = content[start:end]

            return json.loads(content)

        except json.JSONDecodeError as e:
            print(f"      [WARN] JSON parse failed attempt {attempt+1}: {e}")
            if attempt < max_retries - 1:
                time.sleep(20)
            continue
        except Exception as e:
            print(f"      [ERROR] Gemini attempt {attempt+1} failed: {e}")
            if attempt < max_retries - 1:
                time.sleep(20)
            continue

    print("[ERROR] All Gemini retries exhausted")
    return None

# ═══════════════════════════════════════════════════════════════════════════════
#  SECTION 12 — TELEGRAM MESSAGE FORMATTER
#  Formats the AI report into a clean, scannable Telegram message.
#  Rule: readable in under 60 seconds on mobile. No waffle.
# ═══════════════════════════════════════════════════════════════════════════════

def format_telegram_message(report: dict, investor: dict, dashboard_url: str = None) -> str:
    """
    Formats the AI analysis report as a clean Telegram message.
    Structure: Score → Key Findings → Actions → Projection → Link
    """
    today = datetime.date.today().strftime("%b %Y")
    score = report.get("master_score", "N/A")
    score_reason = report.get("master_score_reasoning", "")

    # Score indicator
    if isinstance(score, (int, float)):
        if score >= 8:
            indicator = "STRONG"
        elif score >= 6:
            indicator = "ADEQUATE"
        elif score >= 4:
            indicator = "WEAK"
        else:
            indicator = "CRITICAL"
    else:
        indicator = "N/A"

    lines = [
        f"MF ADVISOR REPORT — {today}",
        f"{'─' * 32}",
        f"PORTFOLIO SCORE: {score}/10 ({indicator})",
        f"{score_reason}",
        "",
    ]

    # No action case
    if report.get("no_action_flag"):
        lines += [
            "VERDICT: NO ACTION REQUIRED THIS MONTH",
            report.get("no_action_reason", ""),
            "",
        ]
    else:
        # Fund verdicts
        lines.append("FUND VERDICTS:")
        for f in report.get("fund_ratings", []):
            verdict_sym = {"HOLD": "ok", "WATCH": "!!", "SWITCH": "!!", "EXIT": "XX"}.get(f["verdict"], "--")
            lines.append(f"  [{verdict_sym}] {f['fund_name'][:30]} — {f['score']}/10 — {f['verdict']}")
        lines.append("")

        # Brutal honesty (top 3)
        lines.append("BRUTAL FINDINGS:")
        for i, finding in enumerate(report.get("brutal_honesty_block", [])[:3], 1):
            lines.append(f"  {i}. {finding}")
        lines.append("")

        # Actions
        lines.append("ACTIONS THIS MONTH:")
        for item in report.get("action_items", []):
            tag = item.get("priority", "FYI")
            lines.append(f"  [{tag}] {item['action']}")
            if item.get("impact_inr"):
                lines.append(f"         Impact: ~Rs {item['impact_inr']:,.0f}")
        lines.append("")

    # Projection snapshot
    proj = report.get("projection", {})
    if proj:
        flat = proj.get("corpus_at_15yr_flat_sip_real_inr", 0)
        stepup = proj.get("corpus_at_15yr_stepup_real_inr", 0)
        lines += [
            "15-YEAR PROJECTION (inflation-adjusted):",
            f"  Flat SIP Rs 20k  : Rs {flat:,.0f}",
            f"  With step-up     : Rs {stepup:,.0f}",
            "",
        ]

    # SIP step-up
    su = report.get("stepup_guidance", {})
    if su and su.get("recommended_sip_increase_inr", 0) > 0:
        lines.append(f"APRIL STEP-UP: Increase SIP by Rs {su['recommended_sip_increase_inr']:,}/month")
        lines.append(f"  {su.get('april_action', '')}")
        lines.append("")

    # Career flag
    career = report.get("career_resilience_flag", {})
    if career:
        lines.append(f"CAREER SIGNAL: {career.get('signal', 'N/A')} — {career.get('ev_sector_status', '')}")
        lines.append("")

    # Tax harvest
    tax = report.get("tax_harvest_alert", {})
    if tax.get("opportunity_exists"):
        lines.append(f"TAX HARVEST ALERT: Book Rs {tax.get('ltcg_headroom_inr',0):,.0f} LTCG tax-free this month")
        lines.append(f"  {tax.get('recommended_action', '')}")
        lines.append("")

    # Dashboard link
    if dashboard_url:
        lines += [
            f"{'─' * 32}",
            f"Full report: {dashboard_url}",
        ]

    lines += [
        f"{'─' * 32}",
        f"Next report: {(datetime.date.today().replace(day=1) + datetime.timedelta(days=32)).replace(day=1) - datetime.timedelta(days=1)}",
        "Engine: MF AI Advisor v1.0",
    ]

    return "\n".join(lines)


# ═══════════════════════════════════════════════════════════════════════════════
#  SECTION 13 — TELEGRAM SENDER
# ═══════════════════════════════════════════════════════════════════════════════

def send_telegram_message(message: str) -> bool:
    """
    Sends message to your Telegram chat.
    Set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID as env variables.
    Get bot token from @BotFather. Get chat ID from @userinfobot.
    """
    bot_token = os.environ.get("TELEGRAM_BOT_TOKEN")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID")

    if not bot_token or not chat_id:
        print("[WARN] Telegram credentials not set — printing to console instead")
        print("\n" + "═" * 50)
        print(message)
        print("═" * 50 + "\n")
        return False

    try:
        url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        response = requests.post(url, json={
            "chat_id": chat_id,
            "text": message,
            "parse_mode": "HTML",
        }, timeout=15)
        response.raise_for_status()
        print("[OK] Telegram message sent successfully")
        return True
    except Exception as e:
        print(f"[ERROR] Telegram send failed: {e}")
        return False


# ═══════════════════════════════════════════════════════════════════════════════
#  SECTION 14 — REPORT SAVER
#  Saves full JSON report locally for dashboard and history tracking.
#  History enables month-over-month score trend tracking.
# ═══════════════════════════════════════════════════════════════════════════════

def save_report(report: dict, portfolio: list, market: dict) -> str:
    """
    Saves full report to JSON file for dashboard consumption and history.
    Returns the saved file path.
    """
    today = datetime.date.today()
    reports_dir = "reports"
    os.makedirs(reports_dir, exist_ok=True)

    filename = f"{reports_dir}/report_{today.strftime('%Y_%m')}.json"
    full_report = {
        "meta": {
            "generated_at": str(datetime.datetime.now()),
            "engine_version": "1.0.0",
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
    """
    Loads past 12 months of scores for trend analysis.
    """
    reports_dir = "reports"
    history = []
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
            except:
                continue
    return history[-12:]  # last 12 months


# ═══════════════════════════════════════════════════════════════════════════════
#  SECTION 15 — ZERODHA KITE FETCHER (Optional Live Data)
#  Replaces dummy portfolio with real Zerodha holdings.
#  Access token must be refreshed manually once per month (30 sec job).
# ═══════════════════════════════════════════════════════════════════════════════

def fetch_zerodha_portfolio() -> Optional[list]:
    """
    Fetches live MF holdings from Zerodha Kite API.
    Requires valid KITE_ACCESS_TOKEN env variable.

    TOKEN REFRESH:
    1. Run kite_auth_helper.py once a month
    2. It opens browser → you log in → token is saved to .env
    3. This function then uses it automatically

    Returns portfolio list in same format as DUMMY_PORTFOLIO.
    """
    api_key = os.environ.get("KITE_API_KEY")
    access_token = os.environ.get("KITE_ACCESS_TOKEN")

    if not api_key or not access_token:
        print("[INFO] Kite credentials not set — using dummy portfolio")
        return None

    try:
        headers = {
            "X-Kite-Version": "3",
            "Authorization": f"token {api_key}:{access_token}",
        }

        # Fetch MF holdings via Coin (Zerodha's MF platform)
        resp = requests.get(
            "https://api.kite.trade/mf/holdings",
            headers=headers,
            timeout=15,
        )
        resp.raise_for_status()
        kite_holdings = resp.json().get("data", [])

        # Map Kite format to engine format
        portfolio = []
        for h in kite_holdings:
            portfolio.append({
                "fund_name": h.get("fund"),
                "isin": h.get("isin", ""),
                "amfi_code": h.get("folio_no", ""),  # use ISIN→AMFI lookup in production
                "category": "Unknown",  # enrich from MFapi.in
                "amc": h.get("fund", "").split(" ")[0],
                "plan": "direct",  # Coin is always direct
                "units": float(h.get("quantity", 0)),
                "avg_nav": float(h.get("average_price", 0)),
                "current_nav": float(h.get("last_price", 0)),
                "invested_inr": float(h.get("amount", 0)),
                "current_value_inr": float(h.get("last_price", 0)) * float(h.get("quantity", 0)),
                "monthly_sip_inr": 0,  # fetch from SIP orders separately
                "sip_start_date": str(datetime.date.today()),
                "oldest_unit_date": str(datetime.date.today()),
                "expense_ratio_pct": 0,   # enrich from MFapi.in
                "exit_load_pct": 1.0,
                "exit_load_window_days": 365,
            })
        print(f"[OK] Fetched {len(portfolio)} holdings from Zerodha")
        return portfolio if portfolio else None

    except Exception as e:
        print(f"[WARN] Zerodha fetch failed: {e} — falling back to dummy data")
        return None


# ═══════════════════════════════════════════════════════════════════════════════
#  SECTION 16 — MAIN ORCHESTRATOR
#  This is what GitHub Actions / cron calls on the last day of each month.
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    print("\n" + "═" * 60)
    print("  MF AI WEALTH ADVISOR — MONTHLY RUN")
    print(f"  {datetime.datetime.now().strftime('%d %B %Y, %H:%M')}")
    print("═" * 60 + "\n")

    # ── Step 1: Load portfolio (live or dummy)
    print("[1/8] Loading portfolio...")
    portfolio = fetch_zerodha_portfolio() or DUMMY_PORTFOLIO
    print(f"      {len(portfolio)} funds loaded")

    # ── Step 2: Fetch market context
    print("[2/8] Fetching market context...")
    market = fetch_market_context()
    print(f"      Nifty P/E: {market['nifty50_pe']} | FII: ₹{market['fii_flow_monthly_cr']}Cr | RBI: {market['rbi_stance']}")

    # ── Step 3: Compute exit costs
    print("[3/8] Calculating exit costs per fund...")
    exit_costs = []
    for fund in portfolio:
        cost = calculate_exit_cost(fund, INVESTOR_PROFILE)
        cost["fund_name"] = fund["fund_name"]
        exit_costs.append(cost)
        print(f"      {fund['fund_name'][:35]}: exit cost ₹{cost['total_exit_cost_inr']:,.0f} ({cost['total_exit_cost_pct']}%)")

    # ── Step 4: Personal risk score
    print("[4/8] Scoring personal financial health...")
    personal_risk = score_personal_risk(INVESTOR_PROFILE, market)
    print(f"      Personal risk score: {personal_risk['personal_risk_score']}/10")
    for flag in personal_risk["flags"]:
        print(f"      FLAG: {flag}")

    # ── Step 5: Step-up guidance
    print("[5/8] Calculating SIP step-up guidance...")
    step_up = calculate_stepup_guidance(INVESTOR_PROFILE, personal_risk, market)
    print(f"      Phase {step_up['phase']} ({step_up['phase_label']}) | Recommended increase: ₹{step_up['sip_increase_recommended_inr']:,}")

    # ── Step 6: Loan vs SIP
    print("[6/8] Running loan vs SIP analysis...")
    loan_vs_sip = analyse_loan_vs_sip(INVESTOR_PROFILE)
    print(f"      Verdict: {loan_vs_sip['verdict']} — {loan_vs_sip['verdict_reason'][:60]}...")

    # ── Step 7: Portfolio overlap
    print("[7/8] Analysing portfolio overlap...")
    overlap = calculate_portfolio_overlap(portfolio)
    print(f"      Overlap: {overlap['overall_overlap_pct']}% ({overlap['verdict']})")

    # ── Step 8: AI synthesis
    print("[8/8] Running AI analysis (Claude)...")
    prompt = build_master_prompt(
        INVESTOR_PROFILE, portfolio, market,
        personal_risk, step_up, loan_vs_sip, overlap, exit_costs
    )

    report = call_gemini_api(prompt)
    if not report:
        print("[ERROR] AI calls failed — aborting")
        return

    print(f"\n      Master score: {report.get('master_score')}/10")
    print(f"      Action items: {len(report.get('action_items', []))}")
    print(f"      No action needed: {report.get('no_action_flag', False)}")

    # ── Save report
    saved_path = save_report(report, portfolio, market)

    # ── Format and send Telegram
    dashboard_url = os.environ.get("DASHBOARD_URL", "")  # e.g. https://yoursite.com/report
    telegram_msg = format_telegram_message(report, INVESTOR_PROFILE, dashboard_url)
    send_telegram_message(telegram_msg)

    # ── Score history
    history = load_score_history()
    if len(history) > 1:
        prev_score = history[-2].get("score", 0)
        curr_score = report.get("master_score", 0)
        trend = "UP" if curr_score > prev_score else "DOWN" if curr_score < prev_score else "FLAT"
        print(f"\n  Score trend: {prev_score} → {curr_score} ({trend})")

    print("\n" + "═" * 60)
    print("  MONTHLY RUN COMPLETE")
    print("═" * 60 + "\n")


if __name__ == "__main__":
    main()
