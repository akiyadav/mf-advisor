# MF AI Advisor — Setup Guide

## Files in this package
```
mf_ai_engine.py          ← MAIN ENGINE (this is what runs monthly)
kite_auth_helper.py      ← Run once/month to refresh Zerodha token
.github/workflows/
  monthly_run.yml        ← GitHub Actions cron (auto-triggers last day of month)
.env.example             ← Copy to .env and fill in your keys
requirements.txt         ← Python dependencies
```

## Step 1 — Install dependencies
```bash
pip install requests anthropic kiteconnect python-dotenv
```

## Step 2 — Set up .env file
Copy `.env.example` to `.env` and fill in:
```
ANTHROPIC_API_KEY=sk-ant-...
GEMINI_API_KEY=AIza...          # optional fallback
TELEGRAM_BOT_TOKEN=123456:ABC...
TELEGRAM_CHAT_ID=987654321
KITE_API_KEY=your_kite_api_key
KITE_API_SECRET=your_kite_secret
KITE_ACCESS_TOKEN=              # filled by kite_auth_helper.py monthly
DASHBOARD_URL=                  # optional: URL of your HTML dashboard
```

## Step 3 — Get Telegram credentials
1. Message @BotFather on Telegram → /newbot → get BOT_TOKEN
2. Message @userinfobot on Telegram → get your CHAT_ID

## Step 4 — Get Zerodha Kite API access
1. Go to developers.kite.trade → create app
2. Get API_KEY and API_SECRET
3. Run `python kite_auth_helper.py` once to get first access token

## Step 5 — Test run with dummy data
```bash
python mf_ai_engine.py
```
Engine runs with dummy portfolio. Check console + Telegram message.

## Step 6 — Switch to real data
Add your real Zerodha API keys to .env.
Run `python kite_auth_helper.py` once a month before the engine runs.

## Step 7 — Deploy to GitHub Actions (auto monthly run)
1. Push this repo to GitHub (private repo)
2. Go to Settings → Secrets → Actions
3. Add all .env variables as Repository Secrets
4. The workflow triggers automatically on last day of each month
5. You can also trigger manually from Actions tab anytime

## Monthly routine (30 seconds)
1. Last day of month: Telegram message arrives automatically
2. If action needed: tap dashboard link for full report
3. First week of month: run `python kite_auth_helper.py` to refresh Zerodha token

## Cost
- Anthropic API: ~₹5-10/month (1 run/month, ~4000 tokens)
- Gemini API: Free fallback
- Telegram Bot: Free
- GitHub Actions: Free (well within free tier)
- Zerodha Kite API: Free for personal use
TOTAL: ₹5-10/month or ₹0 if using Gemini only
