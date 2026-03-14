# Product Vision — Real Madrid Intelligence Platform

## The Goal
A personal Notion dashboard that combines match data, player performance,
and media/social sentiment to predict Real Madrid match outcomes and understand
the team ecosystem at a glance.

---

## Dashboard Structure (Notion)

### 1. Match Prediction View
Per upcoming match, display:
- Historical head-to-head record
- Current form (last 5 matches: W/D/L)
- Home vs away performance split
- xG (expected goals) trend — last 5 matches
- Key player availability (injuries/suspensions)
- Predicted outcome: Win / Draw / Loss + confidence %
- Odds comparison (optional)

### 2. Player Intelligence View
Per player, per week:
- Match rating trend (FotMob ratings over time)
- Minutes played / position
- Current media volume score: how many articles/posts mention this player
  compared to the previous 7 days (↑ or ↓ with % change)
- Sentiment tag: Positive / Neutral / Negative (based on article tone)
- Why are they being talked about? (top headline or key topic cluster)
- Example: "Vinicius Jr. ↑ 340% mentions this week — penalty controversy vs Atletico"

### 3. Team Ecosystem View
News broken down by entity:
- **Player-level:** Injury news, transfer rumors, form commentary, disciplinary
- **Manager-level:** Tactical changes, press conference sentiment, rotation hints
- **Club-level:** Financial news, board decisions, Champions League draw, fan sentiment
- Each story tagged: Player | Manager | Club | Transfer | Injury | Match Preview | Match Review

### 4. Per-Match Intelligence Card
For each match (past or upcoming):
- Match result + scoreline
- Key stats (shots, possession, xG, passes)
- Player ratings table (FotMob)
- Top news stories published within 72h before and after the match
- Social media buzz spike: did conversation volume spike before the match? Why?
- Notable quotes (manager/players from press conferences)

---

## Data Sources to Build

### Already Built
- FotMob match data (JSON → S3 → DuckDB via dbt)
  - Match facts, scores, formations
  - Player ratings and lineups
  - Match statistics (shots, possession, etc.)

### To Build: News & Media Layer

#### Source Options
| Source | Type | API/Method | Priority |
|--------|------|-----------|----------|
| NewsAPI.org | News articles | REST API (free tier: 100 req/day) | High |
| The Guardian | Football news | Open Platform API (free) | High |
| BBC Sport | Articles | RSS feed | Medium |
| Sky Sports | Articles | RSS feed | Medium |
| Twitter/X | Social media | v2 API (limited free) or scraping | Medium |
| Reddit r/realmadrid | Fan sentiment | Reddit API (PRAW, free) | Medium |
| Google News | Aggregated | RSS or SerpAPI | Low |

#### What to Extract Per Article/Post
- `entity` — which player/manager/club the story is about (NER tagging)
- `topic_tag` — Injury | Transfer | Match Preview | Match Review | Controversy | Form
- `sentiment` — Positive / Neutral / Negative (basic NLP)
- `published_at` — timestamp
- `source` — outlet name
- `headline` + `summary`
- `match_id` — link to a specific match if applicable (±72h window)

#### Volume Tracking (Buzz Score)
For each player/manager/club entity:
- Count mentions in the last 7 days vs previous 7 days
- Calculate % change → display as ↑ or ↓ with reason
- Flag outliers (>200% spike) as "Trending"

---

## Proposed Architecture Extension

```
Existing:
  S3 (FotMob JSON) → Airflow → DuckDB → dbt → mart tables

New layer to add:
  News APIs / RSS Feeds
  Reddit API
  Twitter API (optional)
       ↓
  Airflow DAG: extract_media_data (runs daily)
       ↓
  S3: raw/news/{source}/{date}/{article_id}.json
       ↓
  dbt: stg_raw_news → stg_news_entities → mart_media_intelligence
       ↓
  mart_player_buzz (weekly volume + sentiment per player)
  mart_match_context (news linked to each match)
       ↓
  Notion API sync (Python script or Airflow task)
       ↓
  Notion Dashboard
```

---

## Notion Integration
- Use the Notion API to write data to a Notion database
- Each "Match" is a Notion page with properties populated from the mart tables
- Each "Player" is a linked Notion page with their buzz score updated weekly
- Automation: Airflow task runs after dbt build to push latest data to Notion

---

## ML Prediction Layer (Future)
Once enough data is collected:
- Features: rolling xG, form, rest days, opponent strength, home/away, media sentiment
- Model: XGBoost or LightGBM classifier (Win/Draw/Loss)
- Validation: backtesting on 2021–2024 seasons
- Output: probability scores written back to Notion match cards

---

## Implementation Phases

### Phase 1 (Current) — Foundation ✅
- FotMob extraction pipeline (Airflow + S3)
- dbt transformation (staging + marts in DuckDB)
- Basic match, player, and stats data available

### Phase 2 — Fix & Stabilize
- Fix IP rotator so Airflow extraction actually works
- Confirm all 4 seasons are fully populated in S3
- Add 2025/2026 season to config
- Schedule dbt to run after extraction completes (Airflow task dependency)

### Phase 3 — Media Layer
- Build `extract_media_data` Airflow DAG
- Integrate NewsAPI + Guardian API + Reddit
- Add NER entity tagging (spaCy or simple keyword matching)
- Add sentiment analysis (TextBlob or Hugging Face zero-shot)
- dbt models: `stg_raw_news`, `stg_news_entities`, `mart_player_buzz`, `mart_match_context`

### Phase 4 — Notion Dashboard
- Notion API integration
- Automated daily sync
- Match prediction cards
- Player buzz tracker

### Phase 5 — ML Prediction
- Feature engineering in dbt
- Model training + backtesting
- Prediction scores → Notion
