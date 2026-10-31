# OxyPulse Architecture

Last updated: 2026-04-24

## 1. System purpose

OxyPulse is a Telegram-operated OKX futures trading system. It combines autonomous scanning, multi-engine trade selection, risk and exposure controls, exchange execution, persistent state, exit automation, and optional AI-assisted analysis.

The control surface is Telegram, but the real system is the runtime behind it:
- orchestration
- candidate selection
- risk/admission gating
- execution routing
- exit management
- persistent recovery state
- diagnostics and operator tooling

## 2. Main control flow

```text
Telegram command / callback / AI chat
    -> bot.py
    -> trading_engine.py orchestration
    -> category_engine.py candidate scoring + admission
    -> risk / exposure / execution feasibility checks
    -> okx_client.py exchange execution
    -> exit_manager.py trade lifecycle management
    -> notifier.py Telegram lifecycle alerts
```

## 3. Primary modules

### 3.1 Control plane

#### `bot.py`
Main runtime entrypoint.
Responsibilities:
- loads environment config
- builds the Telegram application
- registers command handlers and callbacks
- routes manual operator actions
- routes free-form text into the AI command layer when enabled
- schedules background jobs such as summary/report checks

#### `tg_panels.py`
Telegram UI composition.
Responsibilities:
- panel text
- inline keyboards
- settings navigation
- engine dashboards
- help/menu rendering

### 3.2 Orchestration and decisioning

#### `trading_engine.py`
Global runtime coordinator.
Responsibilities:
- start/stop the autonomous engine loops
- run scan cycles
- coordinate sync and recovery work
- manage engine orchestration and runtime status
- expose operator-facing runtime actions such as close/sync/status

#### `category_engine.py`
Main decision pipeline for new autonomous entries.
Responsibilities:
- candidate ranking
- score and quality evaluation
- admission gating
- slot logic
- replacement policy behavior
- execution handoff
- rejection tagging for diagnostics

This is the heaviest policy surface in the project.

### 3.3 Exchange and execution

#### `okx_client.py`
OKX exchange wrapper.
Responsibilities:
- balances
- positions
- tickers / market data endpoints used by the bot
- leverage setup / fallback handling
- order placement
- close flows
- fill lookups and some exchange-side safety actions

#### `execution_router.py`
Chooses how to execute an admitted trade.
Responsibilities:
- market vs staged execution planning
- execution style selection
- keeping execution strategy separate from admission policy

#### `execution_admission.py`
Execution feasibility guardrail.
Responsibilities:
- symbol cooldowns
- repeated exchange failure suppression
- preflight feasibility checks before final order placement

### 3.4 Exits and position state

#### `exit_manager.py`
Close authority for bot-managed positions.
Responsibilities:
- stop-loss handling
- TP milestone handling
- trailing logic
- breakeven / profit-lock logic
- exchange-side close detection
- close deduplication and lifecycle completion

#### `position_registry.py`
Persistent open-position registry.
Responsibilities:
- register open positions
- update / release local state
- persist recovery metadata
- support restart recovery and UI visibility

### 3.5 Settings, stats, and diagnostics

#### `settings_store.py`
Canonical runtime settings layer.
Responsibilities:
- persist settings to `data/settings.json`
- expose get/set access to runtime modules
- support hot-runtime changes for curated keys
- append operator changes to `data/change_journal.jsonl`

#### `performance_tracker.py`
Performance and drawdown state.
Responsibilities:
- record closed trades
- compute rolling stats
- expose PnL and drawdown metrics

#### `category_tracker.py`
Per-engine performance state.
Responsibilities:
- track win/loss/PnL per engine
- rolling win rate reporting
- engine reports for Telegram panels

#### `rejection_log.py`
Recent blocked-candidate diagnostics.
Responsibilities:
- store structured rejection events
- expose operator insight into why candidates were blocked

#### `sync_health.py`
Bot-vs-exchange sync health helpers.
Responsibilities:
- detect drift and sync quality issues
- support recovery reasoning and diagnostics

## 4. Strategy and signal stack

Relevant files:
- `strategy_categories.py`
- `strategy_profiles.py`
- `signal_consensus.py`
- `confidence_engine.py`
- `ai_signal_engine.py`
- `indicators.py`
- `regime_detector.py`
- `session_context.py`
- `market_mode.py`
- `coin_universe.py`
- `market_filter.py`
- `entry_filter.py`

These modules shape:
- market structure interpretation
- indicator scoring
- confidence and consensus
- regime context
- candidate universe selection
- pre-admission filtering

## 5. Capital, sizing, and exposure stack

Relevant files:
- `risk_manager.py`
- `capital_manager.py`
- `sizing_engine.py`
- `exposure_tracker.py`
- `portfolio_allocator.py`
- `weekly_cap.py`
- `drawdown_guard.py`
- `trade_intent.py`

These modules shape:
- portfolio risk limits
- deployable capital and slot posture
- trade notional sizing
- correlation/exposure governance
- weekly/drawdown behavior
- strategy posture under stress or recovery modes

## 6. Optional AI layer

Relevant files:
- `ai_client.py`
- `ai_commander.py`
- `ai_analyst.py`
- `ai_gatekeeper.py`
- `ai_signal_engine.py`
- `ai_journal.py`

AI is optional and should be advisory or assistive, not the trust boundary for execution.

Current AI use cases include:
- free-form command interpretation
- coin analysis / thesis generation
- AI proposal workflows
- optional signal augmentation
- AI journaling / analysis trails

## 7. Current engine set

Defined in `category_tracker.py`:
- `RYUU`
- `RAIJIN`
- `KAGE`
- `ARASHI`
- `MUSASHI`
- `SHOGUN`
- `TENJIN`
- `IZANAGI`
- `SUSANOO`
- `FUJIN`
- `AMATERASU`
- `TITAN`
- `RONIN`
- `OROCHI`
- `TSUNAMI`
- `SUIJIN`
- `RAIJUU`
- `ENMA`
- `HACHIMAN`
- `HAYATE`
- `KAGUTSUCHI`
- `YATAGARASU`
- `RYUJIN`
- `TENSEI`
- `IBUKI`
- `ONRYO`
- `KANNON`
- `GENBU`

Total in current code: 28 engines.

## 8. Persistent state

Main runtime state under `data/`:
- `settings.json` - canonical persisted runtime config
- `change_journal.jsonl` - append-only operator setting change journal
- `position_registry.json` - open-position registry and recovery state
- `category_stats.json` - per-engine trading stats
- `performance.json` - global performance state
- `engine_weights.json` - weight/adaptation state
- `coin_guard_state.json` - coin cooldown and guard state

Other runtime artifacts:
- `logs/`
- `output/`
- `oxypulse_journal.db`

## 9. Operator surface

Primary Telegram commands in `bot.py`:
- `/start`, `/help`, `/status`
- `/startbot`, `/stopbot`, `/stop`
- `/balance`, `/positions`, `/pnl`, `/p`
- `/trades`, `/engines`, `/scan`, `/universe`
- `/close`, `/closeall`, `/sync`
- `/settings`, `/clearbs`, `/reset`
- `/winrate`, `/v6`, `/blocked`
- `/analyze`, `/aisignal`, `/aiscan`
- per-engine command shortcuts from the live engine list

## 10. Design realities

A few important truths about this repo:
- the codebase moved faster than older docs
- older files referring to 19 or 20 engines are stale
- `category_engine.py` is the densest policy surface and should be treated carefully
- live runtime behavior is shaped not just by code, but by `data/settings.json`
- Telegram is the operator UI, but the real system state lives across runtime modules plus persisted data

## 11. Practical reading order

If you need to re-understand the project quickly, read in this order:
1. `README.md`
2. `PROJECT_SETUP_AND_FLOW.txt`
3. `OxyPulse_Handoff_Brief.md`
4. `bot.py`
5. `trading_engine.py`
6. `category_engine.py`
7. `okx_client.py`
8. `exit_manager.py`
9. `settings_store.py`
10. `position_registry.py`

## Runtime Notes

- The live decision path now records a compact decision trace for admitted, skipped, soft-accepted, and replaced candidates.
- AI is routed through a provider abstraction with OpenAI/OpenRouter selection and an Off mode.
- Telegram diagnostics now include Decision Trace, API Health, Control State, and Engines.
- Size controls are live and affect new trades through the Capital -> Size rooms.
- The current engine count is 28.

