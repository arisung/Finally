# FinAlly — AI Trading Workstation

FinAlly is a visually stunning, AI-powered trading workstation: it streams live market data, lets you trade a simulated portfolio, and includes an LLM chat assistant that can analyze your positions and execute trades on your behalf. It's a Bloomberg-terminal-style UI with an AI copilot, built as the capstone project for an agentic AI coding course — the entire codebase is produced by orchestrated coding agents.

## Status

🚧 Under active development. The **market data subsystem** is complete (see [`planning/MARKET_DATA_SUMMARY.md`](planning/MARKET_DATA_SUMMARY.md)); the portfolio, chat/LLM, and frontend layers are still being built. See [`planning/PLAN.md`](planning/PLAN.md) for the full specification.

## Architecture

- **Frontend**: Next.js (TypeScript), static export served by FastAPI — single origin, single port
- **Backend**: FastAPI (Python), managed with `uv`
- **Database**: SQLite, lazily initialized, volume-mounted for persistence
- **Real-time data**: Server-Sent Events (SSE) from an in-process market data feed — a GBM-based simulator by default, or the Massive (Polygon.io) API when a key is provided
- **AI**: LiteLLM → OpenRouter (Cerebras inference), structured outputs for portfolio analysis and trade execution
- **Deployment**: single Docker container, port 8000

Full rationale for these choices is in [`planning/PLAN.md`](planning/PLAN.md#3-architecture-overview).

## Project Layout

```
finally/
├── frontend/     # Next.js TypeScript app (static export)
├── backend/      # FastAPI uv project — API, DB, market data, LLM integration
├── planning/     # Project specification and agent-facing docs
├── scripts/      # Docker start/stop scripts
├── test/         # Playwright E2E tests
└── db/           # SQLite volume mount (runtime data, gitignored)
```

## Getting Started

The backend is a self-contained `uv` project:

```bash
cd backend
uv sync --dev
uv run pytest
```

Copy `.env.example` to `.env` in the project root and set `OPENROUTER_API_KEY` (required for chat) and optionally `MASSIVE_API_KEY` (real market data; omit to use the built-in simulator).

Docker-based single-command startup (`scripts/start_mac.sh` / `scripts/start_windows.ps1`) will bring up the full stack once the frontend and backend are complete — see [`planning/PLAN.md`](planning/PLAN.md#11-docker--deployment).

## Documentation

All project documentation lives in [`planning/`](planning/), with [`PLAN.md`](planning/PLAN.md) as the source of truth for scope, architecture, API contracts, and testing strategy.
