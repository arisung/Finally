# Review — Changes Since Last Commit (`14550e1`)

## Scope

- `README.md` — modified (rewritten)
- Untracked, not part of this session's edits (pre-existing in the working tree, unrelated to the README change): `.claude-plugin/`, `.claude/agents/`, `.claude/commands/`, `independent-reviewer/` — these are Claude Code tooling/plugin config, not application code, so they're out of scope for a functional review.

## README.md

**Change:** Full rewrite of the root README (31 insertions, 45 deletions net).

**What changed:**
- Added a **Status** section flagging the project as under active development, and correctly scopes the claim to what's actually built — pointing to `planning/MARKET_DATA_SUMMARY.md` for the completed market data subsystem and `planning/PLAN.md` for the full spec.
- Removed the old **Features** and **Quick Start** sections, which described the finished product (Docker one-liner, `docker run` with a live app on `:8000`) as if it worked today.
- Replaced them with a **Getting Started** section scoped to what's real right now: `cd backend && uv sync --dev && uv run pytest`.
- Kept **Architecture** and **Project Layout**, lightly reworded.
- Dropped the standalone **License** section (the `LICENSE` file itself is untouched and still present at repo root).

**Assessment: accurate and correctly scoped.**

Verified against actual repo state:
- `frontend/` does not exist yet — confirmed (`ls frontend` → no such directory). The old README's Docker quick-start would have failed outright; the new version correctly avoids claiming a working `docker run` command.
- `backend/` is a real `uv` project with `pyproject.toml` and a `tests/` directory — the `cd backend && uv sync --dev && uv run pytest` instructions are valid as given.
- `scripts/start_mac.sh` / `start_windows.ps1` referenced in the new README do not exist yet (no `scripts/` directory in the repo). The README hedges this correctly ("will bring up the full stack once the frontend and backend are complete") rather than presenting it as available today — acceptable, but worth flagging since a reader skimming only the code block might miss that hedge.
- `planning/MARKET_DATA_SUMMARY.md` and `planning/PLAN.md` both exist and are linked correctly, including the `#3-architecture-overview` and `#11-docker--deployment` anchors, which match the actual heading text in `PLAN.md`.
- **`.env.example` does not exist in the repo** (confirmed: `test -f .env.example` → missing). The Getting Started instruction "Copy `.env.example` to `.env` ... " will fail for anyone following it as written. This is a real, actionable inaccuracy — either `.env.example` needs to be added to the repo, or the README should instruct creating `.env` directly with the required variables (`OPENROUTER_API_KEY`, optionally `MASSIVE_API_KEY`) until an example file exists.

**Minor observations (non-blocking):**
- The dropped License section is a small regression in discoverability — the `LICENSE` file exists but is no longer linked from the README. Not incorrect, just less convenient than before.

## Other Untracked Files

Not reviewed in depth — these predate this session's changes and are Claude Code plugin/agent configuration (marketplace manifest, a `change-reviewer` agent definition, a `doc-review` command, and an `independent-reviewer` plugin with a hooks config), not part of the FinAlly application itself. Flagging their presence only for completeness; no correctness review performed since they're outside the scope of what changed in this session.
