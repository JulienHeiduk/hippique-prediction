---
name: commit-helper
description: Stage and commit changes following Conventional Commits best practices. Analyzes staged/unstaged changes, proposes a well-structured commit message, and creates the commit safely.
argument-hint: "[optional commit message hint]"
allowed-tools: Bash(git *)
---

## Context

Current git status:
```
!`git status --short`
```

Staged diff:
```
!`git diff --cached`
```

Unstaged diff:
```
!`git diff`
```

Recent commits (for style reference):
```
!`git log --oneline -8`
```

## Task

User hint (if any): $ARGUMENTS

Analyze the changes above and create a commit following these rules:

### Commit message format (Conventional Commits)

```
<type>(<scope>): <subject>

[optional body]
```

**Types:**
- `feat` — new feature or capability
- `fix` — bug fix
- `refactor` — code restructure with no behavior change
- `test` — add or update tests
- `docs` — documentation only
- `chore` — tooling, config, dependencies, CI

**Scope:** the module or layer affected — `scraper`, `features`, `model`, `trading`, `dashboard`, `config`, `tests` (omit if change spans multiple)

**Subject rules:**
- Imperative mood ("add", "fix", "remove" — not "added", "fixes")
- Max 72 characters
- No period at the end

**Body** (only when needed): explain *why*, not *what*. Reference tradeoffs or domain decisions if relevant (e.g., Kelly fraction, EV threshold logic).

### Safety checks before committing

1. Never stage `data/`, `logs/`, `.env`, or `*.duckdb` files — check `.gitignore`
2. Prefer staging specific files over `git add -A`
3. If there are unrelated changes mixed together, commit only the logical unit that coheres

### Steps to execute

1. Show the proposed commit message to the user
2. Stage only the relevant files with `git add <specific files>`
3. Create the commit using a heredoc:
   ```bash
   git commit -m "$(cat <<'EOF'
   <type>(<scope>): <subject>

   <body if needed>

   Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>
   EOF
   )"
   ```
4. Run `git status` to confirm success
