# Branch Strategy

## Purpose

Define branching conventions and commit message standards for this repository.

## Scope

- Applies to all branches and commits in this repository.
- Commit messages and Pull Request titles/descriptions are written in English.

---

## Branch Naming Convention

```
{type}/{short-description}
```

### Branch Types

| Type       | Description                                      | Example                          |
|-----------|--------------------------------------------------|----------------------------------|
| `feature/` | New feature development                          | `feature/jepx-scraper`           |
| `refactor/`| Code changes without altering behavior           | `refactor/duckdb-loader`         |
| `fix/`     | Bug fixes                                        | `fix/csv-encoding-error`         |
| `docs/`    | Documentation additions or updates              | `docs/add-layers-md`             |
| `chore/`   | Dependency updates, config changes, build tasks  | `chore/update-duckdb-1-4-1`      |

### Rules

- Use lowercase letters and hyphens only.
- Keep descriptions short and specific.
- Branch from `main` always.

```bash
# Examples
git checkout -b feature/jepx-scraper
git checkout -b docs/add-infrastructure-md
git checkout -b fix/duckdb-build-error
git checkout -b chore/update-uv-dependencies
```

---

## Commit Message Convention

Follows [Conventional Commits](https://www.conventionalcommits.org/).

### Format

```
{type}: {short description}

{optional body}
```

### Commit Types

| Type       | Description                                      |
|-----------|--------------------------------------------------|
| `feat`     | New feature                                      |
| `refactor` | Code changes without altering behavior           |
| `fix`      | Bug fix                                          |
| `docs`     | Documentation only                               |
| `chore`    | Dependency updates, config changes, build tasks  |
| `test`     | Adding or updating tests                         |
| `style`    | Formatting changes (no logic change)             |

### Rules

- Written in English.
- Use imperative mood in the subject line ("add", not "added").
- Keep the subject line under 72 characters.
- Add a body if the change needs context.

```bash
# Examples
git commit -m "feat: add JEPX scraper class"
git commit -m "fix: handle 404 response in scraper"
git commit -m "docs: add layers.md"
git commit -m "chore: update duckdb to 1.4.1"
git commit -m "refactor: extract catalog config to separate module"

# With body
git commit -m "feat: add upsert to silver layer via DuckDB MERGE INTO

Use DuckDB MERGE INTO instead of PyIceberg native upsert
because PyIceberg does not support MERGE INTO natively as of v0.10.0."
```

---

## Pull Request Convention

### Title

Follows the same format as commit messages.

```
{type}: {short description}
```

### Description Template

```markdown
## Summary
Brief description of what this PR does.

## Changes
- Change 1
- Change 2

## Related Layer
<!-- Which layer does this PR affect? -->
- [ ] Raw
- [ ] Bronze
- [ ] Silver
- [ ] Infrastructure
- [ ] Documentation

## Notes
Any additional context or open questions.
```

### Rules

- Written in English.
- Use squash merge only.
- Delete branch after merge.

---

## Workflow

```
1. Create branch from main
   git checkout -b feature/jepx-scraper

2. Implement and commit
   git add .
   git commit -m "feat: add JEPX scraper class"

3. Push and create PR
   git push origin feature/jepx-scraper
   gh pr create --fill

4. Squash merge and delete branch
   gh pr merge --squash --delete-branch
```
