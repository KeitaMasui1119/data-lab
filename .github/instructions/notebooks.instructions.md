---
description: "Use when editing Jupyter notebooks, notebook-like scripts, exploratory analysis, visualization code, or ad hoc data investigation files in this workspace."
applyTo:
  - "src/Jupyter/**/*.ipynb"
  - "src/others/notebook_like_script.py"
name: "Notebook And Exploration Guidelines"
---

# Notebook Guidelines

- Keep notebooks idempotent so cells can be rerun from top to bottom without hidden state assumptions.
- Use notebooks for exploration, validation, and visualization; move reusable business logic into `src/` or `utility/`.
- Avoid hardcoding secrets or machine-specific paths. Prefer workspace-relative paths and configured credentials.
- When a notebook produces a result that should be operationalized, extract the logic into a Python module and leave the notebook as a thin consumer.
