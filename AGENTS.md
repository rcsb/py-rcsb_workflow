# AGENTS.md

Guidance for AI coding agents working in this repo. See [README.md](README.md) for install and test setup.

## What this repo is

The `rcsb.workflow` Python package: CLI entry points that the weekly update workflows ([rcsb-wuw](https://github.com/rcsb/rcsb-wuw)) run inside pods. Pushes to `master` build and publish the `rcsb/rcsb-workflow` Docker image ([workflow-docker.yaml](.github/workflows/workflow-docker.yaml)), which WUW uses.

- `rcsb/workflow/cli/`: CLI entry points, registered under `[project.scripts]` in [pyproject.toml](pyproject.toml) (`exdb_wf_cli`, `imgs_exec_cli`, `bcif_exec_cli`, `chem_search_index_cli`). Changing their arguments can break WUW DAGs.
- `rcsb/workflow/wuw/`: workflow classes the CLIs call.
- `rcsb/workflow/{bcif,chem,refstats,targets}/`: other workflow logic.
- `rcsb/workflow/tests/`: unittest tests (`test*.py`, fixtures `fixture*.py`).
- `rcsb/mock-data`: git submodule with test data.

## Checks

- Format with `black --line-length 185 rcsb/workflow`.
- `tox` runs flake8, pylint, black, and the tests. Tests need the mock-data submodule and an OpenEye license.

## Git workflow

A push to `master` publishes the production image, so treat `master` as off-limits.

- Start from the latest `master` and do all work on a feature branch.
- Agents may commit and push feature branches with an explicit branch name (`git push -u origin <branch>`).
- After pushing, agents may open a PR against `master` with `gh pr create`. If `gh` isn't available, give the user this link instead:
  `https://github.com/rcsb/py-rcsb_workflow/compare/master...<branch>?expand=1`
- Keep PR titles short. Start the description with a brief summary of a few bullets; put any longer explanation in a separate section below it.
- Never merge PRs (`gh pr merge`) and don't use `gh api`. The user reviews and merges.
- Never push to `master`, never push without naming the branch, never force-push, never use `--no-verify`, and never rewrite published history.
- [.claude/settings.json](.claude/settings.json) backs this up with Claude Code deny rules. They match command text, so they are a backstop, not a guarantee.
