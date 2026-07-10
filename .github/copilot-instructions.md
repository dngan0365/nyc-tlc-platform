# Repository Instructions

This repository is a Python 3.11 data platform. Prefer `ruff` for linting, `black` for formatting, and pytest for validation.

## Agent Coding Convention

When an agent changes code in this repository, follow these rules:

1. Start from the smallest concrete anchor: the failing file, workflow, test, or config entry that controls the behavior.
2. Keep edits minimal and local. Do not widen scope or refactor unrelated code while fixing a specific issue.
3. Preserve existing style, file layout, and naming unless the task explicitly requires a broader cleanup.
4. Validate the touched slice immediately after the first substantive edit with the cheapest useful check available.
5. Prefer repo-native commands such as `make lint`, `make test`, and `make integration-test` over inventing new flows.
6. Use `apply_patch` for file edits and avoid destructive git operations unless the user explicitly requests them.
7. Never overwrite user changes in unrelated files; if a conflict appears, stop and ask before touching it.
8. For GitHub Actions or agent automation, include artifacts, required checks, and ownership rules for critical paths.

High-risk areas:

- `infra/` and `infra/terraform/` for cloud and IAM changes
- `.github/workflows/` for CI, security, and release automation
- `ingestion/`, `spark_jobs/`, and `dbt_project/` for data-flow changes
- `serving/` for runtime and app-facing behavior

Environment setup:

1. Copy `.env.example` to `.env`.
2. Fill in AWS credentials and any local service defaults you need.
3. After Terraform provision completes, run `terraform output -json > infra/terraform/generated/outputs.json` and `python infra/terraform/scripts/generate_env.py` to refresh `.env.generated`.
4. Use `make setup`, `make lint`, `make test`, and `make integration-test` for local validation.

For agent work, keep branch protection, CODEOWNERS, and CI artifacts aligned with the checks documented in the README.
