# Update Script Maintenance Report

Date: 2026-03-04

- Re-ran `scripts/london-life-expectancy.py` and verified the existing pipeline still executes against the London Datastore XLS endpoint.
- Added first GitHub Actions automation workflow at `.github/workflows/actions.yml` with:
  - monthly schedule,
  - manual `workflow_dispatch`,
  - `contents: write` permissions,
  - commit-if-changed behavior for `data/` and `datapackage.json`.
- The current script source remains limited to 2016-2018 content from the legacy London Datastore file.
- Freshness beyond 2016-2018 requires migration to newer ONS successor datasets (latest coverage: 2022-2024), which use a different publication path than the legacy URL in the script.
