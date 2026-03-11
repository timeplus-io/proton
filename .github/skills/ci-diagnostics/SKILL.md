---
name: ci-diagnostics
description: Diagnose Proton CI failures and performance comparison results from GitHub checks and uploaded reports. Use when the user wants failing tests, report URLs, or performance-regression details from CI.
---

# CI Diagnostics

## Inputs

- PR number or URL
- optional check name substring
- optional direct report URL

## First step: collect check status

For a PR:

```bash
gh pr view "$PR" --json title,body,url
gh pr checks "$PR"
REPO=$(gh repo view --json nameWithOwner --jq .nameWithOwner)
SHA=$(gh pr view "$PR" --json commits --jq '.commits[-1].oid')
gh api "repos/$REPO/commits/$SHA/status"
```

Use the commit status payload to find:

- failing or pending contexts
- `target_url` links for uploaded HTML reports, raw logs, or performance artifacts

## Proton report layout

CI uploads reports under:

```text
<pr-number>/<commit-sha>/<normalized-check-name>...
```

The normalization logic is defined in:

- [tests/ci/upload_result_helper.py](../../../tests/ci/upload_result_helper.py)
- [tests/ci/performance_comparison_check.py](../../../tests/ci/performance_comparison_check.py)

Normalize a check name with lowercase and replacements for spaces, `(`, `)`, and `,`.

## Failure triage workflow

1. List failing contexts from `gh pr checks` or commit statuses.
2. Open each `target_url` report first.
3. If the report is sparse, inspect the linked raw log.
4. For test reports, summarize:
   - failing test names
   - first common error signature
   - whether the failure looks deterministic, flaky, infra, or environment-specific
5. Map failures back to touched areas in the diff.

## Performance comparison workflow

Performance comparison artifacts upload:

- `report.html`
- `all-queries.html`
- `all-query-metrics.tsv`
- `queries.rep`
- images/flamegraphs

If you have a `report.html` URL, inspect sibling artifacts by replacing the filename in the same prefix.

When reviewing perf results:

- start from the summary in `report.html`
- inspect `all-query-metrics.tsv` for the biggest `client_time` regressions
- distinguish broad regressions from a few outlier queries
- correlate with touched execution paths, joins, windows, aggregations, or storage reads

## Output expectations

Always report:

1. failing checks
2. best report/log URL for each failing check
3. likely failure class: code bug, flaky test, infra, dependency, or timeout/resource limit
4. smallest next debugging action

For performance changes also report:

1. whether the regression is broad or narrow
2. the most affected workload family
3. whether more local benchmarking is needed before code changes
