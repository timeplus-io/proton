#!/usr/bin/env bash

set -euo pipefail

: "${GH_TOKEN:?GH_TOKEN is required}"
: "${PR_NUMBER:?PR_NUMBER is required}"
: "${GITHUB_REPOSITORY:?GITHUB_REPOSITORY is required}"

repo="$GITHUB_REPOSITORY"
mirror_branch="trusted-pr/${PR_NUMBER}"
comment_marker="<!-- trusted-fork-pr-ci -->"

pr_json="$(gh api "repos/${repo}/pulls/${PR_NUMBER}")"

pr_state="$(jq -r '.state' <<<"$pr_json")"
is_cross_repo="$(jq -r '.head.repo.full_name != .base.repo.full_name' <<<"$pr_json")"
is_draft="$(jq -r '.draft' <<<"$pr_json")"
base_ref="$(jq -r '.base.ref' <<<"$pr_json")"
head_ref="$(jq -r '.head.ref' <<<"$pr_json")"
head_sha="$(jq -r '.head.sha' <<<"$pr_json")"
head_repo="$(jq -r '.head.repo.full_name' <<<"$pr_json")"
pr_title="$(jq -r '.title' <<<"$pr_json")"
pr_url="$(jq -r '.html_url' <<<"$pr_json")"
pr_author="$(jq -r '.user.login' <<<"$pr_json")"
labels_csv="$(jq -r '[.labels[].name] | join(",")' <<<"$pr_json")"
viewer_login="$(gh api user --jq '.login')"
workflow_url="https://github.com/${repo}/actions/workflows/proton_ci.yml"

case "$base_ref" in
  develop|2.9|3.0)
    ;;
  *)
    echo "::error::PR #${PR_NUMBER} targets ${base_ref}, which is not handled by ProtonCI."
    exit 1
    ;;
esac

if [[ "$pr_state" != "open" ]]; then
  echo "::error::PR #${PR_NUMBER} is not open."
  exit 1
fi

if [[ "$is_cross_repo" != "true" ]]; then
  echo "::error::PR #${PR_NUMBER} already comes from ${repo}; use the normal pull_request workflow."
  exit 1
fi

if [[ "$is_draft" == "true" ]]; then
  echo "::error::PR #${PR_NUMBER} is still a draft. Mark it ready for review before running trusted CI."
  exit 1
fi

git config user.name "proton-robot"
git config user.email "proton_robot@timeplus.io"

git fetch origin "pull/${PR_NUMBER}/head"
git checkout --force -B "$mirror_branch" FETCH_HEAD
git push --force origin "HEAD:refs/heads/${mirror_branch}"

dispatch_file="$(mktemp)"
comment_file="$(mktemp)"
trap 'rm -f "$dispatch_file" "$comment_file"' EXIT

jq -n \
  --arg ref "$base_ref" \
  --arg mirror_ref "$mirror_branch" \
  --arg source_pr_number "$PR_NUMBER" \
  --arg pr_labels_csv "$labels_csv" \
  '{ref: $ref, inputs: {ref: $mirror_ref, source_pr_number: $source_pr_number, pr_labels_csv: $pr_labels_csv}}' > "$dispatch_file"

gh api -X POST "repos/${repo}/actions/workflows/proton_ci.yml/dispatches" --input "$dispatch_file" >/dev/null

{
  echo "${comment_marker}"
  echo
  echo "Trusted CI mirror updated for ${pr_title}"
  echo
  echo "- Original PR: ${pr_url}"
  echo "- Original author: @${pr_author}"
  echo "- Original head: \`${head_repo}:${head_ref}\`"
  echo "- Mirrored branch: \`${mirror_branch}\`"
  echo "- Mirrored commit: \`${head_sha}\`"
  echo "- ProtonCI workflow: ${workflow_url}"
  echo
  echo "ProtonCI was dispatched from \`${base_ref}\` and will checkout the trusted mirror branch directly."
  echo "This comment will be updated with the final CI result and the direct run link after ProtonCI finishes."
  echo "Re-run the \`trusted_fork_pr_ci\` workflow after new pushes to the fork PR."
} > "$comment_file"

existing_comment_json="$(gh api "repos/${repo}/issues/${PR_NUMBER}/comments" --paginate | jq -c --arg marker "$comment_marker" --arg viewer "$viewer_login" 'map(select(.user.login == $viewer and (.body | contains($marker)))) | last')"
comment_body="$(<"$comment_file")"

if [[ -n "$existing_comment_json" && "$existing_comment_json" != "null" ]]; then
  comment_id="$(jq -r '.id' <<<"$existing_comment_json")"
  gh api -X PATCH "repos/${repo}/issues/comments/${comment_id}" -f body="$comment_body" >/dev/null
else
  gh api -X POST "repos/${repo}/issues/${PR_NUMBER}/comments" -f body="$comment_body" >/dev/null
fi

if [[ -n "${GITHUB_OUTPUT:-}" ]]; then
  {
    echo "mirror_branch=${mirror_branch}"
    echo "workflow_url=${workflow_url}"
    echo "mirrored_sha=${head_sha}"
  } >> "$GITHUB_OUTPUT"
fi
