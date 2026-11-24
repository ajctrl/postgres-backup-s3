#!/usr/bin/env bash
set -euo pipefail

repo="repos/$GITHUB_REPOSITORY"
bot=${RENOVATE_BOT_LOGIN:?Set the GitHub App bot login}
# Read every page before mutating branches. Work only on this bot's own
# same-repository PRs. expected_head_sha protects concurrent branch updates.
prs=$(gh api --paginate "$repo/pulls?state=open&base=master&per_page=100")
rows=$(jq -r --arg bot "$bot" --arg repo "$GITHUB_REPOSITORY" '
  .[] | select(.user.login == $bot and .head.repo.full_name == $repo)
  | select(.head.ref | startswith("renovate/")) | [.number,.head.sha] | @tsv' <<< "$prs")
failed=0
while IFS=$'\t' read -r number head_sha; do
  [[ -n "$number" ]] || continue
  if ! behind=$(gh api "$repo/compare/master...$head_sha" --jq .behind_by); then
    echo "::warning::Could not compare PR #$number with master"
    failed=1
    continue
  fi
  if [[ "$behind" == 0 ]]; then continue; fi
  if result=$(gh api --method PUT "$repo/pulls/$number/update-branch" -f expected_head_sha="$head_sha" 2>&1); then
    echo "Updating PR #$number"
    continue
  fi
  if [[ "$result" == *'HTTP 422'* || "$result" == *'HTTP 409'* ]]; then
    if pr=$(gh api "$repo/pulls/$number"); then
      reason=$(jq -r --arg sha "$head_sha" '
        if .state == "closed" then "closed"
        elif (.head.sha != null and .head.sha != $sha) then "head changed"
        elif .mergeable == false then "merge conflict"
        else empty end' <<< "$pr")
      if [[ -n "$reason" ]]; then
        echo "::warning::Skipping PR #$number: $reason"
        continue
      fi
    fi
  fi
  echo "::error::Could not update PR #$number: $result"
  failed=1
done <<< "$rows"
exit "$failed"
