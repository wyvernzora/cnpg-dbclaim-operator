#!/usr/bin/env bash
set -euo pipefail

# Conventional Commits v1.0.0 subject lint. Scope is optional and free-form
# (single-module repo — no closed scope enum). Interface: a commit-msg file
# path, or "-" for a single subject line on stdin (used by CI for PR titles /
# pushed commits).

msg_file="${1:?missing commit message file}"
if [[ "$msg_file" == "-" ]]; then
  IFS= read -r subject || true
else
  subject="$(head -n 1 "$msg_file")"
fi

# Exemptions: merge commits and rebase autosquash markers.
case "$subject" in
  "Merge "*|"fixup! "*|"squash! "*) exit 0 ;;
esac

types='feat|fix|docs|refactor|test|build|ci|chore|perf|revert'

if [[ ! "$subject" =~ ^($types)(\([a-z0-9-]+\))?\!?:\ .+ ]]; then
  cat >&2 <<MSG
Commit rejected: subject must be Conventional Commits:
  <type>[(<scope>)][!]: <description>
  types: ${types//|/, }
MSG
  exit 1
fi

if (( ${#subject} > 72 )); then
  echo "Commit rejected: subject exceeds 72 characters." >&2
  exit 1
fi
