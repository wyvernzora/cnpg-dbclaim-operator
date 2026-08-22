#!/usr/bin/env bash
set -euo pipefail

msg_file="${1:?missing commit message file}"
pattern="$(printf '%s%s%s' '[cC]' 'laud' '[eE]')"

if grep -Eiq "(^|[^[:alnum:]])${pattern}([^[:alnum:]]|$)" "$msg_file"; then
  cat >&2 <<'MSG'
Commit rejected: commit message must not mention the blocked assistant name.
MSG
  exit 1
fi
