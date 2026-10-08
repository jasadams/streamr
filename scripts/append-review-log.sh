#!/usr/bin/env bash
# Append stdin to the canonical checkout's daily log from any worktree.
# Adapted from Kyomi. Exit 1: usage/environment error; exit 2: blank entry.
set -euo pipefail
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$script_dir/lib/canonical-root.sh"
if [ "$#" -ne 0 ]; then
    echo 'ERROR: no arguments expected; provide the review entry on stdin.' >&2
    exit 1
fi
entry="$(cat)"
if [ -z "${entry//[[:space:]]/}" ]; then
    echo 'ERROR: refusing to append a blank review entry.' >&2
    exit 2
fi
logs_dir="$(resolve_review_logs_dir)" || exit 1
mkdir -p "$logs_dir"
target_file="$logs_dir/$(date +%F).md"
(
    flock 200
    if [ -s "$target_file" ] && [ -n "$(tail -c1 -- "$target_file")" ]; then
        printf '\n' >> "$target_file"
    fi
    printf '%s\n' "$entry" >> "$target_file"
) 200>"$logs_dir/.append.lock"
printf 'Appended review entry to %s\n' "$target_file" >&2
