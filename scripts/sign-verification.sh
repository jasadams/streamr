#!/usr/bin/env bash
# Sign a verified PR's current head SHA with an Ed25519 PEM key.
# Usage: scripts/sign-verification.sh <pr_number> <private_key_pem_string>
# Based on the identical Kyomi and Trakkt helpers. Preserve the wrapper's
# three-line format: PR number, head SHA, base64 signature of the SHA alone
# (no newline in the signed payload). New commits require fresh verification.
# Never enable shell tracing around the private-key argument.
set -euo pipefail

if [ "$#" -ne 2 ] || [[ ! "$1" =~ ^[1-9][0-9]*$ ]] || [ -z "$2" ]; then
    echo 'ERROR: Usage: sign-verification.sh <pr_number> <private_key_pem_string>' >&2
    exit 1
fi
pr_number="$1"
private_key="$2"

# Linked worktrees share this directory with the main checkout. Absolute
# output also makes calls from a checkout's subdirectories unambiguous.
common_dir="$(git rev-parse --path-format=absolute --git-common-dir)"
if [ -z "$common_dir" ] || [[ "$common_dir" != /* ]] || [ ! -d "$common_dir" ]; then
    echo 'ERROR: cannot resolve the shared Git directory.' >&2
    exit 1
fi
if ! head_sha="$(gh pr view "$pr_number" --json headRefOid -q .headRefOid)"; then
    echo "ERROR: could not resolve PR #$pr_number head SHA." >&2
    exit 1
fi
if [[ ! "$head_sha" =~ ^[0-9a-f]{40}$ ]]; then
    echo "ERROR: PR #$pr_number returned an invalid GitHub head SHA." >&2
    exit 1
fi

umask 077
approvals_dir="$common_dir/verification-approvals"
mkdir -p "$approvals_dir"
# Keep temporary output on the destination filesystem for atomic replacement.
scratch="$(mktemp -d "$approvals_dir/.sign-verification.XXXXXXXX")"
trap 'rm -rf "$scratch"' EXIT
printf '%s\n' "$private_key" > "$scratch/key.pem"
printf '%s' "$head_sha" > "$scratch/sha"
# -rawin supports Ed25519 on OpenSSL 3.0 as well as newer releases.
signature="$(openssl pkeyutl -sign -rawin -inkey "$scratch/key.pem" -in "$scratch/sha" | base64 -w 0)"
[ -n "$signature" ] || { echo 'ERROR: signing failed.' >&2; exit 1; }
printf '%s\n%s\n%s\n' "$pr_number" "$head_sha" "$signature" > "$scratch/approval"
approval_file="$approvals_dir/pr-$pr_number"
mv -T -- "$scratch/approval" "$approval_file"
printf 'Verification approval signed for PR #%s at SHA %s\n' "$pr_number" "$head_sha"
printf 'Approval file: %s\n' "$approval_file"
