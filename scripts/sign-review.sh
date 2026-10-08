#!/usr/bin/env bash
# Sign the current repository's staged diff after review. The caller supplies
# an Ed25519 PEM key as one argument; never enable shell tracing around it.
# Usage: scripts/sign-review.sh <private_key_pem_string> [--allow-unstaged <reason>]
#
# Based on Kyomi's fail-closed signer and Trakkt's two-line approval format.
# .review-approval contains hash, base64 signature, and optionally
# ALLOW-UNSTAGED:<reason>. The reason is included in the signed payload.
# Hash input is `git diff --cached --binary` (including binary contents).
# Verifiers must use the same diff command and reject a stale hash. This
# helper does not install a commit hook or establish a trusted public key.
set -euo pipefail

if [ "$#" -eq 0 ] || [ -z "$1" ]; then
    echo 'ERROR: private key argument required.' >&2
    exit 1
fi
private_key="$1"
shift
allow_unstaged=0
reason=''
if [ "$#" -ne 0 ]; then
    if [ "$#" -ne 2 ] || [ "$1" != '--allow-unstaged' ]; then
        echo 'Usage: scripts/sign-review.sh <private_key_pem_string> [--allow-unstaged <reason>]' >&2
        exit 1
    fi
    reason="$2"
    if [ -z "${reason//[[:space:]]/}" ] || [[ "$reason" == *$'\n'* ]] || [[ "$reason" == *$'\r'* ]]; then
        echo 'ERROR: --allow-unstaged requires a nonempty, single-line reason.' >&2
        exit 1
    fi
    allow_unstaged=1
fi

# Always write approval at this worktree's root, including calls from subdirs.
root="$(git rev-parse --show-toplevel)"
cd "$root"
umask 077
scratch="$(mktemp -d)"
trap 'rm -rf "$scratch"' EXIT

# Capture statuses directly: process substitutions can hide failed Git calls.
git diff --name-only -z > "$scratch/unstaged"
git ls-files --others --exclude-standard -z > "$scratch/untracked"
if [ -s "$scratch/unstaged" ] || [ -s "$scratch/untracked" ]; then
    if [ "$allow_unstaged" -eq 0 ]; then
        echo 'ERROR: refusing to sign: unstaged or untracked changes are outside the staged diff.' >&2
        echo 'Stage the reviewed files, or use --allow-unstaged with a reviewed reason.' >&2
        exit 1
    fi
    printf 'Signing with unstaged/untracked changes. Reason: %s\n' "$reason"
fi

git diff --cached --binary > "$scratch/diff"
if [ ! -s "$scratch/diff" ]; then
    echo 'ERROR: no staged changes to sign.' >&2
    exit 1
fi
diff_hash="$(sha256sum "$scratch/diff" | awk '{print $1}')"
printf '%s' "$diff_hash" > "$scratch/payload"
if [ "$allow_unstaged" -eq 1 ]; then
    printf '\nALLOW-UNSTAGED:%s' "$reason" >> "$scratch/payload"
fi
printf '%s\n' "$private_key" > "$scratch/key.pem"
# -rawin supports Ed25519 on OpenSSL 3.0 as well as newer versions.
signature="$(openssl pkeyutl -sign -rawin -inkey "$scratch/key.pem" -in "$scratch/payload" | base64 -w 0)"
[ -n "$signature" ] || { echo 'ERROR: signing failed.' >&2; exit 1; }
{
    printf '%s\n%s\n' "$diff_hash" "$signature"
    if [ "$allow_unstaged" -eq 1 ]; then
        printf 'ALLOW-UNSTAGED:%s\n' "$reason"
    fi
} > "$scratch/approval"
mv "$scratch/approval" .review-approval
printf 'Review approval signed for diff hash: %s\n' "$diff_hash"
