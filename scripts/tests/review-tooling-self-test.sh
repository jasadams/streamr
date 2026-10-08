#!/usr/bin/env bash
# Real disposable repositories and a fresh Ed25519 key; no production key.
set -euo pipefail
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
sign="$script_dir/sign-review.sh"
append="$script_dir/append-review-log.sh"
scratch="$(mktemp -d)"
trap 'rm -rf "$scratch"' EXIT
export GIT_CONFIG_NOSYSTEM=1 GIT_CONFIG_GLOBAL=/dev/null
export GIT_AUTHOR_NAME='Review test' GIT_AUTHOR_EMAIL='review@example.invalid'
export GIT_COMMITTER_NAME="$GIT_AUTHOR_NAME" GIT_COMMITTER_EMAIL="$GIT_AUTHOR_EMAIL"
export GIT_EDITOR=true
unset GIT_DIR GIT_WORK_TREE GIT_INDEX_FILE
repo="$scratch/main checkout"
git init -q -b main "$repo"
cd "$repo"
printf '/.review-approval\n/docs/review-logs/\n' > .gitignore
printf 'initial\n' > tracked
printf '\000old binary\001' > binary
git add .
git -c core.hooksPath=/dev/null commit -qm initial
openssl genpkey -algorithm ed25519 -out "$scratch/key.pem" 2>/dev/null
openssl pkey -in "$scratch/key.pem" -pubout -out "$scratch/public.pem" 2>/dev/null
key="$(cat "$scratch/key.pem")"
passed=0
pass() { printf 'PASS: %s\n' "$1"; passed=$((passed + 1)); }
reject() {
    local expected="$1" actual=0
    shift
    "$@" > "$scratch/output" 2>&1 || actual=$?
    if [ "$actual" -ne "$expected" ]; then
        printf 'FAIL: expected exit %s; got %s\n' "$expected" "$actual" >&2
        cat "$scratch/output" >&2
        exit 1
    fi
}
# An independent public-key verifier: freshness and cryptographic authenticity
# are separate checks, both required by any future approval consumer.
verify() {
    local current stored signature reason
    current="$(git diff --cached --binary | sha256sum | awk '{print $1}')"
    stored="$(sed -n '1p' .review-approval)"
    [ "$current" = "$stored" ] || return 1
    signature="$(sed -n '2p' .review-approval)"
    reason="$(sed -n '3p' .review-approval)"
    printf '%s' "$stored" > "$scratch/payload"
    if [ -n "$reason" ]; then printf '\n%s' "$reason" >> "$scratch/payload"; fi
    printf '%s' "$signature" | base64 -d > "$scratch/signature"
    openssl pkeyutl -verify -rawin -pubin -inkey "$scratch/public.pem" \
        -in "$scratch/payload" -sigfile "$scratch/signature" >/dev/null 2>&1
}

reject 1 "$sign"
reject 1 "$sign" "$key"
test ! -e .review-approval
pass 'missing key and empty index are rejected'
printf 'reviewed\n' >> tracked
git add tracked
mkdir nested
(cd nested && "$sign" "$key")
verify
test "$(wc -l < .review-approval)" -eq 2
pass 'one key argument signs exact staged hash, including invocation from subdirectory'
printf 'more\n' >> tracked
reject 1 "$sign" "$key"
git add tracked
reject 1 verify
pass 'unstaged tracked edits are rejected; index changes invalidate previous approval'
printf 'new\n' > $'new file\nwith newline'
reject 1 "$sign" "$key"
git add .
git restore --staged -- $'new file\nwith newline'
reject 1 "$sign" "$key"
pass 'untracked files, including a newly unstaged file, are rejected'
reject 1 "$sign" "$key" --allow-unstaged ''
reject 1 "$sign" "$key" --allow-unstaged $'bad\nreason'
reject 1 "$sign" "$key" --allow-unstaged $'bad\rreason'
reject 1 "$sign" "$key" --unknown
"$sign" "$key" --allow-unstaged 'unrelated local work'
verify
sed -i '3s/local/unreviewed/' .review-approval
reject 1 verify
pass 'override reason is required, single-line, and cryptographically bound'
rm -- $'new file\nwith newline'
mkdir -p docs/review-logs
printf 'local log\n' > docs/review-logs/local.md
"$sign" "$key"
verify
printf 'ALLOW-UNSTAGED:forged\n' >> .review-approval
reject 1 verify
pass 'ignored logs do not block signing; appended forged override fails verification'
cp .review-approval "$scratch/previous-approval"
reject 1 "$sign" 'invalid key'
cmp .review-approval "$scratch/previous-approval"
pass 'invalid key fails without replacing prior approval'
printf '\000first binary\001' > binary
git add binary
"$sign" "$key"
verify
printf '\000second binary\001' > binary
git add binary
reject 1 verify
pass 'changed binary contents invalidate approval'

# Exercise fail-closed Git handling without relying on permissions/root UID.
real_git="$(command -v git)"
mkdir "$scratch/bin"
printf '#!/usr/bin/env bash\nif [ "$1" = diff ]; then exit 73; fi\nexec %q "$@"\n' "$real_git" > "$scratch/bin/git"
chmod +x "$scratch/bin/git"
reject 73 env PATH="$scratch/bin:$PATH" "$sign" "$key"
pass 'Git diff errors cannot silently produce an approval'

git -c core.hooksPath=/dev/null commit -qm reviewed
worktree="$scratch/linked checkout"
git worktree add -qb review-test "$worktree" main
today="$(date +%F)"
target="$repo/docs/review-logs/$today.md"
mkdir -p "$worktree/subdir"
(cd "$worktree/subdir"; printf 'first entry\n' | "$append")
test ! -e "$worktree/docs/review-logs"
printf 'first entry\n' > "$scratch/expected"
cmp "$target" "$scratch/expected"
printf 'without newline' >> "$target"
(cd "$worktree"; printf 'second entry\n' | "$append")
printf 'without newline\nsecond entry\n' >> "$scratch/expected"
cmp "$target" "$scratch/expected"
pass 'worktree and subdirectory appends resolve canonical log and preserve existing entries'
reject 2 "$append" < /dev/null
reject 2 "$append" <<< '   '
reject 1 "$append" unexpected <<< entry
cmp "$target" "$scratch/expected"
(cd "$scratch"; reject 1 "$append" <<< entry)
pass 'blank entries, arguments, and non-repository invocation are rejected'
printf '#!/usr/bin/env bash\nif [[ "$*" == *--path-format=absolute* ]]; then exit 129; fi\nexec %q "$@"\n' "$real_git" > "$scratch/bin/git"
reject 1 env PATH="$scratch/bin:$PATH" "$append" <<< entry
cmp "$target" "$scratch/expected"
pass 'unsupported absolute Git common-dir resolution fails without guessing a log path'
git worktree remove "$worktree"
cmp "$target" "$scratch/expected"
pass 'canonical log survives linked worktree removal'
printf '%s review tooling checks passed.\n' "$passed"
