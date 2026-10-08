#!/usr/bin/env bash
# Mock GitHub only; exercise real Git worktrees and Ed25519 crypto with a
# fresh disposable key. No GitHub access, production key, or installed hook.
set -euo pipefail
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
sign="$script_dir/sign-verification.sh"
scratch="$(mktemp -d)"
trap 'rm -rf "$scratch"' EXIT
export GIT_CONFIG_NOSYSTEM=1 GIT_CONFIG_GLOBAL=/dev/null
export GIT_AUTHOR_NAME='Verification test' GIT_AUTHOR_EMAIL='verify@example.invalid'
export GIT_COMMITTER_NAME="$GIT_AUTHOR_NAME" GIT_COMMITTER_EMAIL="$GIT_AUTHOR_EMAIL"
unset GIT_DIR GIT_WORK_TREE GIT_INDEX_FILE
mkdir "$scratch/bin"
cat > "$scratch/bin/gh" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
[ "$#" -eq 7 ] && [ "$1" = pr ] && [ "$2" = view ] && [ "$3" = 10 ] &&
    [ "$4" = --json ] && [ "$5" = headRefOid ] && [ "$6" = -q ] && [ "$7" = .headRefOid ] || exit 99
printf '%s\n' "$TEST_HEAD_SHA"
exit "${TEST_GH_STATUS:-0}"
EOF
chmod +x "$scratch/bin/gh"
export PATH="$scratch/bin:$PATH"
export TEST_HEAD_SHA=0123456789abcdef0123456789abcdef01234567
repo="$scratch/main checkout"
git init -q -b main "$repo"
cd "$repo"
git -c core.hooksPath=/dev/null commit --allow-empty -qm initial
openssl genpkey -algorithm ed25519 -out "$scratch/key.pem" 2>/dev/null
openssl pkey -in "$scratch/key.pem" -pubout -out "$scratch/public.pem" 2>/dev/null
key="$(cat "$scratch/key.pem")"
approval="$repo/.git/verification-approvals/pr-10"
passed=0
pass() { printf 'PASS: %s\n' "$1"; passed=$((passed + 1)); }
reject() {
    local actual=0
    "$@" > "$scratch/output" 2>&1 || actual=$?
    if [ "$actual" -eq 0 ]; then
        echo 'FAIL: command unexpectedly succeeded' >&2
        exit 1
    fi
}
# Model the existing wrapper: compare PR and current head before checking
# the SHA-only signature. This deliberately does not modify that wrapper.
verify() {
    [ "$(sed -n '1p' "$approval")" = 10 ] || return 1
    [ "$(sed -n '2p' "$approval")" = "$TEST_HEAD_SHA" ] || return 1
    printf '%s' "$TEST_HEAD_SHA" > "$scratch/payload"
    sed -n '3p' "$approval" | base64 -d > "$scratch/signature"
    openssl pkeyutl -verify -rawin -pubin -inkey "$scratch/public.pem" \
        -in "$scratch/payload" -sigfile "$scratch/signature" >/dev/null 2>&1
}
"$sign" 10 "$key"
test "$(wc -l < "$approval")" -eq 3
test "$(stat -c %a "$approval")" = 600
verify
cp "$approval" "$scratch/original"
pass 'compatible three-line approval verifies over SHA alone with private file permissions'

worktree="$scratch/linked checkout"
git worktree add -qb verification-test "$worktree" main
mkdir "$worktree/nested"
(cd "$worktree/nested"; "$sign" 10 "$key")
cmp "$approval" "$scratch/original"
test ! -e "$worktree/verification-approvals"
pass 'linked worktree subdirectory writes to shared Git common-dir'

reject "$sign"
reject "$sign" 10
reject "$sign" 10 "$key" extra
reject "$sign" 0 "$key"
reject "$sign" 01 "$key"
reject "$sign" -1 "$key"
reject "$sign" '../elsewhere' "$key"
reject "$sign" 10 ''
cmp "$approval" "$scratch/original"
pass 'invalid arguments fail without replacing approval'

for invalid in '' null short 0123456789ABCDEF0123456789ABCDEF01234567 \
    $'0123456789abcdef0123456789abcdef01234567\nextra'; do
    reject env TEST_HEAD_SHA="$invalid" "$sign" 10 "$key"
    cmp "$approval" "$scratch/original"
done
reject env TEST_GH_STATUS=7 "$sign" 10 "$key"
cmp "$approval" "$scratch/original"
pass 'invalid heads and failed GitHub call (even with valid stdout) preserve approval'

reject "$sign" 10 'invalid key'
cmp "$approval" "$scratch/original"
test "$(find "$repo/.git/verification-approvals" -mindepth 1 -maxdepth 1 | wc -l)" -eq 1
pass 'invalid key preserves approval and removes temporary secret/output files'

TEST_HEAD_SHA=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
reject verify
pass 'a changed PR head rejects the previous approval'
"$sign" 10 "$key"
verify
sed -i '3s/./A/g' "$approval"
reject verify
pass 'fresh head signs successfully and signature tampering is rejected'

(cd "$scratch"; reject "$sign" 10 "$key")
test ! -e "$scratch/verification-approvals"
pass 'non-repository invocation fails without writing approval'
git worktree remove "$worktree"
printf '%s verification signing checks passed.\n' "$passed"
