#!/usr/bin/env bash
# Hermetic tests: command stubs make failed/partial listings reproducible.
set -euo pipefail
script=$(cd "$(dirname "$0")/.." && pwd)/check-ticket-in-flight.sh
sandbox=$(mktemp -d)
trap 'rm -rf "$sandbox"' EXIT
mkdir -p "$sandbox/bin" "$sandbox/tree"
export EXPECTED_REPO=$(cd "$(dirname "$script")/.." && pwd)
export FIXTURES=$sandbox
export PATH="$sandbox/bin:$PATH"
cat > "$sandbox/bin/git" <<'STUB'
#!/usr/bin/env bash
set -eu
[[ $PWD == "$EXPECTED_REPO" ]] || { echo "wrong working directory: $PWD" >&2; exit 98; }
case "$*" in
    'rev-parse --abbrev-ref HEAD') command=current ;;
    'ls-remote --heads '* ) command=remote ;;
    'worktree list --porcelain') command=worktrees ;;
    "branch --list --format=%(refname:short)") command=branches ;;
    *) echo "unexpected git arguments: $*" >&2; exit 98 ;;
esac
cat "$FIXTURES/$command"
if [[ -f $FIXTURES/$command-fail ]]; then echo "$command failed" >&2; exit 9; fi
STUB
cat > "$sandbox/bin/gh" <<'STUB'
#!/usr/bin/env bash
set -eu
[[ $1 == api && $2 == --paginate && $3 == 'repos/jasadams/streamr/pulls?state=all&per_page=100' && $4 == --jq ]] || exit 98
# Assert the actual query only reads head refs, never titles/bodies or a limit.
[[ $5 == *'.head.ref'* && $5 != *'.title'* && $5 != *'.body'* ]] || exit 98
cat "$FIXTURES/prs"
if [[ -f $FIXTURES/prs-fail ]]; then echo 'pagination failed after page 1' >&2; exit 9; fi
STUB
chmod +x "$sandbox/bin/git" "$sandbox/bin/gh"
passed=0
reset() {
    rm -f "$sandbox/"*-fail "$sandbox/tree/STRANDED.md"
    printf 'main\n' > "$sandbox/current"
    : > "$sandbox/remote"
    : > "$sandbox/worktrees"
    : > "$sandbox/branches"
    : > "$sandbox/prs"
}
check() {
    local expected=$1 label=$2 status=0
    shift 2
    output=$("$script" "$@" 2>&1) || status=$?
    if [[ $status != "$expected" ]]; then
        printf 'FAIL %s: expected %s, got %s\n%s\n' "$label" "$expected" "$status" "$output" >&2
        exit 1
    fi
    passed=$((passed + 1))
}
contains() {
    [[ $output == *"$1"* ]] || { printf 'Missing output %s\n%s\n' "$1" "$output" >&2; exit 1; }
}
reset
check 0 'empty listings' STR-17
contains 'CLEAR: STR-17'
original_directory=$PWD
cd /tmp
check 0 'invoke outside repository' STR-17
cd "$original_directory"
check 2 'missing ticket'
check 2 'wrong team' KYO-17
check 2 'bare number' 17
check 2 'extra remote' STR-17 origin upstream
check 2 'missing flag value' STR-17 --self
check 2 'self must match requested ticket' STR-17 --self jason/str-170-other
check 2 'unknown flag' STR-17 --wat
check 0 'remote override' STR-17 upstream

# A numeric boundary catches exact/bare and legacy keys, never STR-170.
for branch in jason/str-17-fix STR-17 jason/str17-fix feature/STR17; do
    reset
    printf '%s\n' "$branch" > "$sandbox/branches"
    check 1 "local branch $branch" STR-17
    contains "$branch"
done
for branch in jason/str-170-fix jason/str170-fix jason/str-117-fix jason/notstr-17-fix; do
    reset
    printf '%s\n' "$branch" > "$sandbox/branches"
    check 0 "boundary $branch" STR-17
done
reset
printf 'aabb\trefs/heads/jason/str-17-remote\n' > "$sandbox/remote"
check 1 'remote heads' STR-17
contains 'remote branch origin'
for state in OPEN CLOSED MERGED; do
    reset
    printf '123\t%s\tjason/str-17-pr\n' "$state" > "$sandbox/prs"
    check 1 "PR state $state" STR-17
    contains "PR #123 ($state)"
done
reset
# Many rows simulate a paginated corpus; a late hit must remain visible.
for ((row=1; row<=401; row++)); do printf '%s\tMERGED\tjason/str-170-other\n' "$row"; done > "$sandbox/prs"
printf '402\tCLOSED\tjason/str17-late\n' >> "$sandbox/prs"
check 1 'PR beyond first 400 rows' STR-17
contains 'PR #402'
reset
printf 'worktree %s\nHEAD abc\nbranch refs/heads/jason/str-17-local\n\n' "$sandbox/tree" > "$sandbox/worktrees"
check 1 'local worktree' STR-17
contains "local worktree $sandbox/tree"
reset
printf 'jason/str-17-self\n' > "$sandbox/current"
printf 'jason/str-17-self\n' > "$sandbox/branches"
printf 'aabb\trefs/heads/jason/str-17-self\n' > "$sandbox/remote"
printf '12\tOPEN\tjason/str-17-self\n' > "$sandbox/prs"
check 0 'current matching ticket excludes own evidence' STR-17
printf 'main\n' > "$sandbox/current"
check 1 'cwd main does not exclude ticket' STR-17
check 0 'explicit self from another cwd' STR-17 --self jason/str-17-self
printf 'jason/str-17-competitor\n' > "$sandbox/current"
printf 'jason/str-17-competitor\n' >> "$sandbox/branches"
check 1 'explicit self does not also hide current competitor' STR-17 --self jason/str-17-self
printf 'main\n' > "$sandbox/current"
printf 'jason/str-17-self\n' > "$sandbox/branches"
check 0 'exact operator ignore' STR-17 --ignore-branch jason/str-17-self
check 1 'ignore does not match by prefix' STR-17 --ignore-branch jason/str-17
reset
printf 'aabb\trefs/heads/stranded/jason/str-17-old\n' > "$sandbox/remote"
printf 'stranded/jason/str-17-old\n' > "$sandbox/branches"
check 0 'stranded refs preserve work' STR-17
contains 'PRESERVED STRANDED WORK'
contains 'stranded/jason/str-17-old'
printf '9\tCLOSED\tstranded/jason/str-17-old\n' > "$sandbox/prs"
check 1 'stranded namespace never hides PR' STR-17
contains 'PR #9'
reset
printf 'worktree %s\nHEAD abc\nbranch refs/heads/jason/str-17-local\n\n' "$sandbox/tree" > "$sandbox/worktrees"
printf 'jason/str-17-local\n' > "$sandbox/branches"
printf 'Preserved STR-170\n' > "$sandbox/tree/STRANDED.md"
check 1 'marker must name exact ticket' STR-17
printf 'Preserved STR-17xx\n' > "$sandbox/tree/STRANDED.md"
check 1 'marker rejects alphanumeric suffix' STR-17
printf 'Preserved STR-17\n' > "$sandbox/tree/STRANDED.md"
check 0 'matching local marker preserves local work' STR-17
contains 'PRESERVED STRANDED WORK'
printf 'aabb\trefs/heads/jason/str-17-local\n' > "$sandbox/remote"
check 1 'local marker cannot hide remote' STR-17
printf '8\tMERGED\tjason/str-17-local\n' > "$sandbox/prs"
check 1 'local marker cannot hide PR' STR-17
contains 'PR #8'

for command in current remote worktrees branches prs; do
    reset
    touch "$sandbox/$command-fail"
    check 3 "failed $command listing" STR-17
    contains 'INCOMPLETE'
done
reset
printf '1\tOPEN\tjason/str-17-partial\n' > "$sandbox/prs"
touch "$sandbox/prs-fail"
check 3 'partial pagination is incomplete' STR-17
contains 'pagination failed'
[[ $output != *'PR #1'* ]] || { echo 'Partial PR output was consumed' >&2; exit 1; }
reset
printf 'unreadable\n' > "$sandbox/prs"
check 3 'malformed PR output' STR-17
reset
printf '1\tOPEN\t\n' > "$sandbox/prs"
check 3 'empty PR head fails closed' STR-17
reset
printf 'aabb\twrong/ref\n' > "$sandbox/remote"
check 3 'malformed remote output' STR-17
reset
printf 'unexpected worktree record\n' > "$sandbox/worktrees"
check 3 'malformed worktree output' STR-17
reset
printf 'jason/str-17-hit\n' > "$sandbox/branches"
touch "$sandbox/remote-fail"
check 3 'failure takes precedence over hit' STR-17
contains 'IN FLIGHT'
contains 'INCOMPLETE'
printf 'PASS: %s ticket guard checks\n' "$passed"
