#!/usr/bin/env bash
# Streamr ticket pickup/review guard. Only exit 0 authorizes a claim.
# Exit: 0 clear, 1 existing work, 2 invalid arguments, 3 incomplete checks.
# PR head refs (including closed/merged PRs) are evidence; titles and bodies
# are deliberately never searched. Pagination has no arbitrary row ceiling.
set -uo pipefail
repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd) || exit 3
cd "$repo_root" || exit 3

usage() {
    echo 'Usage: scripts/check-ticket-in-flight.sh STR-N [--self BRANCH] [--ignore-branch BRANCH]... [REMOTE]' >&2
    exit 2
}
[[ $# -ge 1 && $1 =~ ^[Ss][Tt][Rr]-([1-9][0-9]*)$ ]] || usage
number=${BASH_REMATCH[1]}
shift
remote=origin
remote_set=false
self_branch=''
declare -a excludes=() hits=() failures=() preserved=() tombstoned=()
while [[ $# -gt 0 ]]; do
    case $1 in
        --self|--ignore-branch)
            [[ $# -ge 2 && -n $2 && $2 != -* ]] || usage
            if [[ $1 == --self ]]; then
                [[ -z $self_branch ]] || usage
                self_branch=$2
            else
                excludes+=("$2")
            fi
            shift 2
            ;;
        -*) usage ;;
        *)
            $remote_set && usage
            remote=$1
            remote_set=true
            shift
            ;;
    esac
done

# Bound both sides of the team key, and especially the numeric suffix.
# Streamr has both str-17 and str17 historical branch conventions.
matches_ticket() {
    local lower=${1,,}
    [[ $lower =~ (^|[^a-z0-9])str-?${number}([^0-9]|$) ]]
}
is_excluded() {
    local excluded
    for excluded in "${excludes[@]}"; do
        [[ $1 == "$excluded" ]] && return 0
    done
    return 1
}
if [[ -n $self_branch ]]; then
    matches_ticket "$self_branch" || usage
    excludes+=("$self_branch")
fi

# Capture command status directly: never pipe a failed listing into a parser
# that can accidentally turn incomplete evidence into a clear verdict.
if current=$(git rev-parse --abbrev-ref HEAD 2>&1); then
    if [[ -z $self_branch ]] && matches_ticket "$current"; then excludes+=("$current"); fi
else
    failures+=("current branch: $current")
fi
error_file=$(mktemp) || { echo 'Could not create error capture file' >&2; exit 3; }
trap 'rm -f "$error_file"' EXIT

# Enforce usable REST fields before emitting rows. A malformed page is an
# incomplete check, even when earlier pages contained valid rows.
pr_filter='.[] | if ((.number | type) == "number" and (.head.ref | type) == "string" and (.head.ref | length) > 0 and (.state == "open" or .state == "closed")) then [.number, (if .merged_at then "MERGED" else (.state | ascii_upcase) end), .head.ref] | @tsv else error("Malformed PR record") end'
if prs=$(gh api --paginate 'repos/jasadams/streamr/pulls?state=all&per_page=100' --jq "$pr_filter" 2>"$error_file"); then
    while IFS= read -r row; do
        [[ -n $row ]] || continue
        if [[ $row =~ ^([0-9]+)$'\t'(OPEN|CLOSED|MERGED)$'\t'([^$'\t']+)$ ]]; then
            pr_number=${BASH_REMATCH[1]}
            pr_state=${BASH_REMATCH[2]}
            branch=${BASH_REMATCH[3]}
            if matches_ticket "$branch" && ! is_excluded "$branch"; then
                hits+=("PR #$pr_number ($pr_state): $branch")
            fi
        else
            failures+=("PR listing: malformed row '$row'")
        fi
    done <<< "$prs"
else
    failures+=("PR listing: $(cat "$error_file")")
fi

classify_branch() {
    local source=$1 branch=$2
    if [[ $branch == stranded/* ]]; then
        # A remote tombstone preserves work; it never hides a PR above.
        if matches_ticket "${branch#stranded/}"; then
            preserved+=("$source: $branch")
        fi
    elif matches_ticket "$branch" && ! is_excluded "$branch"; then
        hits+=("$source: $branch")
    fi
}
if refs=$(git ls-remote --heads "$remote" 2>"$error_file"); then
    while IFS= read -r row; do
        [[ -n $row ]] || continue
        if [[ $row =~ ^[0-9a-fA-F]+$'\t'refs/heads/(.+)$ ]]; then
            classify_branch "remote branch $remote" "${BASH_REMATCH[1]}"
        else
            failures+=("remote branches: malformed row '$row'")
        fi
    done <<< "$refs"
else
    failures+=("remote branches ($remote): $(cat "$error_file")")
fi

flush_worktree() {
    local marker
    [[ -n $wt_branch ]] || return 0
    if matches_ticket "$wt_branch" && ! is_excluded "$wt_branch"; then
        if [[ $wt_branch == stranded/* ]]; then
            preserved+=("local worktree $wt_path: $wt_branch")
        elif [[ -f $wt_path/STRANDED.md ]]; then
            if marker=$(cat "$wt_path/STRANDED.md" 2>"$error_file"); then
                # A marker must name this exact ticket. It only suppresses
                # this local worktree and its branch, never remote/PR work.
                if [[ ${marker,,} =~ (^|[^a-z0-9])str-${number}([^a-z0-9]|$) ]]; then
                    preserved+=("local worktree $wt_path: $wt_branch")
                    tombstoned+=("$wt_branch")
                else
                    hits+=("local worktree $wt_path: $wt_branch")
                fi
            else
                failures+=("stranded marker ($wt_path): $(cat "$error_file")")
            fi
        else
            hits+=("local worktree $wt_path: $wt_branch")
        fi
    fi
}
if worktrees=$(git worktree list --porcelain 2>"$error_file"); then
    wt_path=''
    wt_branch=''
    while IFS= read -r row; do
        case $row in
            'worktree '*)
                flush_worktree
                wt_path=${row#worktree }
                wt_branch=''
                ;;
            'branch refs/heads/'*) wt_branch=${row#branch refs/heads/} ;;
            '') flush_worktree; wt_path=''; wt_branch='' ;;
            HEAD\ *|detached|bare|locked|locked\ *|prunable|prunable\ *) ;;
            *) failures+=("worktree listing: malformed row '$row'") ;;
        esac
    done <<< "$worktrees"
    flush_worktree
else
    failures+=("local worktrees: $(cat "$error_file")")
fi
if branches=$(git branch --list --format='%(refname:short)' 2>"$error_file"); then
    for branch in "${tombstoned[@]}"; do excludes+=("$branch"); done
    while IFS= read -r branch; do
        [[ -n $branch ]] || continue
        classify_branch 'local branch' "$branch"
    done <<< "$branches"
else
    failures+=("local branches: $(cat "$error_file")")
fi

if [[ ${#preserved[@]} -gt 0 ]]; then
    echo 'PRESERVED STRANDED WORK (available for salvage):'
    printf '  ~ %s\n' "${preserved[@]}"
fi
if [[ ${#hits[@]} -gt 0 ]]; then
    echo "IN FLIGHT: STR-$number"
    printf '  - %s\n' "${hits[@]}"
fi
if [[ ${#failures[@]} -gt 0 ]]; then
    echo "INCOMPLETE: failing closed; do not claim STR-$number"
    printf '  ! %s\n' "${failures[@]}"
    exit 3
fi
[[ ${#hits[@]} -eq 0 ]] || exit 1
echo "CLEAR: STR-$number"
exit 0
