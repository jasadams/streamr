# Source this helper to resolve the main checkout from any linked worktree.
# Adapted from Kyomi; absolute common-dir output requires Git >= 2.31.
resolve_canonical_root() {
    if ! git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
        echo 'ERROR: not inside a Git worktree.' >&2
        return 1
    fi
    local common_dir
    if ! common_dir="$(git rev-parse --path-format=absolute --git-common-dir)"; then
        echo 'ERROR: cannot resolve canonical clone (requires Git >= 2.31).' >&2
        return 1
    fi
    dirname "$common_dir"
}

resolve_review_logs_dir() {
    local canonical_root
    canonical_root="$(resolve_canonical_root)" || return 1
    printf '%s/docs/review-logs\n' "$canonical_root"
}
