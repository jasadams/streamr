#!/usr/bin/env bash
# Run the CI Clippy check through the repository's container runner.
set -euo pipefail

usage() {
    cat <<'EOF'
Usage: scripts/preflight-clippy.sh [-p <crate>]... [--help]

Run Clippy with the CI flags and --locked. Defaults to the whole workspace;
repeat -p <crate> to check selected packages instead.

Exit codes: 0 success, 1 Clippy/build failure, 2 usage error, 3 missing runner.
EOF
}

packages=()
while (( $# )); do
    case "$1" in
        -p)
            if (( $# < 2 )) || [[ -z "$2" || "$2" == -* ]]; then
                echo 'Error: -p requires a crate name.' >&2
                usage >&2
                exit 2
            fi
            packages+=(-p "$2")
            shift 2
            ;;
        --help)
            usage
            exit 0
            ;;
        *)
            echo "Error: unknown argument: $1" >&2
            usage >&2
            exit 2
            ;;
    esac
done

repo_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
runner="$repo_dir/scripts/cargo-dev"
if [[ ! -x "$runner" ]]; then
    echo "Error: missing executable runner: $runner" >&2
    exit 3
fi

scope=(--workspace)
if (( ${#packages[@]} )); then
    scope=("${packages[@]}")
fi
cd -- "$repo_dir"
if "$runner" clippy --locked --no-deps --all-features --all-targets "${scope[@]}" -- -D warnings; then
    exit 0
else
    exit 1
fi
