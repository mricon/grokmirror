#!/usr/bin/env sh

set -eu

# Run an import smoke check and pytest under a spread of interpreter versions
# across the supported range (see `requires-python` in pyproject.toml). This
# is not part of ci.sh because materialising a venv per version is slow;
# invoke this before releases and after changes that could depend on
# version-specific runtime behaviour (import-time annotations, stdlib APIs,
# etc.).
#
# Missing interpreters are pulled automatically from uv's managed Python
# cache (python-build-standalone), so no system packages or sudo are needed.
#
# Override the version list: PYTHONS="3.9 3.15" ./ci-matrix.sh

# The versions servers actually run, not every release in between: 3.9 is
# AlmaLinux 9's python3, 3.12 is AlmaLinux 10's, and the newest release
# catches breakage early.
PYTHONS="${PYTHONS:-3.9 3.12 3.15}"

# Install any requested interpreters that are missing. This is an explicit
# step because a dev may have set UV_PYTHON_DOWNLOADS=manual globally to
# prevent `uv sync` from reaching out to the network mid-workflow; running
# it once up front scopes the network access to this script.
# shellcheck disable=SC2086
uv python install $PYTHONS

# Collect failures so the run reports a complete matrix instead of bailing on
# the first broken interpreter.
failed=""

for py in $PYTHONS; do
    printf '\n=== Python %s ===\n' "$py"
    # Each version gets its own project environment so switching interpreters
    # does not thrash the default .venv and each sync is incremental.
    UV_PROJECT_ENVIRONMENT=".venv-$py"
    export UV_PROJECT_ENVIRONMENT
    # --locked for the same reason as in ci.sh: every interpreter in the sweep
    # should be testing the dependency versions recorded in uv.lock.
    if ! uv sync --all-extras --all-groups --locked --python "$py"; then
        failed="$failed $py(sync)"
        continue
    fi
    if ! uv run python -c 'import grokmirror, sys; print("import grokmirror OK on", sys.version.split()[0])'; then
        failed="$failed $py(import)"
        continue
    fi
    if ! uv run pytest --durations=20; then
        failed="$failed $py(pytest)"
        continue
    fi
done

if [ -n "$failed" ]; then
    printf '\nFAILURES:%s\n' "$failed"
    exit 1
fi

printf '\nAll interpreters passed: %s\n' "$PYTHONS"
