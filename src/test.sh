#!/usr/bin/env bash

set -u # crash on missing env
set -e # stop on any error

echo() {
   builtin echo -e "$@"
}

export COVERAGE_FILE="/tmp/.coverage"

# Find all python files in ./gobeventconsumer
FILES=$(find . -type f -name "*.py" | grep gobeventconsumer)

# DIRTY_FILES will skip mypy checks, but other checks will run
DIRTY_FILES=(
  ./gobeventconsumer/__main__.py
)

# CLEAN_FILES is FILES - DIRTY_FILES. CLEAN_FILES will see all checks
CLEAN_FILES=$(echo ${FILES[@]} ${DIRTY_FILES[@]} | tr ' ' '\n' | sort | uniq -u | tr '\n' ' ')

echo "Running mypy on non-dirty files"
#mypy --follow-imports=skip --install-types ${CLEAN_FILES[@]}

echo "\nRunning unit tests"
coverage run --source=gobeventconsumer -m pytest

echo "Coverage report"
coverage report --fail-under=100

echo "\nCheck if Black finds no potential reformat fixes"
black --check --diff ${FILES[@]}

echo "\nCheck for potential import sort"
isort --check --diff --src-path=gobeventconsumer ${FILES[@]}

echo "\nRunning Flake8 style checks"
flake8 ${FILES[@]}

echo "\nChecks complete"
