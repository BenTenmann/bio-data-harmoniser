#!/bin/bash

set -exuo pipefail

OS_NAME=$(uname -s)

if [[ "$OS_NAME" = "Darwin" ]]; then
  if ! command -v brew &> /dev/null; then
    echo "Homebrew is not installed. Please install it and try again."
    exit 1
  fi
  brew services start postgresql
else
  if ! command -v pg_ctlcluster &> /dev/null; then
    echo "PostgreSQL is not installed. Please install it and try again."
    exit 1
  fi
  pg_ctlcluster 13 main start -- --wait
fi
