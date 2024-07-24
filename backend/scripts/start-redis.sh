#!/bin/bash

set -euxo pipefail

OS_NAME=$(uname -s)

if [[ "$OS_NAME" = "Darwin" ]]; then
  if ! command -v brew &> /dev/null; then
    echo "Homebrew is not installed. Please install it and try again."
    exit 1
  fi
  brew services start redis
else
  if ! command -v redis-server &> /dev/null; then
    echo "Redis is not installed. Please install it and try again."
    exit 1
  fi
  redis-server --daemonize yes
fi
