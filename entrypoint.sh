#!/bin/bash

set -euxo pipefail

# we source the .bashrc file to ensure that npm is available
source ~/.bashrc || true

make setup_airflow
make local -j 5
