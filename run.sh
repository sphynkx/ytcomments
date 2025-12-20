#!/bin/bash

bash install/docker/replica-init.sh

source .venv/bin/activate || true # Try to activate if exists

# Ensure python path sees current dir
export PYTHONPATH=$PYTHONPATH:.

python main.py