#!/bin/sh
source ../.venv/bin/activate
uv pip install --upgrade pip
uv pip install -r requirements.txt
deactivate
