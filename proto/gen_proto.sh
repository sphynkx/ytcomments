#!/usr/bin/env bash
set -euo pipefail

# Generate stubs from ytcomments.proto
# Make sure that ytcomments.proto is identical to one from yurtube service!!

cd "$(dirname "$0")"

source ../.venv/bin/activate 

python -m grpc_tools.protoc \
  -I . \
  --python_out=. \
  --grpc_python_out=. \
  ytcomments.proto

sed -i 's/^import ytcomments_pb2 as ytcomments__pb2/from . import ytcomments_pb2 as ytcomments__pb2/' ytcomments_pb2_grpc.py

echo "Generated: ytcomments_pb2.py ytcomments_pb2_grpc.py in $(pwd)"

##############
python -m grpc_tools.protoc \
  -I . \
  --python_out=. \
  --grpc_python_out=. \
  info.proto

sed -i 's/^import info_pb2 as info__pb2/from . import info_pb2 as info__pb2/' info_pb2_grpc.py

echo "Generated: info_pb2.py info_pb2_grpc.py in $(pwd)"
