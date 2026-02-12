from __future__ import annotations

import logging
import socket
from typing import Dict

import grpc

from config.app_cfg import app_cfg
from utils.time_ut import uptime_sec

from proto import info_pb2, info_pb2_grpc

log = logging.getLogger("info_srv")


class InfoServicer(info_pb2_grpc.InfoServicer):
    def All(self, request: info_pb2.InfoRequest, context: grpc.ServicerContext) -> info_pb2.InfoResponse:
        labels: Dict[str, str] = {
            "service": "ytcomments",
        }
        return info_pb2.InfoResponse(
            app_name=app_cfg.app_name,
            instance_id=app_cfg.instance_id,
            host=socket.gethostname(),
            version=app_cfg.version,
            uptime=int(uptime_sec()),
            labels=labels,
            metrics={},
            build_hash=app_cfg.build_hash,
            build_time=app_cfg.build_time,
        )