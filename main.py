from __future__ import annotations

import logging
from concurrent import futures

import grpc

from utils.log_ut import setup_logging

try:
    from dotenv import load_dotenv  # type: ignore
    load_dotenv()
except Exception:
    pass

from config.app_cfg import app_cfg
from db.couchbase_db import ping

from proto import ytcomments_pb2_grpc as ytcomments_pbg
from proto import info_pb2_grpc as info_pbg

from srv.ytcomments_grpc_srv import YtCommentsServicer
from srv.info_grpc_srv import InfoServicer

log = logging.getLogger("main")


def main() -> None:
    setup_logging()
    log.info("starting ytcomments (grpc) on %s:%s", app_cfg.grpc_host, app_cfg.grpc_port)

    if not ping():
        raise SystemExit("Couchbase ping failed; refusing to start")

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=16))
    ytcomments_pbg.add_YtCommentsServicer_to_server(YtCommentsServicer(), server)
    info_pbg.add_InfoServicer_to_server(InfoServicer(), server)

    server.add_insecure_port(f"{app_cfg.grpc_host}:{app_cfg.grpc_port}")
    server.start()
    log.info("server started")
    server.wait_for_termination()


if __name__ == "__main__":
    main()