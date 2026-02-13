from __future__ import annotations

import logging
import signal
import threading
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

    stop_event = threading.Event()

    def _on_signal(signum, frame):  # noqa: ARG001
        # First signal initiates graceful shutdown
        if not stop_event.is_set():
            log.info("signal %s received; shutting down...", signum)
            stop_event.set()
        else:
            # Second signal: exit faster
            log.warning("signal %s received again; forcing stop", signum)
            try:
                server.stop(grace=0)
            except Exception:
                pass

    signal.signal(signal.SIGINT, _on_signal)
    signal.signal(signal.SIGTERM, _on_signal)

    # Wait until SIGINT/SIGTERM
    stop_event.wait()

    # Graceful stop: allow in-flight RPCs to finish
    try:
        server.stop(grace=5).wait(timeout=6)
        log.info("server stopped")
    except Exception as e:
        log.warning("server stop error: %s", e)


if __name__ == "__main__":
    main()