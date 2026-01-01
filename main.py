import asyncio
import signal
import time
from grpc.aio import server as grpc_server
from grpc_reflection.v1alpha import reflection
from concurrent.futures import ThreadPoolExecutor
import logging

from server.comments_srv import YtCommentsService
from services.info_srv import InfoService
from config.main_cfg import Config
from db.mongo_db import MongoDatabase
from proto import ytcomments_pb2_grpc, ytcomments_pb2
from grpc.health.v1 import health_pb2, health_pb2_grpc
from grpc.health.v1.health import HealthServicer
from proto import info_pb2_grpc, info_pb2


async def wait_mongo_ready(db, logger, timeout_sec: int = 20):
    deadline = time.monotonic() + timeout_sec
    last_err = None
    while time.monotonic() < deadline:
        try:
            db.command('ping')
            logger.info("Mongo ping OK")
            return
        except Exception as e:
            last_err = e
            logger.warning("Mongo ping failed: %s", e)
            await asyncio.sleep(1)
    raise RuntimeError(f"Mongo not ready: {last_err}")


async def serve():
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    config = Config()
    logger.info("Configuration loaded.")

    # future tunings for db-connection (see db/mongo_db.py)
    # serverSelectionTimeoutMS=2000, connectTimeoutMS=2000, socketTimeoutMS=5000
    mongo_client = MongoDatabase.connect(config)
    db = mongo_client[config.MONGO_DB_NAME]
    logger.info("Connected to MongoDB.")
    await wait_mongo_ready(db, logger, timeout_sec=20)

    grpc = grpc_server(ThreadPoolExecutor(max_workers=10))
    ytcomments_service = YtCommentsService(db)
    ytcomments_pb2_grpc.add_YtCommentsServicer_to_server(ytcomments_service, grpc)

    health_servicer = HealthServicer()
    # Set global (empty service-name) status to SERVING for admin checks
    health_servicer.set("", health_pb2.HealthCheckResponse.SERVING)
    health_pb2_grpc.add_HealthServicer_to_server(health_servicer, grpc)

    info_service = InfoService(
        app_name="YurTube Comments Service",
        host=f"{config.YTCOMMENTS_HOST}:{config.YTCOMMENTS_PORT}",
        version="1.0.0",
        build_hash="abc123",
        build_time="2025-12-30T12:00:00Z",
    )
    info_pb2_grpc.add_InfoServicer_to_server(info_service, grpc)

    reflection.enable_server_reflection(
        service_names=[
            ytcomments_pb2.DESCRIPTOR.services_by_name['YtComments'].full_name,
            info_pb2.DESCRIPTOR.services_by_name['Info'].full_name,
            health_pb2.DESCRIPTOR.services_by_name['Health'].full_name,
            reflection.SERVICE_NAME,
        ],
        server=grpc,
    )

    host, port = config.YTCOMMENTS_HOST, config.YTCOMMENTS_PORT
    server_address = f"{host}:{port}"
    grpc.add_insecure_port(server_address)
    logger.info(f"gRPC server listening at {server_address}.")

    await grpc.start()

    stop_event = asyncio.Event()

    def handle_sigint():
        logger.info("Received termination signal. Shutting down gracefully...")
        stop_event.set()

    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGINT, handle_sigint)
    loop.add_signal_handler(signal.SIGTERM, handle_sigint)

    await stop_event.wait()

    await grpc.stop(grace=10)  # 10 sec for finalizations
    logger.info("gRPC server shut down gracefully.")

if __name__ == "__main__":
    asyncio.run(serve())