import asyncio
from grpc.aio import server as grpc_server
from grpc_reflection.v1alpha import reflection
from concurrent.futures import ThreadPoolExecutor
import logging

from server.comments_srv import YtCommentsService
from config.main_cfg import Config
from db.mongo_db import MongoDatabase
from proto import ytcomments_pb2_grpc, ytcomments_pb2


async def serve():
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    config = Config()
    logger.info("Configuration loaded.")

    mongo_client = MongoDatabase.connect(config)
    db = mongo_client[config.MONGO_DB_NAME]
    logger.info("Connected to MongoDB.")

    grpc = grpc_server(ThreadPoolExecutor(max_workers=10))
    ytcomments_service = YtCommentsService(db)
    ytcomments_pb2_grpc.add_YtCommentsServicer_to_server(ytcomments_service, grpc)

    reflection.enable_server_reflection(
        service_names=[
            ytcomments_pb2.DESCRIPTOR.services_by_name['YtComments'].full_name,
            reflection.SERVICE_NAME,
        ],
        server=grpc,
    )

    host, port = config.YTCOMMENTS_HOST, config.YTCOMMENTS_PORT
    server_address = f"{host}:{port}"
    grpc.add_insecure_port(server_address)
    logger.info(f"gRPC server listening at {server_address}.")

    await grpc.start()
    await grpc.wait_for_termination()

if __name__ == "__main__":
    asyncio.run(serve())