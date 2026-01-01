import grpc
from grpc.aio import server as aio_server

from grpc.health.v1 import health_pb2_grpc, health_pb2
from grpc_reflection.v1alpha import reflection

from proto import info_pb2_grpc

async def create_server(info_service, port: int):
    """
    Creates and starts a gRPC server, registering the necessary services.

    :param info_service: InfoService instance.
    :param port: Port for the gRPC server.
    :return: Reference to the running gRPC server.
    """
    server = aio_server()

    health_servicer = health_pb2_grpc.HealthServicer()
    health_pb2_grpc.add_HealthServicer_to_server(health_servicer, server)
    health_servicer.set("", health_pb2.HealthCheckResponse.SERVING)

    info_pb2_grpc.add_InfoServicer_to_server(info_service, server)

    SERVICE_NAMES = (
        reflection.SERVICE_NAME,
        health_pb2.DESCRIPTOR.services_by_name["Health"].full_name,
        info_pb2_grpc.DESCRIPTOR.services_by_name["Info"].full_name,
    )
    reflection.enable_server_reflection(SERVICE_NAMES, server)

    server.add_insecure_port(f"[::]:{port}")
    await server.start()
    print(f"gRPC server listening on port {port}...")
    return server