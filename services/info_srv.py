import time
import socket
from proto import info_pb2, info_pb2_grpc

class InfoService(info_pb2_grpc.InfoServicer):
    def __init__(self, app_name, host, version, build_hash=None, build_time=None):
        self.app_name = app_name
        self.host = host
        self.version = version
        self.build_hash = build_hash
        self.build_time = build_time
        self.start_time = time.time()  # uptime origin

    def All(self, request, context):
        uptime_sec = time.time() - self.start_time
        instance_id = socket.gethostname()

        resp = info_pb2.InfoResponse(
            app_name=self.app_name,
            instance_id=instance_id,
            host=self.host,
            version=self.version,
            uptime=int(uptime_sec),
            labels={"env": "production"},  # optional labels
            metrics={"uptime_sec": uptime_sec},  # optional metrics
        )

        if self.build_hash:
            resp.build_hash = self.build_hash
        if self.build_time:
            resp.build_time = self.build_time

        return resp