from proto import ytcomments_pb2, ytcomments_pb2_grpc

class YtCommentsService(ytcomments_pb2_grpc.YtCommentsServicer):
    def __init__(self, db):
        self.db = db
    
    async def ListTop(self, request, context):
        # dummy
        return ytcomments_pb2.ListTopResponse()

    async def ListReplies(self, request, context):
        return ytcomments_pb2.ListRepliesResponse()

    async def Create(self, request, context):
        return ytcomments_pb2.CreateCommentResponse()

    async def Edit(self, request, context):
        return ytcomments_pb2.EditCommentResponse()

    async def Delete(self, request, context):
        return ytcomments_pb2.DeleteCommentResponse()

    async def Restore(self, request, context):
        return ytcomments_pb2.RestoreCommentResponse()

    async def GetCounts(self, request, context):
        return ytcomments_pb2.GetCountsResponse()