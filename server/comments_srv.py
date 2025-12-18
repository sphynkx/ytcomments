from proto import ytcomments_pb2, ytcomments_pb2_grpc
from pymongo.errors import PyMongoError
from bson.objectid import ObjectId
import time


class YtCommentsService(ytcomments_pb2_grpc.YtCommentsServicer):
    def __init__(self, db):
        self.db = db

    async def ListTop(self, request, context):
        return ytcomments_pb2.ListTopResponse()

    async def ListReplies(self, request, context):
        return ytcomments_pb2.ListRepliesResponse()

    async def Create(self, request, context):
        try:
            now = int(time.time() * 1000)  # ms since epoch

            # New comment structure
            new_comment = {
                "video_id": request.video_id,
                "user_uid": request.ctx.user_uid,
                "username": request.ctx.username,
                "channel_id": request.ctx.channel_id,
                "parent_id": ObjectId(request.parent_id) if request.parent_id else None,
                "content_raw": request.content_raw,
                "content_html": request.content_html,
                "is_deleted": False,
                "deleted_by": None,
                "deleted_at": None,
                "edited": False,
                "created_at": now,
                "updated_at": now,
                "reply_count": 0,
            }

            # Insert comment to MongoDB
            result = await self.db.comments.insert_one(new_comment)

            # Optionaly - update replys count in parent comment.
            if request.parent_id:
                await self.db.comments.update_one(
                    {"_id": ObjectId(request.parent_id)},
                    {"$inc": {"reply_count": 1}}
                )

            # Form reply
            return ytcomments_pb2.CreateCommentResponse(
                comment=ytcomments_pb2.Comment(
                    id=str(result.inserted_id),
                    video_id=request.video_id,
                    parent_id=request.parent_id or "",
                    content_raw=request.content_raw,
                    content_html=request.content_html,
                    is_deleted=False,
                    edited=False,
                    created_at=now,
                    updated_at=now,
                    user_uid=request.ctx.user_uid,
                    username=request.ctx.username,
                    channel_id=request.ctx.channel_id,
                    reply_count=0,
                )
            )

        except PyMongoError as e:
            context.set_code(ytcomments_pb2_grpc.StatusCode.INTERNAL)
            context.set_details(f"Database error: {str(e)}")
            return ytcomments_pb2.CreateCommentResponse()

    async def Edit(self, request, context):
        return ytcomments_pb2.EditCommentResponse()

    async def Delete(self, request, context):
        return ytcomments_pb2.DeleteCommentResponse()

    async def Restore(self, request, context):
        return ytcomments_pb2.RestoreCommentResponse()

    async def GetCounts(self, request, context):
        return ytcomments_pb2.GetCountsResponse()