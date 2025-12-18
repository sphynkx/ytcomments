from proto import ytcomments_pb2, ytcomments_pb2_grpc
from pymongo.errors import PyMongoError
from bson.objectid import ObjectId
import base64
import json
import time


class YtCommentsService(ytcomments_pb2_grpc.YtCommentsServicer):
    def __init__(self, db):
        self.db = db

    async def ListTop(self, request, context):
        try:
            # Top level filter
            filter_query = {"video_id": request.video_id, "parent_id": None}
            if not request.include_deleted:
                filter_query["is_deleted"] = False

            # New or old at begin??
            sort_order = [("created_at", -1)] if request.sort == ytcomments_pb2.SortOrder.NEWEST_FIRST else [("created_at", 1)]

            # Pagination
            page_size = max(1, request.page_size)

            # Find comment in DB
            comments = self.db.comments.find(filter_query).sort(sort_order).limit(page_size)
            result_comments = []
            async for comment in comments:
                result_comments.append(
                    ytcomments_pb2.Comment(
                        id=str(comment["_id"]),
                        video_id=comment["video_id"],
                        parent_id="",
                        content_raw=comment["content_raw"],
                        content_html=comment["content_html"],
                        is_deleted=comment["is_deleted"],
                        edited=comment["edited"],
                        created_at=comment["created_at"],
                        updated_at=comment["updated_at"],
                        user_uid=comment["user_uid"],
                        username=comment.get("username", ""),
                        channel_id=comment.get("channel_id", ""),
                        reply_count=comment["reply_count"],
                    )
                )

            total_count = await self.db.comments.count_documents(filter_query)

            return ytcomments_pb2.ListTopResponse(
                items=result_comments,
                next_page_token="",  # Pagination (still not implemented!!)
                total_count=total_count,
            )
        except PyMongoError as e:
            context.set_code(ytcomments_pb2_grpc.StatusCode.INTERNAL)
            context.set_details(f"Database error: {str(e)}")
            return ytcomments_pb2.ListTopResponse()

    async def ListReplies(self, request, context):
        try:
            # Decode cursor (page_token)
            cursor = None
            if request.page_token:
                try:
                    cursor_data = json.loads(base64.b64decode(request.page_token).decode("utf-8"))
                    cursor = {
                        "created_at": cursor_data["created_at"],
                        "_id": ObjectId(cursor_data["id"]),
                    }
                except (ValueError, KeyError, base64.binascii.Error):
                    context.set_code(ytcomments_pb2_grpc.StatusCode.INVALID_ARGUMENT)
                    context.set_details("Invalid page_token.")
                    return ytcomments_pb2.ListRepliesResponse()

            # Form filter
            filter_query = {"parent_id": ObjectId(request.parent_id)}
            if not request.include_deleted:
                filter_query["is_deleted"] = False

            # Default is NEWEST_FIRST
            sort_order = [("created_at", -1)] if request.sort == ytcomments_pb2.SortOrder.NEWEST_FIRST else [("created_at", 1)]

            # Add cursors to filter
            if cursor:
                if request.sort == ytcomments_pb2.SortOrder.NEWEST_FIRST:
                    filter_query["$or"] = [
                        {"created_at": {"$lt": cursor["created_at"]}},
                        {
                            "created_at": cursor["created_at"],
                            "_id": {"$lt": cursor["_id"]},
                        },
                    ]
                elif request.sort == ytcomments_pb2.SortOrder.OLDEST_FIRST:
                    filter_query["$or"] = [
                        {"created_at": {"$gt": cursor["created_at"]}},
                        {
                            "created_at": cursor["created_at"],
                            "_id": {"$gt": cursor["_id"]},
                        },
                    ]

            # Make request to DB
            comments_cursor = (
                self.db.comments.find(filter_query)
                .sort(sort_order)
                .limit(request.page_size)
            )

            # Save comments list
            comments = []
            async for comment in comments_cursor:
                comments.append(
                    ytcomments_pb2.Comment(
                        id=str(comment["_id"]),
                        video_id=comment["video_id"],
                        parent_id=str(comment["parent_id"]),
                        content_raw=comment["content_raw"],
                        content_html=comment["content_html"],
                        is_deleted=comment["is_deleted"],
                        edited=comment["edited"],
                        created_at=comment["created_at"],
                        updated_at=comment["updated_at"],
                        user_uid=comment["user_uid"],
                        username=comment.get("username", ""),
                        channel_id=comment.get("channel_id", ""),
                        reply_count=comment["reply_count"],
                    )
                )

            # Make next_page_token
            next_page_token = None
            if comments:
                last_comment = comments[-1]
                next_page_token = base64.b64encode(
                    json.dumps(
                        {
                            "created_at": last_comment.created_at,
                            "id": last_comment.id,
                        }
                    ).encode("utf-8")
                ).decode("utf-8")

            # Count all replys
            total_count = await self.db.comments.count_documents(filter_query)

            return ytcomments_pb2.ListRepliesResponse(
                items=comments,
                next_page_token=next_page_token or "",
                total_count=total_count,
            )

        except PyMongoError as e:
            context.set_code(ytcomments_pb2_grpc.StatusCode.INTERNAL)
            context.set_details(f"Database error: {str(e)}")
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