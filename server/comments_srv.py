from proto import ytcomments_pb2, ytcomments_pb2_grpc
from pymongo.errors import PyMongoError
from bson.objectid import ObjectId
import base64
import json
import time
import logging


class YtCommentsService(ytcomments_pb2_grpc.YtCommentsServicer):
    def __init__(self, db):
        self.db = db
        logging.basicConfig(level=logging.DEBUG, format="[%(levelname)s] %(message)s")
        logger = logging.getLogger("YtCommentsService")

    async def ListTop(self, request, context):
        self.logger.debug(f"Received ListTop request: {request}")
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
            self.logger.debug("ListTop completed successfully.")
        except PyMongoError as e:
            self.logger.error(f"ListTop failed: {e}")
            context.set_code(ytcomments_pb2_grpc.StatusCode.INTERNAL)
            context.set_details(f"Database error: {str(e)}")
            return ytcomments_pb2.ListTopResponse()

    async def ListReplies(self, request, context):
        self.logger.debug(f"Received ListReplies request: {request}")
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
            self.logger.debug("ListReplies completed successfully.")

        except PyMongoError as e:
            self.logger.error(f"ListReplies failed: {e}")
            context.set_code(ytcomments_pb2_grpc.StatusCode.INTERNAL)
            context.set_details(f"Database error: {str(e)}")
            return ytcomments_pb2.ListRepliesResponse()


    async def Create(self, request, context):
        self.logger.debug(f"Received Create request: {request}")
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
            self.logger.debug("Create completed successfully.")

        except PyMongoError as e:
            self.logger.error(f"Create failed: {e}")
            context.set_code(ytcomments_pb2_grpc.StatusCode.INTERNAL)
            context.set_details(f"Database error: {str(e)}")
            return ytcomments_pb2.CreateCommentResponse()

    async def Edit(self, request, context):
        self.logger.debug(f"Received Edit request: {request}")
        try:
            # Is comment exists??
            comment = await self.db.comments.find_one({"_id": ObjectId(request.comment_id)})
            if not comment:
                context.set_code(ytcomments_pb2_grpc.StatusCode.NOT_FOUND)
                context.set_details("Comment not found.")
                return ytcomments_pb2.EditCommentResponse()

            # Check is deleted
            if comment.get("is_deleted", False):
                context.set_code(ytcomments_pb2_grpc.StatusCode.FAILED_PRECONDITION)
                context.set_details("Cannot edit a deleted comment.")
                return ytcomments_pb2.EditCommentResponse()

            # Update it..
            updated_comment = {
                "content_raw": request.content_raw,
                "content_html": comment["content_html"],  # `content_html` field is prepared on app side.
                "edited": True,
                "updated_at": int(time.time() * 1000),
            }

            result = await self.db.comments.update_one(
                {"_id": ObjectId(request.comment_id)},
                {"$set": updated_comment}
            )
            if result.modified_count == 0:
                context.set_code(ytcomments_pb2_grpc.StatusCode.UNKNOWN)
                context.set_details("Failed to update the comment.")
                return ytcomments_pb2.EditCommentResponse()

            # Return updated comment
            updated_comment_from_db = await self.db.comments.find_one({"_id": ObjectId(request.comment_id)})
            return ytcomments_pb2.EditCommentResponse(
                comment=ytcomments_pb2.Comment(
                    id=str(updated_comment_from_db["_id"]),
                    video_id=updated_comment_from_db["video_id"],
                    parent_id=updated_comment_from_db.get("parent_id", ""),
                    content_raw=updated_comment_from_db["content_raw"],
                    content_html=updated_comment_from_db["content_html"],
                    is_deleted=updated_comment_from_db["is_deleted"],
                    edited=updated_comment_from_db["edited"],
                    created_at=updated_comment_from_db["created_at"],
                    updated_at=updated_comment_from_db["updated_at"],
                    user_uid=updated_comment_from_db["user_uid"],
                    username=updated_comment_from_db["username"],
                    channel_id=updated_comment_from_db["channel_id"],
                    reply_count=updated_comment_from_db["reply_count"],
                )
            )
            self.logger.debug("Edit completed successfully.")
        except PyMongoError as e:
            self.logger.error(f"Edit failed: {e}")
            context.set_code(ytcomments_pb2_grpc.StatusCode.INTERNAL)
            context.set_details(f"Database error: {str(e)}")
            return ytcomments_pb2.EditCommentResponse()

    async def Delete(self, request, context):
        self.logger.debug(f"Received Delete request: {request}")
        try:
            # Is comment exists??
            comment = await self.db.comments.find_one({"_id": ObjectId(request.comment_id)})
            if not comment:
                context.set_code(ytcomments_pb2_grpc.StatusCode.NOT_FOUND)
                context.set_details("Comment not found.")
                return ytcomments_pb2.DeleteCommentResponse()

            # Full delete (optionally)
            if request.hard_delete:
                result = await self.db.comments.delete_one({"_id": ObjectId(request.comment_id)})
                if result.deleted_count == 0:
                    context.set_code(ytcomments_pb2_grpc.StatusCode.UNKNOWN)
                    context.set_details("Failed to hard delete the comment.")
                    return ytcomments_pb2.DeleteCommentResponse()
            else:
                # Soft delete
                update_result = await self.db.comments.update_one(
                    {"_id": ObjectId(request.comment_id)},
                    {
                        "$set": {
                            "is_deleted": True,
                            "deleted_at": int(time.time() * 1000),
                            "deleted_by": request.ctx.user_uid,
                        }
                    }
                )
                if update_result.modified_count == 0:
                    context.set_code(ytcomments_pb2_grpc.StatusCode.UNKNOWN)
                    context.set_details("Failed to soft delete the comment.")
                    return ytcomments_pb2.DeleteCommentResponse()

            # return updates comment if deletion was successful.
            deleted_comment = await self.db.comments.find_one({"_id": ObjectId(request.comment_id)})

            return ytcomments_pb2.DeleteCommentResponse(
                comment=ytcomments_pb2.Comment(
                    id=str(deleted_comment["_id"]),
                    video_id=deleted_comment["video_id"],
                    parent_id=deleted_comment.get("parent_id", ""),
                    content_raw=deleted_comment["content_raw"],
                    content_html=deleted_comment["content_html"],
                    is_deleted=deleted_comment["is_deleted"],
                    edited=deleted_comment["edited"],
                    created_at=deleted_comment["created_at"],
                    updated_at=deleted_comment["updated_at"],
                    user_uid=deleted_comment["user_uid"],
                    username=deleted_comment["username"],
                    channel_id=deleted_comment["channel_id"],
                    reply_count=deleted_comment["reply_count"],
                )
            )
            self.logger.debug("Delete completed successfully.")
        except PyMongoError as e:
            self.logger.error(f"Delete failed: {e}")
            context.set_code(ytcomments_pb2_grpc.StatusCode.INTERNAL)
            context.set_details(f"Database error: {str(e)}")
            return ytcomments_pb2.DeleteCommentResponse()

    async def Restore(self, request, context):
        self.logger.debug(f"Received Restore request: {request}")
        try:
            # Is comment exists??
            comment = await self.db.comments.find_one({"_id": ObjectId(request.comment_id)})
            if not comment:
                context.set_code(ytcomments_pb2_grpc.StatusCode.NOT_FOUND)
                context.set_details("Comment not found.")
                return ytcomments_pb2.RestoreCommentResponse()

            # Check is deleted
            if not comment.get("is_deleted", False):
                context.set_code(ytcomments_pb2_grpc.StatusCode.FAILED_PRECONDITION)
                context.set_details("Comment is not deleted.")
                return ytcomments_pb2.RestoreCommentResponse()

            # Restore comment
            update_result = await self.db.comments.update_one(
                {"_id": ObjectId(request.comment_id)},
                {
                    "$set": {
                        "is_deleted": False,
                        "deleted_at": None,
                        "deleted_by": None,
                    }
                }
            )
            if update_result.modified_count == 0:
                context.set_code(ytcomments_pb2_grpc.StatusCode.UNKNOWN)
                context.set_details("Failed to restore the comment.")
                return ytcomments_pb2.RestoreCommentResponse()

            # Return updated comment state
            restored_comment = await self.db.comments.find_one({"_id": ObjectId(request.comment_id)})

            return ytcomments_pb2.RestoreCommentResponse(
                comment=ytcomments_pb2.Comment(
                    id=str(restored_comment["_id"]),
                    video_id=restored_comment["video_id"],
                    parent_id=restored_comment.get("parent_id", ""),
                    content_raw=restored_comment["content_raw"],
                    content_html=restored_comment["content_html"],
                    is_deleted=restored_comment["is_deleted"],
                    edited=restored_comment["edited"],
                    created_at=restored_comment["created_at"],
                    updated_at=restored_comment["updated_at"],
                    user_uid=restored_comment["user_uid"],
                    username=restored_comment["username"],
                    channel_id=restored_comment["channel_id"],
                    reply_count=restored_comment["reply_count"],
                )
            )
            self.logger.debug("Restore completed successfully.")
        except PyMongoError as e:
            self.logger.error(f"Restore failed: {e}")
            context.set_code(ytcomments_pb2_grpc.StatusCode.INTERNAL)
            context.set_details(f"Database error: {str(e)}")
            return ytcomments_pb2.RestoreCommentResponse()

    async def GetCounts(self, request, context):
        self.logger.debug(f"Received GetCounts request: {request}")
        try:
            # Count only visible top level comments.
            filter_top_level = {"video_id": request.video_id, "parent_id": None}
            if not request.ctx.is_moderator:  # Consider moderator (for visibility deleted comments)
                filter_top_level["is_deleted"] = False

            # Top level comments
            top_level_count = await self.db.comments.count_documents(filter_top_level)

            # All comments
            filter_all = {"video_id": request.video_id}
            if not request.ctx.is_moderator:
                filter_all["is_deleted"] = False

            # All comments count
            total_count = await self.db.comments.count_documents(filter_all)

            return ytcomments_pb2.GetCountsResponse(
                top_level_count=top_level_count,
                total_count=total_count
            )
            self.logger.debug("GetCounts completed successfully.")
        except PyMongoError as e:
            self.logger.error(f"GetCounts failed: {e}")
            context.set_code(ytcomments_pb2_grpc.StatusCode.INTERNAL)
            context.set_details(f"Database error: {str(e)}")
            return ytcomments_pb2.GetCountsResponse()