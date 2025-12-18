from proto import ytcomments_pb2, ytcomments_pb2_grpc
from pymongo.errors import PyMongoError
from bson.objectid import ObjectId
import base64
import json
import time
import logging
import grpc


class YtCommentsService(ytcomments_pb2_grpc.YtCommentsServicer):
    def __init__(self, db):
        self.db = db
        logging.basicConfig(level=logging.DEBUG, format="[%(levelname)s] %(message)s")
        self.logger = logging.getLogger("YtCommentsService")
        print("[srv] YtCommentsService initialized")

    # ----- Helpers: legacy root/chunks -----

    def _legacy_root(self, video_id: str):
        try:
            return self.db.video_comments_root.find_one({"video_id": video_id})
        except Exception as e:
            print(f"[srv] legacy_root error: {e}")
            return None

    def _legacy_root_by_child(self, parent_id: str):
        try:
            return self.db.video_comments_root.find_one({f"comments.{parent_id}": {"$exists": True}})
        except Exception as e:
            print(f"[srv] legacy_root_by_child error: {e}")
            return None

    def _legacy_text(self, chunk_id: str, local_id: str) -> str:
        if not chunk_id or not local_id:
            return ""
        try:
            oid = ObjectId(chunk_id)
            chunk = self.db.video_comments_chunks.find_one({"_id": oid})
        except Exception:
            chunk = self.db.video_comments_chunks.find_one({"_id": chunk_id})
        if not chunk:
            return ""
        texts = chunk.get("texts", {}) or {}
        txt = texts.get(local_id)
        return txt if isinstance(txt, str) else ""

    def _visible(self, meta: dict, include_deleted: bool) -> bool:
        if include_deleted:
            return True
        return bool(meta.get("visible", True))

    def _sort_key(self, meta: dict) -> int:
        try:
            return int(meta.get("created_at", 0))
        except Exception:
            return 0

    def _legacy_children_ids(self, root: dict, parent_id: str) -> list:
        cmap = (root.get("tree_aux") or {}).get("children_map") or {}
        return list(cmap.get(parent_id, []) or [])

    def _legacy_roots_ids(self, root: dict) -> list:
        depth = (root.get("tree_aux") or {}).get("depth_index") or {}
        return list(depth.get("0", []) or [])

    def _reply_count_visible(self, root: dict, comments_map: dict, cid: str, include_deleted: bool) -> int:
        ids = self._legacy_children_ids(root, cid)
        cnt = 0
        for rid in ids:
            m = comments_map.get(rid) or {}
            if self._visible(m, include_deleted):
                cnt += 1
        return cnt

    # ----- RPCs -----

    async def ListTop(self, request, context):
        print(f"[srv] ListTop: req video_id={request.video_id} page_size={request.page_size} include_deleted={request.include_deleted} sort={request.sort}")
        try:
            root = self._legacy_root(request.video_id)
            if not root:
                print("[srv] ListTop: legacy root not found, returning empty")
                return ytcomments_pb2.ListTopResponse(items=[], next_page_token="", total_count=0)

            comments_map = root.get("comments", {}) or {}
            root_ids = self._legacy_roots_ids(root)
            print(f"[srv] ListTop: legacy root found, comments_map={len(comments_map)} roots={len(root_ids)}")

            items = []
            for cid in root_ids:
                meta = comments_map.get(cid) or {}
                if not isinstance(meta, dict):
                    continue
                if not self._visible(meta, request.include_deleted):
                    continue
                cref = meta.get("chunk_ref") or {}
                txt = self._legacy_text(cref.get("chunk_id", ""), cref.get("local_id", ""))
                created_sec = int(meta.get("created_at", 0))
                updated_sec = int(meta.get("updated_at", meta.get("created_at", 0)))
                reply_cnt = self._reply_count_visible(root, comments_map, cid, request.include_deleted)
                items.append(
                    (
                        cid,
                        ytcomments_pb2.Comment(
                            id=str(cid),
                            video_id=request.video_id,
                            parent_id="",
                            content_raw=txt,
                            content_html=txt,
                            is_deleted=False,
                            edited=bool(meta.get("edited", False)),
                            created_at=created_sec,
                            updated_at=updated_sec,
                            user_uid=str(meta.get("author_uid", "")),
                            username=str(meta.get("author_name", "")),
                            channel_id=str(meta.get("channel_id", "")) if meta.get("channel_id") else "",
                            reply_count=reply_cnt,
                        ),
                        meta,
                    )
                )

            newest_first = (request.sort == ytcomments_pb2.SortOrder.NEWEST_FIRST)
            items.sort(key=lambda t: self._sort_key(t[2]), reverse=newest_first)

            page_size = max(1, int(request.page_size))
            items = items[:page_size]
            pb_items = [t[1] for t in items]

            total_visible = sum(1 for cid in root_ids if self._visible(comments_map.get(cid) or {}, request.include_deleted))
            print(f"[srv] ListTop: items={len(pb_items)} total_visible_roots={total_visible}")

            return ytcomments_pb2.ListTopResponse(items=pb_items, next_page_token="", total_count=total_visible)
        except PyMongoError as e:
            print(f"[srv] ListTop failed (PyMongoError): {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"Database error: {str(e)}")
            return ytcomments_pb2.ListTopResponse()
        except Exception as e:
            print(f"[srv] ListTop unexpected error: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return ytcomments_pb2.ListTopResponse()

    async def ListReplies(self, request, context):
        print(f"[srv] ListReplies: req parent_id={request.parent_id} page_size={request.page_size} include_deleted={request.include_deleted} sort={request.sort} page_token={request.page_token!r}")
        try:
            root = self._legacy_root_by_child(request.parent_id)
            if not root:
                print("[srv] ListReplies: legacy root by parent not found, return empty")
                return ytcomments_pb2.ListRepliesResponse(items=[], next_page_token="", total_count=0)

            comments_map = root.get("comments", {}) or {}
            child_ids = self._legacy_children_ids(root, request.parent_id)
            print(f"[srv] ListReplies: legacy children for {request.parent_id}: {len(child_ids)}")

            pairs = []
            for cid in child_ids:
                meta = comments_map.get(cid) or {}
                if not isinstance(meta, dict):
                    continue
                if not self._visible(meta, request.include_deleted):
                    continue
                cref = meta.get("chunk_ref") or {}
                txt = self._legacy_text(cref.get("chunk_id", ""), cref.get("local_id", ""))
                created_sec = int(meta.get("created_at", 0))
                updated_sec = int(meta.get("updated_at", meta.get("created_at", 0)))
                pb = ytcomments_pb2.Comment(
                    id=str(cid),
                    video_id=str(root.get("video_id") or ""),
                    parent_id=str(request.parent_id),
                    content_raw=txt,
                    content_html=txt,
                    is_deleted=False,
                    edited=bool(meta.get("edited", False)),
                    created_at=created_sec,
                    updated_at=updated_sec,
                    user_uid=str(meta.get("author_uid", "")),
                    username=str(meta.get("author_name", "")),
                    channel_id=str(meta.get("channel_id", "")) if meta.get("channel_id") else "",
                    reply_count=self._reply_count_visible(root, comments_map, cid, request.include_deleted),
                )
                pairs.append((cid, meta, pb))

            oldest_first = (request.sort == ytcomments_pb2.SortOrder.OLDEST_FIRST)
            pairs.sort(key=lambda t: self._sort_key(t[1]), reverse=not oldest_first)

            page_size = max(1, int(request.page_size))
            pairs = pairs[:page_size]
            pb_items = [t[2] for t in pairs]

            total_visible = sum(1 for cid in child_ids if self._visible(comments_map.get(cid) or {}, request.include_deleted))
            print(f"[srv] ListReplies: items={len(pb_items)} total_visible={total_visible}")

            return ytcomments_pb2.ListRepliesResponse(items=pb_items, next_page_token="", total_count=total_visible)
        except PyMongoError as e:
            print(f"[srv] ListReplies failed (PyMongoError): {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"Database error: {str(e)}")
            return ytcomments_pb2.ListRepliesResponse()
        except Exception as e:
            print(f"[srv] ListReplies unexpected error: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return ytcomments_pb2.ListRepliesResponse()

    async def Create(self, request, context):
        print(f"[srv] Create: req video_id={request.video_id} parent_id={request.parent_id!r} user_uid={getattr(request.ctx, 'user_uid', '')}")
        try:
            now = int(time.time())

            new_comment = {
                "video_id": request.video_id,
                "user_uid": getattr(request.ctx, "user_uid", ""),
                "username": getattr(request.ctx, "username", ""),
                "channel_id": getattr(request.ctx, "channel_id", ""),
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

            result = self.db.comments.insert_one(new_comment)

            if request.parent_id:
                self.db.comments.update_one({"_id": ObjectId(request.parent_id)}, {"$inc": {"reply_count": 1}})

            print(f"[srv] Create: inserted_id={str(result.inserted_id)}")
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
                    user_uid=getattr(request.ctx, "user_uid", ""),
                    username=getattr(request.ctx, "username", ""),
                    channel_id=getattr(request.ctx, "channel_id", ""),
                    reply_count=0,
                )
            )

        except PyMongoError as e:
            print(f"[srv] Create failed (PyMongoError): {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"Database error: {str(e)}")
            return ytcomments_pb2.CreateCommentResponse()
        except Exception as e:
            print(f"[srv] Create unexpected error: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return ytcomments_pb2.CreateCommentResponse()

    async def Edit(self, request, context):
        print(f"[srv] Edit: req comment_id={request.comment_id}")
        try:
            comment = self.db.comments.find_one({"_id": ObjectId(request.comment_id)})
            if not comment:
                print("[srv] Edit: not found")
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details("Comment not found.")
                return ytcomments_pb2.EditCommentResponse()

            if comment.get("is_deleted", False):
                print("[srv] Edit: comment is deleted")
                context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
                context.set_details("Cannot edit a deleted comment.")
                return ytcomments_pb2.EditCommentResponse()

            updated_comment = {
                "content_raw": request.content_raw,
                "content_html": comment.get("content_html", ""),
                "edited": True,
                "updated_at": int(time.time()),
            }

            result = self.db.comments.update_one({"_id": ObjectId(request.comment_id)}, {"$set": updated_comment})
            if result.modified_count == 0:
                print("[srv] Edit: modified_count=0")
                context.set_code(grpc.StatusCode.UNKNOWN)
                context.set_details("Failed to update the comment.")
                return ytcomments_pb2.EditCommentResponse()

            updated_comment_from_db = self.db.comments.find_one({"_id": ObjectId(request.comment_id)})
            print(f"[srv] Edit: updated_id={request.comment_id}")
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
                    username=updated_comment_from_db.get("username", ""),
                    channel_id=updated_comment_from_db.get("channel_id", ""),
                    reply_count=updated_comment_from_db.get("reply_count", 0),
                )
            )
        except PyMongoError as e:
            print(f"[srv] Edit failed (PyMongoError): {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"Database error: {str(e)}")
            return ytcomments_pb2.EditCommentResponse()
        except Exception as e:
            print(f"[srv] Edit unexpected error: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return ytcomments_pb2.EditCommentResponse()

    async def Delete(self, request, context):
        print(f"[srv] Delete: req comment_id={request.comment_id} hard_delete={request.hard_delete}")
        try:
            comment = self.db.comments.find_one({"_id": ObjectId(request.comment_id)})
            if not comment:
                print("[srv] Delete: not found")
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details("Comment not found.")
                return ytcomments_pb2.DeleteCommentResponse()

            if request.hard_delete:
                result = self.db.comments.delete_one({"_id": ObjectId(request.comment_id)})
                if result.deleted_count == 0:
                    print("[srv] Delete: hard delete failed")
                    context.set_code(grpc.StatusCode.UNKNOWN)
                    context.set_details("Failed to hard delete the comment.")
                    return ytcomments_pb2.DeleteCommentResponse()
            else:
                update_result = self.db.comments.update_one(
                    {"_id": ObjectId(request.comment_id)},
                    {
                        "$set": {
                            "is_deleted": True,
                            "deleted_at": int(time.time()),
                            "deleted_by": getattr(request.ctx, "user_uid", ""),
                        }
                    }
                )
                if update_result.modified_count == 0:
                    print("[srv] Delete: soft delete failed (modified_count=0)")
                    context.set_code(grpc.StatusCode.UNKNOWN)
                    context.set_details("Failed to soft delete the comment.")
                    return ytcomments_pb2.DeleteCommentResponse()

            deleted_comment = self.db.comments.find_one({"_id": ObjectId(request.comment_id)})
            print(f"[srv] Delete: done id={request.comment_id}")
            return ytcomments_pb2.DeleteCommentResponse(
                comment=ytcomments_pb2.Comment(
                    id=str(deleted_comment["_id"]),
                    video_id=deleted_comment["video_id"],
                    parent_id=deleted_comment.get("parent_id", ""),
                    content_raw=deleted_comment.get("content_raw", ""),
                    content_html=deleted_comment.get("content_html", ""),
                    is_deleted=deleted_comment.get("is_deleted", False),
                    edited=deleted_comment.get("edited", False),
                    created_at=deleted_comment.get("created_at", 0),
                    updated_at=deleted_comment.get("updated_at", 0),
                    user_uid=deleted_comment.get("user_uid", ""),
                    username=deleted_comment.get("username", ""),
                    channel_id=deleted_comment.get("channel_id", ""),
                    reply_count=deleted_comment.get("reply_count", 0),
                )
            )
        except PyMongoError as e:
            print(f"[srv] Delete failed (PyMongoError): {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"Database error: {str(e)}")
            return ytcomments_pb2.DeleteCommentResponse()
        except Exception as e:
            print(f"[srv] Delete unexpected error: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return ytcomments_pb2.DeleteCommentResponse()

    async def Restore(self, request, context):
        print(f"[srv] Restore: req comment_id={request.comment_id}")
        try:
            comment = self.db.comments.find_one({"_id": ObjectId(request.comment_id)})
            if not comment:
                print("[srv] Restore: not found")
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details("Comment not found.")
                return ytcomments_pb2.RestoreCommentResponse()

            if not comment.get("is_deleted", False):
                print("[srv] Restore: comment is not deleted")
                context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
                context.set_details("Comment is not deleted.")
                return ytcomments_pb2.RestoreCommentResponse()

            update_result = self.db.comments.update_one(
                {"_id": ObjectId(request.comment_id)},
                {"$set": {"is_deleted": False, "deleted_at": None, "deleted_by": None}}
            )
            if update_result.modified_count == 0:
                print("[srv] Restore: modified_count=0")
                context.set_code(grpc.StatusCode.UNKNOWN)
                context.set_details("Failed to restore the comment.")
                return ytcomments_pb2.RestoreCommentResponse()

            restored_comment = self.db.comments.find_one({"_id": ObjectId(request.comment_id)})
            print(f"[srv] Restore: done id={request.comment_id}")
            return ytcomments_pb2.RestoreCommentResponse(
                comment=ytcomments_pb2.Comment(
                    id=str(restored_comment["_id"]),
                    video_id=restored_comment["video_id"],
                    parent_id=restored_comment.get("parent_id", ""),
                    content_raw=restored_comment.get("content_raw", ""),
                    content_html=restored_comment.get("content_html", ""),
                    is_deleted=restored_comment.get("is_deleted", False),
                    edited=restored_comment.get("edited", False),
                    created_at=restored_comment.get("created_at", 0),
                    updated_at=restored_comment.get("updated_at", 0),
                    user_uid=restored_comment.get("user_uid", ""),
                    username=restored_comment.get("username", ""),
                    channel_id=restored_comment.get("channel_id", ""),
                    reply_count=restored_comment.get("reply_count", 0),
                )
            )
        except PyMongoError as e:
            print(f"[srv] Restore failed (PyMongoError): {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"Database error: {str(e)}")
            return ytcomments_pb2.RestoreCommentResponse()
        except Exception as e:
            print(f"[srv] Restore unexpected error: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return ytcomments_pb2.RestoreCommentResponse()

    async def GetCounts(self, request, context):
        print(f"[srv] GetCounts: req video_id={request.video_id} is_moderator={getattr(request.ctx, 'is_moderator', False)}")
        try:
            root = self._legacy_root(request.video_id)
            if root:
                comments_map = root.get("comments", {}) or {}
                roots = self._legacy_roots_ids(root)
                def _vis(m: dict) -> bool:
                    if request.ctx and getattr(request.ctx, "is_moderator", False):
                        return True
                    return self._visible(m, False)

                top_level = [cid for cid in roots if _vis(comments_map.get(cid) or {})]
                all_visible = [cid for cid, m in comments_map.items() if _vis(m)]
                print(f"[srv] GetCounts (legacy): top_level={len(top_level)} total={len(all_visible)}")
                return ytcomments_pb2.GetCountsResponse(
                    top_level_count=len(top_level),
                    total_count=len(all_visible)
                )

            print("[srv] GetCounts: legacy root not found")
            return ytcomments_pb2.GetCountsResponse(top_level_count=0, total_count=0)
        except PyMongoError as e:
            print(f"[srv] GetCounts failed (PyMongoError): {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"Database error: {str(e)}")
            return ytcomments_pb2.GetCountsResponse()
        except Exception as e:
            print(f"[srv] GetCounts unexpected error: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return ytcomments_pb2.GetCountsResponse()