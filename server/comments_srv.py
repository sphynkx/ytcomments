from proto import ytcomments_pb2, ytcomments_pb2_grpc
from pymongo.errors import PyMongoError
from bson.objectid import ObjectId
import base64
import json
import time
import logging
import grpc
import random
import string


def _gen_local_id(length: int = 13) -> str:
    alphabet = string.ascii_letters + string.digits
    return "".join(random.choice(alphabet) for _ in range(length))


class YtCommentsService(ytcomments_pb2_grpc.YtCommentsServicer):
    def __init__(self, db):
        self.db = db
        logging.basicConfig(level=logging.DEBUG, format="[%(levelname)s] %(message)s")
        self.logger = logging.getLogger("YtCommentsService")
        ##print("[srv] YtCommentsService initialized")

    # ----- Helpers: legacy root/chunks -----

    def _legacy_root(self, video_id: str):
        try:
            return self.db.video_comments_root.find_one({"video_id": video_id})
        except Exception as e:
            ##print(f"[srv] legacy_root error: {e}")
            return None

    def _legacy_root_by_child(self, parent_id: str):
        try:
            return self.db.video_comments_root.find_one({f"comments.{parent_id}": {"$exists": True}})
        except Exception as e:
            ##print(f"[srv] legacy_root_by_child error: {e}")
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

    def _compute_depth(self, root: dict, comments_map: dict, cid: str) -> int:
        d = 0
        cur = cid
        for _ in range(100):
            meta = comments_map.get(cur) or {}
            pid = meta.get("parent_id")
            if not pid:
                break
            d += 1
            cur = pid
        return d

    # ----- RPCs: read -----

    async def ListTop(self, request, context):
        ##print(f"[srv] ListTop: req video_id={request.video_id} page_size={request.page_size} include_deleted={request.include_deleted} sort={request.sort}")
        try:
            root = self._legacy_root(request.video_id)
            if not root:
                ##print("[srv] ListTop: legacy root not found, returning empty")
                return ytcomments_pb2.ListTopResponse(items=[], next_page_token="", total_count=0)

            comments_map = root.get("comments", {}) or {}
            root_ids = self._legacy_roots_ids(root)
            ##print(f"[srv] ListTop: legacy root found, comments_map={len(comments_map)} roots={len(root_ids)}")

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
            ##print(f"[srv] ListTop: items={len(pb_items)} total_visible_roots={total_visible}")

            return ytcomments_pb2.ListTopResponse(items=pb_items, next_page_token="", total_count=total_visible)
        except PyMongoError as e:
            ##print(f"[srv] ListTop failed (PyMongoError): {e}")
            context.set_code(grpc.StatusCode.UNAVAILABLE)
            context.set_details(f"Database error: {str(e)}")
            return ytcomments_pb2.ListTopResponse()
        except Exception as e:
            ##print(f"[srv] ListTop unexpected error: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return ytcomments_pb2.ListTopResponse()

    async def ListReplies(self, request, context):
        ##print(f"[srv] ListReplies: req parent_id={request.parent_id} page_size={request.page_size} include_deleted={request.include_deleted} sort={request.sort} page_token={request.page_token!r}")
        try:
            root = self._legacy_root_by_child(request.parent_id)
            if not root:
                ##print("[srv] ListReplies: legacy root by parent not found, return empty")
                return ytcomments_pb2.ListRepliesResponse(items=[], next_page_token="", total_count=0)

            comments_map = root.get("comments", {}) or {}
            child_ids = self._legacy_children_ids(root, request.parent_id)
            ##print(f"[srv] ListReplies: legacy children for {request.parent_id}: {len(child_ids)}")

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
            ##print(f"[srv] ListReplies: items={len(pb_items)} total_visible={total_visible}")

            return ytcomments_pb2.ListRepliesResponse(items=pb_items, next_page_token="", total_count=total_visible)
        except PyMongoError as e:
            ##print(f"[srv] ListReplies failed (PyMongoError): {e}")
            context.set_code(grpc.StatusCode.UNAVAILABLE)
            context.set_details(f"Database error: {str(e)}")
            return ytcomments_pb2.ListRepliesResponse()
        except Exception as e:
            ##print(f"[srv] ListReplies unexpected error: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return ytcomments_pb2.ListRepliesResponse()

    async def GetCounts(self, request, context):
        ##print(f"[srv] GetCounts: req video_id={request.video_id} is_moderator={getattr(request.ctx, 'is_moderator', False)}")
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
                ##print(f"[srv] GetCounts (legacy): top_level={len(top_level)} total={len(all_visible)}")
                return ytcomments_pb2.GetCountsResponse(
                    top_level_count=len(top_level),
                    total_count=len(all_visible)
                )

            ##print("[srv] GetCounts: legacy root not found")
            return ytcomments_pb2.GetCountsResponse(top_level_count=0, total_count=0)
        except PyMongoError as e:
            ##print(f"[srv] GetCounts failed (PyMongoError): {e}")
            context.set_code(grpc.StatusCode.UNAVAILABLE)
            context.set_details(f"Database error: {str(e)}")
            return ytcomments_pb2.GetCountsResponse()
        except Exception as e:
            ##print(f"[srv] GetCounts unexpected error: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return ytcomments_pb2.GetCountsResponse()

    # ----- RPCs: write -----

    async def Create(self, request, context):
        ##print(f"[srv] Create: req video_id={request.video_id} parent_id={request.parent_id!r} user_uid={getattr(request.ctx, 'user_uid', '')}")
        try:
            now = int(time.time())
            root = self._legacy_root(request.video_id)
            if not root:
                ##print("[srv] Create: legacy root not found")
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details("Video not found in legacy root.")
                return ytcomments_pb2.CreateCommentResponse()

            comments_map = root.get("comments", {}) or {}
            tree_aux = root.get("tree_aux", {}) or {}
            children_map = tree_aux.get("children_map", {}) or {}
            depth_index = tree_aux.get("depth_index", {}) or {}

            # New IDs
            cid_oid = ObjectId()
            cid = str(cid_oid)
            chunk_oid = ObjectId()
            chunk_id_str = str(chunk_oid)
            local_id = _gen_local_id()

            # Save text chunk
            self.db.video_comments_chunks.insert_one({"_id": chunk_oid, "texts": {local_id: request.content_raw}})

            # Compose meta as in legacy
            meta = {
                "author_uid": getattr(request.ctx, "user_uid", "") or "",
                "author_name": getattr(request.ctx, "username", "") or "",
                "channel_id": getattr(request.ctx, "channel_id", "") or "",
                "created_at": now,
                "edited": False,
                "visible": True,
                "likes": 0,
                "dislikes": 0,
                "votes": {},
                "chunk_ref": {
                    "chunk_id": chunk_id_str,
                    "local_id": local_id
                },
                "parent_id": request.parent_id if request.parent_id else None
            }

            # Write into root.comments.<cid>
            self.db.video_comments_root.update_one(
                {"_id": root["_id"]},
                {"$set": {f"comments.{cid}": meta}}
            )

            # Update tree_aux children_map / depth_index
            if request.parent_id:
                # append to children of parent
                self.db.video_comments_root.update_one(
                    {"_id": root["_id"]},
                    {"$push": {f"tree_aux.children_map.{request.parent_id}": cid}},
                )
                # compute depth and append to depth_index.<d>
                comments_map[cid] = meta  # for depth calculation
                d = self._compute_depth(root, comments_map, cid)
                self.db.video_comments_root.update_one(
                    {"_id": root["_id"]},
                    {"$push": {f"tree_aux.depth_index.{str(d)}": cid}},
                )
            else:
                # root-level: add to depth_index["0"]
                self.db.video_comments_root.update_one(
                    {"_id": root["_id"]},
                    {"$push": {"tree_aux.depth_index.0": cid}},
                )

            ##print(f"[srv] Create: inserted_id={cid}")
            return ytcomments_pb2.CreateCommentResponse(
                comment=ytcomments_pb2.Comment(
                    id=cid,
                    video_id=request.video_id,
                    parent_id=request.parent_id or "",
                    content_raw=request.content_raw,
                    content_html=request.content_raw,
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
            ##print(f"[srv] Create failed (PyMongoError): {e}")
            context.set_code(grpc.StatusCode.UNAVAILABLE)
            context.set_details(f"Database error: {str(e)}")
            return ytcomments_pb2.CreateCommentResponse()
        except Exception as e:
            ##print(f"[srv] Create unexpected error: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return ytcomments_pb2.CreateCommentResponse()

    async def Edit(self, request, context):
        ##print(f"[srv] Edit: req comment_id={request.comment_id}")
        try:
            # find legacy root by comment id
            root = self._legacy_root_by_child(request.comment_id)
            if not root:
                ##print("[srv] Edit: legacy root not found by comment")
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details("Comment not found.")
                return ytcomments_pb2.EditCommentResponse()

            comments_map = root.get("comments", {}) or {}
            meta = comments_map.get(request.comment_id)
            if not meta:
                ##print("[srv] Edit: comment meta missing")
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details("Comment not found.")
                return ytcomments_pb2.EditCommentResponse()

            if not meta.get("visible", True):
                ##print("[srv] Edit: comment is deleted")
                context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
                context.set_details("Cannot edit a deleted comment.")
                return ytcomments_pb2.EditCommentResponse()

            # update text in chunk
            cref = meta.get("chunk_ref") or {}
            chunk_id = cref.get("chunk_id")
            local_id = cref.get("local_id")
            if not chunk_id or not local_id:
                ##print("[srv] Edit: chunk_ref missing")
                context.set_code(grpc.StatusCode.UNKNOWN)
                context.set_details("Chunk reference missing.")
                return ytcomments_pb2.EditCommentResponse()

            try:
                oid = ObjectId(chunk_id)
                self.db.video_comments_chunks.update_one({"_id": oid}, {"$set": {f"texts.{local_id}": request.content_raw}})
            except Exception:
                self.db.video_comments_chunks.update_one({"_id": chunk_id}, {"$set": {f"texts.{local_id}": request.content_raw}})

            # set edited flag and updated_at
            now = int(time.time())
            self.db.video_comments_root.update_one(
                {"_id": root["_id"]},
                {"$set": {f"comments.{request.comment_id}.edited": True, f"comments.{request.comment_id}.updated_at": now}}
            )

            ##print(f"[srv] Edit: updated_id={request.comment_id}")
            updated_meta = self._legacy_root_by_child(request.comment_id).get("comments", {}).get(request.comment_id, {})
            return ytcomments_pb2.EditCommentResponse(
                comment=ytcomments_pb2.Comment(
                    id=str(request.comment_id),
                    video_id=str(root.get("video_id") or ""),
                    parent_id=str(updated_meta.get("parent_id") or ""),
                    content_raw=request.content_raw,
                    content_html=request.content_raw,
                    is_deleted=not bool(updated_meta.get("visible", True)),
                    edited=True,
                    created_at=int(updated_meta.get("created_at", 0)),
                    updated_at=now,
                    user_uid=str(updated_meta.get("author_uid", "")),
                    username=str(updated_meta.get("author_name", "")),
                    channel_id=str(updated_meta.get("channel_id", "")) if updated_meta.get("channel_id") else "",
                    reply_count=0,
                )
            )
        except PyMongoError as e:
            ##print(f"[srv] Edit failed (PyMongoError): {e}")
            context.set_code(grpc.StatusCode.UNAVAILABLE)
            context.set_details(f"Database error: {str(e)}")
            return ytcomments_pb2.EditCommentResponse()
        except Exception as e:
            ##print(f"[srv] Edit unexpected error: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return ytcomments_pb2.EditCommentResponse()

    async def Delete(self, request, context):
        ##print(f"[srv] Delete: req comment_id={request.comment_id} hard_delete={request.hard_delete}")
        try:
            root = self._legacy_root_by_child(request.comment_id)
            if not root:
                ##print("[srv] Delete: legacy root not found by comment")
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details("Comment not found.")
                return ytcomments_pb2.DeleteCommentResponse()

            # soft delete => visible=False, tombstone=True
            now = int(time.time())
            if not request.hard_delete:
                self.db.video_comments_root.update_one(
                    {"_id": root["_id"]},
                    {"$set": {
                        f"comments.{request.comment_id}.visible": False,
                        f"comments.{request.comment_id}.tombstone": True,
                        f"comments.{request.comment_id}.deleted_at": now,
                        f"comments.{request.comment_id}.deleted_by": getattr(request.ctx, "user_uid", ""),
                    }}
                )
            else:
                # hard delete => remove comment from map and from children_map/depth_index
                doc = self._legacy_root_by_child(request.comment_id)
                comments_map = doc.get("comments", {}) or {}
                meta = comments_map.get(request.comment_id) or {}
                parent_id = meta.get("parent_id")
                # unset the comment
                self.db.video_comments_root.update_one(
                    {"_id": root["_id"]},
                    {"$unset": {f"comments.{request.comment_id}": ""}}
                )
                # pull from children_map and depth_index
                if parent_id:
                    self.db.video_comments_root.update_one({"_id": root["_id"]}, {"$pull": {f"tree_aux.children_map.{parent_id}": request.comment_id}})
                # compute depth for depth_index pull
                depth_index = (doc.get("tree_aux") or {}).get("depth_index", {}) or {}
                # naive search: remove from any depth bucket if exists
                for k in list(depth_index.keys()):
                    self.db.video_comments_root.update_one({"_id": root["_id"]}, {"$pull": {f"tree_aux.depth_index.{k}": request.comment_id}})

            # return current meta
            updated_root = self._legacy_root_by_child(request.comment_id)
            updated_meta = (updated_root or {}).get("comments", {}).get(request.comment_id, {})
            ##print(f"[srv] Delete: done id={request.comment_id}")
            return ytcomments_pb2.DeleteCommentResponse(
                comment=ytcomments_pb2.Comment(
                    id=str(request.comment_id),
                    video_id=str((updated_root or root).get("video_id") or ""),
                    parent_id=str(updated_meta.get("parent_id") or ""),
                    content_raw=self._legacy_text(
                        ((updated_meta.get("chunk_ref") or {}).get("chunk_id") or ""),
                        ((updated_meta.get("chunk_ref") or {}).get("local_id") or "")
                    ) if updated_meta else "",
                    content_html=self._legacy_text(
                        ((updated_meta.get("chunk_ref") or {}).get("chunk_id") or ""),
                        ((updated_meta.get("chunk_ref") or {}).get("local_id") or "")
                    ) if updated_meta else "",
                    is_deleted=updated_meta is not None and not bool(updated_meta.get("visible", True)),
                    edited=bool((updated_meta or {}).get("edited", False)),
                    created_at=int((updated_meta or {}).get("created_at", 0)),
                    updated_at=int((updated_meta or {}).get("updated_at", 0)),
                    user_uid=str((updated_meta or {}).get("author_uid", "")),
                    username=str((updated_meta or {}).get("author_name", "")),
                    channel_id=str((updated_meta or {}).get("channel_id", "")) if (updated_meta or {}).get("channel_id") else "",
                    reply_count=0,
                )
            )
        except PyMongoError as e:
            ##print(f"[srv] Delete failed (PyMongoError): {e}")
            context.set_code(grpc.StatusCode.UNAVAILABLE)
            context.set_details(f"Database error: {str(e)}")
            return ytcomments_pb2.DeleteCommentResponse()
        except Exception as e:
            ##print(f"[srv] Delete unexpected error: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return ytcomments_pb2.DeleteCommentResponse()

    async def Restore(self, request, context):
        ##print(f"[srv] Restore: req comment_id={request.comment_id}")
        try:
            root = self._legacy_root_by_child(request.comment_id)
            if not root:
                ##print("[srv] Restore: legacy root not found by comment")
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details("Comment not found.")
                return ytcomments_pb2.RestoreCommentResponse()

            # restore => visible=True, tombstone=False
            self.db.video_comments_root.update_one(
                {"_id": root["_id"]},
                {"$set": {
                    f"comments.{request.comment_id}.visible": True,
                    f"comments.{request.comment_id}.tombstone": False,
                    f"comments.{request.comment_id}.deleted_at": None,
                    f"comments.{request.comment_id}.deleted_by": None,
                }}
            )

            updated_root = self._legacy_root_by_child(request.comment_id)
            updated_meta = (updated_root or {}).get("comments", {}).get(request.comment_id, {})
            ##print(f"[srv] Restore: done id={request.comment_id}")
            return ytcomments_pb2.RestoreCommentResponse(
                comment=ytcomments_pb2.Comment(
                    id=str(request.comment_id),
                    video_id=str((updated_root or root).get("video_id") or ""),
                    parent_id=str(updated_meta.get("parent_id") or ""),
                    content_raw=self._legacy_text(
                        ((updated_meta.get("chunk_ref") or {}).get("chunk_id") or ""),
                        ((updated_meta.get("chunk_ref") or {}).get("local_id") or "")
                    ) if updated_meta else "",
                    content_html=self._legacy_text(
                        ((updated_meta.get("chunk_ref") or {}).get("chunk_id") or ""),
                        ((updated_meta.get("chunk_ref") or {}).get("local_id") or "")
                    ) if updated_meta else "",
                    is_deleted=not bool(updated_meta.get("visible", True)),
                    edited=bool(updated_meta.get("edited", False)),
                    created_at=int(updated_meta.get("created_at", 0)),
                    updated_at=int(updated_meta.get("updated_at", 0)),
                    user_uid=str(updated_meta.get("author_uid", "")),
                    username=str(updated_meta.get("author_name", "")),
                    channel_id=str(updated_meta.get("channel_id", "")) if updated_meta.get("channel_id") else "",
                    reply_count=0,
                )
            )
        except PyMongoError as e:
            ##print(f"[srv] Restore failed (PyMongoError): {e}")
            context.set_code(grpc.StatusCode.UNAVAILABLE)
            context.set_details(f"Database error: {str(e)}")
            return ytcomments_pb2.RestoreCommentResponse()
        except Exception as e:
            ##print(f"[srv] Restore unexpected error: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return ytcomments_pb2.RestoreCommentResponse()