from __future__ import annotations

import logging
import time
import uuid

import grpc

from db.couchbase_db import upsert_json

from proto import ytcomments_pb2 as pb
from proto import ytcomments_pb2_grpc as pbg

log = logging.getLogger("ytcomments_srv")


def _now_ms() -> int:
    return int(time.time() * 1000)


class YtCommentsServicer(pbg.YtCommentsServicer):
    def ListTop(self, request: pb.ListTopRequest, context: grpc.ServicerContext) -> pb.ListTopResponse:
        return pb.ListTopResponse(items=[], next_page_token="", total_count=0)

    def ListReplies(self, request: pb.ListRepliesRequest, context: grpc.ServicerContext) -> pb.ListRepliesResponse:
        return pb.ListRepliesResponse(items=[], next_page_token="", total_count=0)

    def Create(self, request: pb.CreateCommentRequest, context: grpc.ServicerContext) -> pb.CreateCommentResponse:
        cid = uuid.uuid4().hex
        now = _now_ms()
        c = pb.Comment(
            id=cid,
            video_id=request.video_id,
            parent_id=request.parent_id or "",
            content_raw=request.content_raw or "",
            content_html="",  # HTML will later
            is_deleted=False,
            edited=False,
            created_at=now,
            updated_at=now,
            user_uid=(request.ctx.user_uid if request.ctx else "") or "",
            username=(request.ctx.username if request.ctx else "") or "",
            channel_id=(request.ctx.channel_id if request.ctx else "") or "",
            reply_count=0,
        )

        # Minimal DB write to ensure Couchbase is working
        upsert_json(f"comment::{cid}", {"id": cid, "video_id": request.video_id, "created_at": now})

        return pb.CreateCommentResponse(comment=c)

    def Edit(self, request: pb.EditCommentRequest, context: grpc.ServicerContext) -> pb.EditCommentResponse:
        now = _now_ms()
        c = pb.Comment(
            id=request.comment_id,
            content_raw=request.content_raw or "",
            edited=True,
            updated_at=now,
        )
        upsert_json(f"comment_edit::{request.comment_id}", {"comment_id": request.comment_id, "updated_at": now})
        return pb.EditCommentResponse(comment=c)

    def Delete(self, request: pb.DeleteCommentRequest, context: grpc.ServicerContext) -> pb.DeleteCommentResponse:
        now = _now_ms()
        c = pb.Comment(id=request.comment_id, is_deleted=True, updated_at=now)
        upsert_json(f"comment_delete::{request.comment_id}", {"comment_id": request.comment_id, "hard": bool(request.hard_delete), "updated_at": now})
        return pb.DeleteCommentResponse(comment=c)

    def Restore(self, request: pb.RestoreCommentRequest, context: grpc.ServicerContext) -> pb.RestoreCommentResponse:
        now = _now_ms()
        c = pb.Comment(id=request.comment_id, is_deleted=False, updated_at=now)
        upsert_json(f"comment_restore::{request.comment_id}", {"comment_id": request.comment_id, "updated_at": now})
        return pb.RestoreCommentResponse(comment=c)

    def GetCounts(self, request: pb.GetCountsRequest, context: grpc.ServicerContext) -> pb.GetCountsResponse:
        return pb.GetCountsResponse(top_level_count=0, total_count=0)