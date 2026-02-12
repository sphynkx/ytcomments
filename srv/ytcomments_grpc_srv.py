from __future__ import annotations

import logging
import uuid

import grpc

from db.couchbase_db import (
    create_comment,
    delete_comment,
    edit_comment,
    get_counts,
    list_replies,
    list_top,
    restore_comment,
)
from proto import ytcomments_pb2 as pb
from proto import ytcomments_pb2_grpc as pbg

log = logging.getLogger("ytcomments_srv")


def _pb_from_doc(d: dict) -> pb.Comment:
    return pb.Comment(
        id=d.get("id", ""),
        video_id=d.get("video_id", ""),
        parent_id=d.get("parent_id", "") or "",
        content_raw=d.get("content_raw", "") or "",
        content_html=d.get("content_html", "") or "",
        is_deleted=bool(d.get("is_deleted", False)),
        edited=bool(d.get("edited", False)),
        created_at=int(d.get("created_at", 0) or 0),
        updated_at=int(d.get("updated_at", 0) or 0),
        user_uid=d.get("user_uid", "") or "",
        username=d.get("username", "") or "",
        channel_id=d.get("channel_id", "") or "",
        reply_count=int(d.get("reply_count", 0) or 0),
    )


def _page_size(req) -> int:
    v = int(req.page_size or 0)
    if v <= 0:
        v = 50
    return min(v, 200)


def _newest_first(sort: int) -> bool:
    # NEWEST_FIRST=1, OLDEST_FIRST=2, 0 unspecified
    return sort == pb.NEWEST_FIRST


class YtCommentsServicer(pbg.YtCommentsServicer):
    def ListTop(self, request: pb.ListTopRequest, context: grpc.ServicerContext) -> pb.ListTopResponse:
        video_id = (request.video_id or "").strip()
        if not video_id:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "video_id is required")

        items, next_token, total = list_top(
            video_id=video_id,
            page_size=_page_size(request),
            page_token=(request.page_token or ""),
            newest_first=_newest_first(request.sort),
            include_deleted=bool(request.include_deleted),
        )
        return pb.ListTopResponse(
            items=[_pb_from_doc(x) for x in items],
            next_page_token=next_token,
            total_count=int(total),
        )

    def ListReplies(self, request: pb.ListRepliesRequest, context: grpc.ServicerContext) -> pb.ListRepliesResponse:
        video_id = (request.video_id or "").strip()
        if not video_id:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "video_id is required")

        parent_id = (request.parent_id or "").strip()
        if not parent_id:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "parent_id is required")

        items, next_token, total = list_replies(
            video_id=video_id,
            parent_id=parent_id,
            page_size=_page_size(request),
            page_token=(request.page_token or ""),
            newest_first=_newest_first(request.sort),
            include_deleted=bool(request.include_deleted),
        )
        return pb.ListRepliesResponse(
            items=[_pb_from_doc(x) for x in items],
            next_page_token=next_token,
            total_count=int(total),
        )

    def Create(self, request: pb.CreateCommentRequest, context: grpc.ServicerContext) -> pb.CreateCommentResponse:
        video_id = (request.video_id or "").strip()
        if not video_id:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "video_id is required")

        parent_id = (request.parent_id or "").strip()
        content_raw = (request.content_raw or "").strip()
        if not content_raw:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "content_raw is required")

        user_uid = (request.ctx.user_uid if request.ctx else "") or ""
        username = (request.ctx.username if request.ctx else "") or ""
        channel_id = (request.ctx.channel_id if request.ctx else "") or ""

        cid = uuid.uuid4().hex

        try:
            doc = create_comment(
                video_id=video_id,
                parent_id=parent_id,
                comment_id=cid,
                content_raw=content_raw,
                user_uid=user_uid,
                username=username,
                channel_id=channel_id,
            )
        except KeyError:
            context.abort(grpc.StatusCode.NOT_FOUND, "parent comment not found")

        return pb.CreateCommentResponse(comment=_pb_from_doc(doc))

    def Edit(self, request: pb.EditCommentRequest, context: grpc.ServicerContext) -> pb.EditCommentResponse:
        video_id = (request.video_id or "").strip()
        if not video_id:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "video_id is required")

        comment_id = (request.comment_id or "").strip()
        if not comment_id:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "comment_id is required")

        doc = edit_comment(video_id, comment_id, (request.content_raw or ""))
        return pb.EditCommentResponse(comment=_pb_from_doc(doc))

    def Delete(self, request: pb.DeleteCommentRequest, context: grpc.ServicerContext) -> pb.DeleteCommentResponse:
        video_id = (request.video_id or "").strip()
        if not video_id:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "video_id is required")

        comment_id = (request.comment_id or "").strip()
        if not comment_id:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "comment_id is required")

        doc = delete_comment(video_id, comment_id, hard_delete=bool(request.hard_delete))
        return pb.DeleteCommentResponse(comment=_pb_from_doc(doc))

    def Restore(self, request: pb.RestoreCommentRequest, context: grpc.ServicerContext) -> pb.RestoreCommentResponse:
        video_id = (request.video_id or "").strip()
        if not video_id:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "video_id is required")

        comment_id = (request.comment_id or "").strip()
        if not comment_id:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "comment_id is required")

        doc = restore_comment(video_id, comment_id)
        return pb.RestoreCommentResponse(comment=_pb_from_doc(doc))

    def GetCounts(self, request: pb.GetCountsRequest, context: grpc.ServicerContext) -> pb.GetCountsResponse:
        video_id = (request.video_id or "").strip()
        if not video_id:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "video_id is required")

        top, total = get_counts(video_id)
        return pb.GetCountsResponse(top_level_count=int(top), total_count=int(total))