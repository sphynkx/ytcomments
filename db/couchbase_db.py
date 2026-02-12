from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Optional, Tuple, List

from config.couchbase_cfg import cb_cfg

log = logging.getLogger("cb_db")

try:
    from couchbase.cluster import Cluster
    from couchbase.auth import PasswordAuthenticator
    from couchbase.options import ClusterOptions, ClusterTimeoutOptions
    from couchbase.exceptions import (
        CouchbaseException,
        DocumentNotFoundException,
        CasMismatchException,
    )
except Exception as e:  # pragma: no cover
    raise RuntimeError(
        "Couchbase SDK is not available. "
        "You must run this service on Python 3.10 with couchbase SDK installed."
    ) from e


@dataclass
class CouchbaseCtx:
    cluster: Cluster
    bucket: Any
    scope: Any
    coll: Any


_ctx: Optional[CouchbaseCtx] = None


def _now_ms() -> int:
    return int(time.time() * 1000)


def connect() -> CouchbaseCtx:
    global _ctx
    if _ctx is not None:
        return _ctx

    log.info(
        "cb cfg: connstr=%s user=%s bucket=%s scope=%s collection=%s",
        cb_cfg.connstr,
        cb_cfg.username,
        cb_cfg.bucket,
        cb_cfg.scope,
        cb_cfg.collection,
    )

    auth = PasswordAuthenticator(cb_cfg.username, cb_cfg.password)
    opts = ClusterOptions(
        auth,
        timeout_options=ClusterTimeoutOptions(
            kv_timeout=timedelta(seconds=float(cb_cfg.kv_timeout_sec)),
        ),
    )

    cluster = Cluster(cb_cfg.connstr, opts)

    try:
        cluster.wait_until_ready(timedelta(seconds=max(float(cb_cfg.kv_timeout_sec), 5.0)))
    except Exception as e:
        log.warning("cluster.wait_until_ready failed/unsupported: %s", e)

    bucket = cluster.bucket(cb_cfg.bucket)
    scope = bucket.scope(cb_cfg.scope)
    coll = scope.collection(cb_cfg.collection)

    _ctx = CouchbaseCtx(cluster=cluster, bucket=bucket, scope=scope, coll=coll)
    log.info(
        "connected: connstr=%s bucket=%s scope=%s collection=%s",
        cb_cfg.connstr,
        cb_cfg.bucket,
        cb_cfg.scope,
        cb_cfg.collection,
    )
    return _ctx


def ping() -> bool:
    try:
        ctx = connect()
        ctx.bucket.ping()
        return True
    except CouchbaseException as e:
        log.error("ping failed: %s", e)
        return False


# ---------------------------
# Thread: one doc per video
# ---------------------------

def thread_doc_id(video_id: str) -> str:
    return f"thread::{video_id}"


def _empty_thread(video_id: str) -> dict:
    now = _now_ms()
    return {
        "type": "comment_thread",
        "video_id": video_id,
        "created_at": now,
        "updated_at": now,
        "next_seq": 1,
        "comments": {},          # comment_id -> comment object
        "top_index": [],         # list[comment_id] (creation order)
        "replies_index": {},     # parent_id -> list[comment_id] (creation order)
        "counts": {"total": 0, "top": 0},
    }


def _get_or_create_thread(video_id: str) -> tuple[dict, int]:
    ctx = connect()
    did = thread_doc_id(video_id)
    try:
        res = ctx.coll.get(did)
        return res.content_as[dict], res.cas
    except DocumentNotFoundException:
        ctx.coll.upsert(did, _empty_thread(video_id))
        res = ctx.coll.get(did)
        return res.content_as[dict], res.cas


def _replace_thread(video_id: str, doc: dict, cas: int) -> int:
    ctx = connect()
    did = thread_doc_id(video_id)
    doc["updated_at"] = _now_ms()
    res = ctx.coll.replace(did, doc, cas=cas)
    return res.cas


def _retry_cas(op, retries: int = 30):
    last = None
    for _ in range(retries):
        try:
            return op()
        except CasMismatchException as e:
            last = e
            continue
    raise last or RuntimeError("CAS retry exhausted")


def _parse_offset(page_token: str) -> int:
    if not page_token:
        return 0
    try:
        return max(int(page_token), 0)
    except Exception:
        return 0


def _sorted_ids(ids: list[str], doc: dict, newest_first: bool) -> list[str]:
    # Store ids in create order, also suport reverse for NEWEST_FIRST
    if not newest_first:
        return ids
    return list(reversed(ids))


def create_comment(
    video_id: str,
    parent_id: str,
    comment_id: str,
    content_raw: str,
    user_uid: str,
    username: str,
    channel_id: str,
) -> dict:
    parent_id = parent_id or ""

    def op():
        thread, cas = _get_or_create_thread(video_id)

        if parent_id and parent_id not in thread["comments"]:
            raise KeyError("parent_not_found")

        now = _now_ms()
        seq = int(thread.get("next_seq", 1) or 1)
        thread["next_seq"] = seq + 1

        c = {
            "id": comment_id,
            "video_id": video_id,
            "parent_id": parent_id,
            "content_raw": content_raw or "",
            "content_html": "",
            "is_deleted": False,
            "edited": False,
            "created_at": now,
            "updated_at": now,
            "user_uid": user_uid or "",
            "username": username or "",
            "channel_id": channel_id or "",
            "reply_count": 0,
            "seq": seq,
        }

        thread["comments"][comment_id] = c

        if not parent_id:
            thread["top_index"].append(comment_id)
            thread["counts"]["top"] = int(thread["counts"].get("top", 0) or 0) + 1
        else:
            arr = thread["replies_index"].setdefault(parent_id, [])
            arr.append(comment_id)
            # bump parent reply_count
            thread["comments"][parent_id]["reply_count"] = int(thread["comments"][parent_id].get("reply_count", 0) or 0) + 1

        thread["counts"]["total"] = int(thread["counts"].get("total", 0) or 0) + 1

        _replace_thread(video_id, thread, cas)
        return c

    return _retry_cas(op)


def list_top(video_id: str, page_size: int, page_token: str, newest_first: bool, include_deleted: bool) -> tuple[list[dict], str, int]:
    thread, _ = _get_or_create_thread(video_id)
    ids = _sorted_ids(list(thread.get("top_index", []) or []), thread, newest_first)

    if not include_deleted:
        ids = [cid for cid in ids if not bool(thread["comments"].get(cid, {}).get("is_deleted", False))]

    total = len(ids)
    off = _parse_offset(page_token)
    slice_ids = ids[off: off + page_size]
    items = [thread["comments"][cid] for cid in slice_ids if cid in thread["comments"]]

    next_off = off + len(slice_ids)
    next_token = str(next_off) if next_off < total else ""
    return items, next_token, total


def list_replies(video_id: str, parent_id: str, page_size: int, page_token: str, newest_first: bool, include_deleted: bool) -> tuple[list[dict], str, int]:
    thread, _ = _get_or_create_thread(video_id)
    parent_id = parent_id or ""

    ids = list((thread.get("replies_index", {}) or {}).get(parent_id, []) or [])
    ids = _sorted_ids(ids, thread, newest_first)

    if not include_deleted:
        ids = [cid for cid in ids if not bool(thread["comments"].get(cid, {}).get("is_deleted", False))]

    total = len(ids)
    off = _parse_offset(page_token)
    slice_ids = ids[off: off + page_size]
    items = [thread["comments"][cid] for cid in slice_ids if cid in thread["comments"]]

    next_off = off + len(slice_ids)
    next_token = str(next_off) if next_off < total else ""
    return items, next_token, total


def edit_comment(video_id: str, comment_id: str, content_raw: str) -> dict:
    def op():
        thread, cas = _get_or_create_thread(video_id)
        if comment_id not in thread["comments"]:
            raise KeyError("not_found")
        c = thread["comments"][comment_id]
        c["content_raw"] = content_raw or ""
        c["edited"] = True
        c["updated_at"] = _now_ms()
        _replace_thread(video_id, thread, cas)
        return c

    return _retry_cas(op)


def delete_comment(video_id: str, comment_id: str, hard_delete: bool) -> dict:
    def op():
        thread, cas = _get_or_create_thread(video_id)
        if comment_id not in thread["comments"]:
            raise KeyError("not_found")

        c = thread["comments"][comment_id]
        parent_id = c.get("parent_id", "") or ""

        if hard_delete:
            # remove from indexes
            if not parent_id:
                if comment_id in thread["top_index"]:
                    thread["top_index"].remove(comment_id)
                    thread["counts"]["top"] = max(int(thread["counts"].get("top", 0) or 0) - 1, 0)
            else:
                arr = (thread.get("replies_index", {}) or {}).get(parent_id, [])
                if comment_id in arr:
                    arr.remove(comment_id)
                if parent_id in thread["comments"]:
                    thread["comments"][parent_id]["reply_count"] = max(
                        int(thread["comments"][parent_id].get("reply_count", 0) or 0) - 1,
                        0,
                    )

            del thread["comments"][comment_id]
            thread["counts"]["total"] = max(int(thread["counts"].get("total", 0) or 0) - 1, 0)

            out = {
                "id": comment_id,
                "video_id": video_id,
                "parent_id": parent_id,
                "content_raw": "",
                "content_html": "",
                "is_deleted": True,
                "edited": True,
                "created_at": int(c.get("created_at", 0) or 0),
                "updated_at": _now_ms(),
                "user_uid": c.get("user_uid", "") or "",
                "username": c.get("username", "") or "",
                "channel_id": c.get("channel_id", "") or "",
                "reply_count": int(c.get("reply_count", 0) or 0),
            }
        else:
            c["is_deleted"] = True
            c["content_raw"] = ""
            c["content_html"] = ""
            c["updated_at"] = _now_ms()
            out = c

        _replace_thread(video_id, thread, cas)
        return out

    return _retry_cas(op)


def restore_comment(video_id: str, comment_id: str) -> dict:
    def op():
        thread, cas = _get_or_create_thread(video_id)
        if comment_id not in thread["comments"]:
            raise KeyError("not_found")
        c = thread["comments"][comment_id]
        c["is_deleted"] = False
        c["updated_at"] = _now_ms()
        _replace_thread(video_id, thread, cas)
        return c

    return _retry_cas(op)


def get_counts(video_id: str) -> tuple[int, int]:
    thread, _ = _get_or_create_thread(video_id)
    top = int(thread.get("counts", {}).get("top", 0) or 0)
    total = int(thread.get("counts", {}).get("total", 0) or 0)
    return top, total