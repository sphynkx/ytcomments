from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Optional

from config.couchbase_cfg import cb_cfg

log = logging.getLogger("cb_db")

try:
    from couchbase.cluster import Cluster
    from couchbase.auth import PasswordAuthenticator
    from couchbase.options import ClusterOptions, ClusterTimeoutOptions
    from couchbase.exceptions import CouchbaseException
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
        cluster.wait_until_ready(timedelta(seconds=float(cb_cfg.kv_timeout_sec)))
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


def upsert_json(doc_id: str, value: Any) -> None:
    ctx = connect()
    ctx.coll.upsert(doc_id, value)


def get_json(doc_id: str) -> Optional[Any]:
    ctx = connect()
    try:
        res = ctx.coll.get(doc_id)
        return res.content_as[dict]
    except Exception:
        return None