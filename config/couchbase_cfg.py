import os
from dataclasses import dataclass


@dataclass(frozen=True)
class CouchbaseCfg:
    connstr: str = os.getenv("CB_CONNSTR", "couchbase://127.0.0.1")
    username: str = os.getenv("CB_USERNAME", "Administrator")
    password: str = os.getenv("CB_PASSWORD", "password")
    bucket: str = os.getenv("CB_BUCKET", "ytcomments")
    scope: str = os.getenv("CB_SCOPE", "_default")
    collection: str = os.getenv("CB_COLLECTION", "_default")
    kv_timeout_sec: float = float(os.getenv("CB_KV_TIMEOUT_SEC", "2.5"))


cb_cfg = CouchbaseCfg()