import os
from dataclasses import dataclass


def _getenv_bool(name: str, default: bool = False) -> bool:
    v = (os.getenv(name, "") or "").strip().lower()
    if not v:
        return bool(default)
    return v in ("1", "true", "yes", "on")


@dataclass(frozen=True)
class AppCfg:
    grpc_host: str = os.getenv("YTCOMMENTS_GRPC_HOST", "0.0.0.0")
    grpc_port: int = int(os.getenv("YTCOMMENTS_GRPC_PORT", "9093"))

    # TLS (disabled for MVP)
    grpc_tls_enabled: bool = _getenv_bool("YTCOMMENTS_GRPC_TLS_ENABLED", False)
    grpc_tls_cert_path: str = os.getenv("YTCOMMENTS_GRPC_TLS_CERT", "").strip()
    grpc_tls_key_path: str = os.getenv("YTCOMMENTS_GRPC_TLS_KEY", "").strip()
    grpc_tls_ca_path: str = os.getenv("YTCOMMENTS_GRPC_TLS_CA", "").strip()  # optional (mTLS / client auth)

    app_name: str = os.getenv("APP_NAME", "ytcomments")
    instance_id: str = os.getenv("INSTANCE_ID", "")
    version: str = os.getenv("APP_VERSION", "dev")
    build_hash: str = os.getenv("BUILD_HASH", "")
    build_time: str = os.getenv("BUILD_TIME", "")


app_cfg = AppCfg()