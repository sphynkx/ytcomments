import logging
import os


def setup_logging() -> None:
    level = (os.getenv("LOG_LEVEL", "INFO") or "INFO").upper()
    logging.basicConfig(
        level=getattr(logging, level, logging.INFO),
        format="%(asctime)s %(levelname).1s %(name)s: %(message)s",
    )