import time


_started = time.time()


def uptime_sec() -> int:
    return int(time.time() - _started)