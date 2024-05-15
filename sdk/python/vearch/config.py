from typing import NamedTuple


class Config(NamedTuple):
    host: str
    token: str = ""
    max_retries: int = 3
    max_connections: int = 12
    timeout: int = 30


# DefaultConfig = Config(host="localhost:9001")
DefaultConfig = Config(host="http://test-api-interface-1-router.vectorbase.svc.sq01.n.jd.local", token="secret")