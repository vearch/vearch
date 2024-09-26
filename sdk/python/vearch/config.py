from typing import NamedTuple

DEFAULT_TOKEN = ""
DEFAULT_RETRIES = 3
DEFAULT_MAX_CONNECTIONS = 12
DEFAULT_TIMEOUT = 30


class Config(NamedTuple):
    host: str
    token: str = DEFAULT_TOKEN
    max_retries: int = DEFAULT_RETRIES
    max_connections: int = DEFAULT_MAX_CONNECTIONS
    timeout: int = DEFAULT_TIMEOUT
