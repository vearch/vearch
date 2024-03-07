import requests
from requests.adapters import HTTPAdapter
from vearch.utils import singleton
from vearch.config import Config, DefaultConfig
from vearch.core.const import DATABASE_URI, LIST_DATABASE_URI
from vearch.core.result import Result, get_result


@singleton
class RestClient(object):
    def __init__(self):
        httpAdapter = HTTPAdapter(pool_maxsize=DefaultConfig.max_connections, max_retries=DefaultConfig.max_retries)
        s = requests.Session()
        s.mount("http://", adapter=httpAdapter)
        self.s = s
        self.host = DefaultConfig.host
        self.token = DefaultConfig.token
        self.timeout = 30

    def config(self, config: Config):
        httpAdapter = HTTPAdapter(pool_maxsize=config.max_connections, max_retries=config.max_retries)
        s = requests.Session()
        s.mount("http://", adapter=httpAdapter)
        self.s = s
        self.host = config.host
        self.token = config.token
        self.timeout = config.timeout

    def _create_db(self, database_name) -> Result:
        url_params = {"database_name": database_name}
        url = self.host + DATABASE_URI % url_params
        req = requests.request(method="POST", url=url, headers={"token": self.token})
        resp = self.s.send(req)
        return get_result(resp)

    def _drop_db(self, database_name) -> Result:
        url_params = {"database_name": database_name}
        url = self.host + DATABASE_URI % url_params
        req = requests.request(method="DELETE", url=url, headers={"token": self.token})
        resp = self.s.send(req)
        return get_result(resp)

    def _list_db(self) -> Result:
        url = self.host + LIST_DATABASE_URI
        req = requests.request(method="GET", url=url, headers={"token": self.token})
        resp = self.s.send(req)
        return get_result(resp)


client = RestClient()
