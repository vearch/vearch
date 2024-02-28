import requests
from requests.adapters import HTTPAdapter
from vearch.utils import singleton
from vearch.config import Config, DefaultConfig


@singleton
class RestClient(object):
    def __init__(self):
        httpAdapter = HTTPAdapter(pool_maxsize=DefaultConfig.max_connections, max_retries=DefaultConfig.max_retries)
        s = requests.Session()
        s.mount("http://", adapter=httpAdapter)
        self.s = s
        self.host = DefaultConfig.host
        self.timeout = 30

    def config(self, config: Config):
        httpAdapter = HTTPAdapter(pool_maxsize=config.max_connections, max_retries=config.max_retries)
        s = requests.Session()
        s.mount("http://", adapter=httpAdapter)
        self.s = s
        self.host = config.host
        self.timeout = config.timeout

    def _create_db(self, database_name) -> None:
        url_params = {"db_name": database_name}
        url = self.host + "/%(db_name)s" % url_params
        self.s.post(url)

    def _drop_db(self, database_name) -> None:
        url_params = {"db_name": database_name}
        url = self.host + "/%(db_name)s" % url_params
        self.s.delete(url)


client = RestClient()
