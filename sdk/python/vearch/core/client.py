import requests
from requests.adapters import HTTPAdapter
from vearch.utils import singleton, compute_sign_auth
from vearch.config import Config, DefaultConfig
from vearch.const import DATABASE_URI, LIST_DATABASE_URI, AUTH_KEY, CODE_SUCCESS, LIST_SPACE_URI
from vearch.result import Result, get_result
import logging

logger = logging.getLogger("vearch")


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
        logger.debug("curl -X POST " + url + " -H Authorization:%s" % self.token)
        sign = compute_sign_auth(secret=self.token)
        resp = requests.request(method="POST", url=url, auth=sign)
        if resp.status_code != 200:
            logger.error("resp:" + str(resp.text))
        else:
            logger.debug("resp:" + str(resp.text))
        return get_result(resp)

    def _drop_db(self, database_name: str) -> Result:
        url_params = {"database_name": database_name}
        url = self.host + (DATABASE_URI % url_params)
        logger.debug(url)
        sign = compute_sign_auth(secret=self.token)
        resp = requests.request(method="DELETE", url=url,auth=sign)
        return get_result(resp)

    def _list_db(self) -> Result:
        url = self.host + LIST_DATABASE_URI
        sign = compute_sign_auth(secret=self.token)
        resp = requests.request(method="GET", url=url,auth=sign)
        return get_result(resp)

    
    def _list_space(self,database_name: str) -> Result:
        url_params = {"database_name": database_name}
        url = self.host + (LIST_SPACE_URI % url_params)
        sign = compute_sign_auth(secret=self.token)
        resp = requests.request(method="GET", url=url,auth=sign)
        return get_result(resp)

client = RestClient()
