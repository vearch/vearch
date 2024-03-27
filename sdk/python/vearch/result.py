import json
import requests
from vearch.exception import VearchException
import logging

logger = logging.getLogger("vearch")


class ResultStatus:
    success = "success"
    failed = "failed"


class Result(object):
    def __init__(self, code: str = "", err_msg: str = "", text: str = ""):
        self.code = code
        self.err_msg = err_msg
        self.text = text

    def dict_str(self):
        ret = {"code": self.code, "err_msg": self.err_msg, "content": self.text}
        ret_str = json.dumps(ret)
        return ret_str


def get_result(resp: requests.Response) -> Result:
    r = Result()
    logger.debug(resp.text)
    ret = json.loads(resp.text)
    r.code = ret.get("code", -1)
    r.text = ret.get("data", "")
    r.err_msg = ret.get("msg", "")
    if resp.status_code / 100 == 2:
        if r.code != 200:
            logger.error("result code:" + str(r.code) + "msg:" + r.err_msg)
            raise VearchException(r.code, r.err_msg)
        return r
    else:
        logger.error("respone status code:" + str(resp.status_code) + "data:" + resp.text)
        if r.code != -1:
            raise VearchException(r.code, r.err_msg)
