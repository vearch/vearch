import json
import requests


class ResultStatus:
    success = "success"
    failed = "failed"


class Result(object):
    def __init__(self, code: str = "", err_msg: str = "", content: str = ""):
        self.code = code
        self.err_msg = err_msg
        self.content = content

    def dict_str(self):
        ret = {"code": self.code, "err_msg": self.err_msg, "content": self.content}
        ret_str = json.dumps(ret)
        return ret_str


def get_result(resp: requests.Response) -> Result:
    r = Result()
    if resp.status_code / 100 == 2:
        r.code = ResultStatus.success
        r.content = resp.content
        return r
    r.code = ResultStatus.failed
    r.err_msg = resp.content
    return r
