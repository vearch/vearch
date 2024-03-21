import json
import requests


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
    if resp.status_code / 100 == 2:
        ret=json.loads(resp.text)
        r.code = ret.get("code")
        r.text = ret.get("data","")
        r.err_msg=ret.get("msg","")
        return r
    r.code = ResultStatus.failed
    r.err_msg = resp.text
    return r
