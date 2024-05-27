import json
import requests
from vearch.exception import VearchException
import logging
from typing import List
from vearch.const import CODE_SUCCESS

logger = logging.getLogger("vearch")


class ResultStatus:
    success = "success"
    failed = "failed"


class Result(object):
    def __init__(self, code: str = "", msg: str = "", data: str = ""):
        self.code = code
        self.msg = msg
        self.data = data

    def dict_str(self):
        ret = {"code": self.code, "msg": self.msg, "data": self.data}
        ret_str = json.dumps(ret)
        return ret_str


class UpsertResult(object):
    def __init__(self, code: int = 0, msg: str = "", total: int = 0):
        self.code = code
        self.msg = msg
        self.total = total
        self.document_ids = []

    @classmethod
    def parse_upsert_result_from_response(cls, resp: requests.Response):
        """
       response content like:
       {
            "code":0,
            "msg":"success",
            "data": {
                "total":5,
                "document_ids":[
                    {"_id":"-7406650708070185766","status":200,"error":"success"},
                    {"status":200,"error":"success","_id":"-1644104496683872820"},
                    {"_id":"-509921751725925904","status":200,"error":"success"},
                    {"status":200,"error":"success","_id":"6142641378725051944"},
                    {"_id":"-2560796653511183804","status":200,"error":"success"}]
                }
            }
        }

        :param resp:
        :return:
        """
        ret = json.loads(resp.text)
        code = ret.get("code", -1)
        msg = ret.get("msg", "")
        data = ret.get("data", None)
        total = -1
        document_ids = None
        if data != None:
            total = data.get("total", -1)
            document_ids = data.get("document_ids", [])
        ur = cls(code, msg, total)
        ur.document_ids = document_ids
        return ur

    def get_document_ids(self) -> List:
        ids = []
        for document in self.document_ids:
            id = document.get("_id")
            ids.append(id)
        return ids


class SearchResult(object):
    def __init__(self, code: int = 0, msg: str = "", documents=[]):
        self.code = code
        self.msg = msg
        self.documents = documents

    @classmethod
    def parse_search_result_from_response(cls, resp: requests.Response):
        ret = json.loads(resp.text)
        code = ret.get("code", -1)
        msg = ret.get("msg", "")
        data = ret.get("data", None)
        documents = None
        if data != None:
            documents = data.get("documents", None)
        sr = cls(code, msg, documents=documents)
        return sr


class DeleteResult(object):
    def __init__(self, code: int = 0, msg: str = "", total: int = 0):
        self.code = code
        self.msg = msg
        self.total = total
        self.document_ids = []

    @classmethod
    def parse_delete_result_from_response(cls, resp: requests.Response):
        """
       response content like:
       {
            "code":0,
            "msg":"success",
            "data": {
                "total":5,
                "document_ids":["-7406650708070185766","-1644104496683872820""-509921751725925904"]
            }
        }

        :param resp:
        :return:
        """
        ret = json.loads(resp.text)
        code = ret.get("code", -1)
        msg = ret.get("msg", "")
        data = ret.get("data", None)
        total = -1
        document_ids = []
        if data != None:
            total = data.get("total", -1)
            document_ids = data.get("document_ids", [])
        dr = cls(code, msg, total)
        dr.document_ids = document_ids
        return dr


def get_result(resp: requests.Response) -> Result:
    r = Result()
    ret = json.loads(resp.text)
    r.code = ret.get("code", -1)
    r.data = ret.get("data", "")
    r.msg = ret.get("msg", "")
    return r
