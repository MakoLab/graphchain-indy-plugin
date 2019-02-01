import json

from plenum.common.request import Request
from plenum.common.constants import TXN_TIME, TXN_TYPE
from plenum.common.types import f, OPERATION
import base64
from stp_core.common.log import getlogger

UTF_8 = "utf-8"


logger = getlogger()


def from_base64(encoded_graph):
    return base64.b64decode(encoded_graph)


def to_base64(raw_graph):
    return base64.b64encode(raw_graph)


def bytes_to_str(value: bytes) -> str:
    return value.decode(UTF_8)


def str_to_bytes(value: str) -> bytes:
    return value.encode(UTF_8)


def bytes_to_dict(value: bytes) -> dict:
    return json.loads(value.decode(UTF_8))


def dict_to_bytes(value: dict) -> bytes:
    return json.dumps(value).encode(UTF_8)


def req_to_txn(req: Request, cons_time=None):
    """
    Transform a client request such that it can be stored in the ledger.
    Also this is what will be returned to the client in the reply
    :param req:
    :param cons_time: UTC epoch at which consensus was reached
    :return:
    """

    if isinstance(req, dict):
        if TXN_TYPE in req:
            return req
        data = req
    else:
        data = req.as_dict

    res = {
        f.IDENTIFIER.nm: data.get(f.IDENTIFIER.nm),
        f.REQ_ID.nm: data[f.REQ_ID.nm],
        f.SIG.nm: data.get(f.SIG.nm, None),
        f.SIGS.nm: data.get(f.SIGS.nm, None),
        TXN_TIME: cons_time or data.get(TXN_TIME)
    }
    res.update(data[OPERATION])
    return res
