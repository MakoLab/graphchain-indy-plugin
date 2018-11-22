from abc import ABC, abstractmethod
from enum import IntEnum

from stp_core.common.log import getlogger

from plenum.server.plugin.graphchain.constants import STARDOG, NEPTUNE

logger = getlogger()

HANDLED_TS_TYPES = [
    STARDOG,
    NEPTUNE
]


def check_whether_ts_type_is_supported(ts_type: str):
    return ts_type in HANDLED_TS_TYPES


class GraphStoreType(IntEnum):
    STARDOG = 1
    NEPTUNE = 2


class GraphStore(ABC):
    IHASH_PREFIX = "http://lei.info/{}"

    INSERT_GRAPH_QUERY_TEMPLATE = """
    INSERT DATA {{
        GRAPH <{}> {{
            {}
        }}
    }}
    """

    def __init__(self, ts_db_name, ts_url):
        msg = "Creating a new GraphStore with the triple store URL '{}' the database name '{}'." \
            .format(ts_db_name, ts_url)
        logger.info(msg)

        self._ts_db_name = ts_db_name
        self._ts_url = ts_url
        self._node_ts_url = ts_url + "/" + ts_db_name

    @abstractmethod
    def check_whether_db_exists(self):
        pass

    @abstractmethod
    def add_graph(self, raw_graph, graph_format, graph_hash):
        pass

    def _get_endpoint_address(self):
        return self._ts_url

    def _get_ts_db_url(self):
        return self._node_ts_url

    def _get_sparql_endpoint_for_query(self):
        return self._get_ts_db_url() + "/query"

    def _get_sparql_endpoint_for_update(self):
        return self._get_ts_db_url() + "/update"
