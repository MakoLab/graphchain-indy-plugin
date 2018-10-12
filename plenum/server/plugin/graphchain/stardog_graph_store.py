import requests
from SPARQLWrapper import SPARQLWrapper
from rdflib import Graph

from plenum.server.plugin.graphchain.graph_store import GraphStore
from plenum.server.plugin.graphchain.logger import get_debug_logger

logger = get_debug_logger()


class StardogGraphStore(GraphStore):
    def __init__(self, ts_db_name, ts_url, ts_user, ts_pass):
        super(StardogGraphStore, self).__init__(ts_db_name, ts_url, ts_user, ts_pass)

        msg = "Created a new StardogGraphStore with with user equal to '{}' and URL equal to '{}'." \
            .format(ts_user, self._node_ts_url)
        logger.info(msg)

    def check_whether_db_exists(self):
        logger.debug("Checking whether a triple store with db '{}' exists...".format(self._node_ts_url))

        url = self._get_ts_db_url()
        r = requests.get(url, auth=(self._ts_user, self._ts_pass))
        status_code = r.status_code
        logger.debug("Status type of response whether db exists: {}.".format(status_code))

        return status_code == 200

    def add_graph(self, raw_graph, graph_format, graph_hash):
        logger.debug("Adding graph to the triple store...")

        ihash = GraphStore.IHASH_PREFIX.format(graph_hash)

        g = Graph()
        g.parse(data=raw_graph, format=graph_format)

        sparql_query = SPARQLWrapper(
            self._get_sparql_endpoint_for_query(),
            self._get_sparql_endpoint_for_update())

        query = GraphStore.INSERT_GRAPH_QUERY_TEMPLATE.format(ihash, g.serialize(format='nt').decode())
        sparql_query.setQuery(query)
        sparql_query.method = 'POST'
        sparql_query.setCredentials(self._ts_user, self._ts_pass)
        sparql_query.query()
