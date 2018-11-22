import requests
from SPARQLWrapper import SPARQLWrapper, N3
from rdflib import Graph

from plenum.server.plugin.graphchain.graph_store import GraphStore
from plenum.server.plugin.graphchain.logger import get_debug_logger

logger = get_debug_logger()


class NeptuneGraphStore(GraphStore):

    def __init__(self, ts_name, ts_url):
        super(NeptuneGraphStore, self).__init__(ts_name, ts_url)

        msg = "Created a new NeptuneGraphStore with URL equal to '{}'." \
            .format(ts_url)
        logger.info(msg)

    def check_whether_db_exists(self):
        logger.debug("Checking whether a triple store with db '{}' exists...".format(self._ts_url))

        url = self._ts_url + "?query=SELECT%20%3Fs%20WHERE%20%7B%20%3Fs%20%3Fs%20%3Fs%20.%20%7D"
        r = requests.get(url)
        status_code = r.status_code
        logger.debug("Status type of response whether db exists: {}.".format(status_code))

        return status_code == 200

    def add_graph(self, raw_graph, graph_format, graph_hash):
        logger.debug("Adding graph to the triple store...")

        ihash = GraphStore.IHASH_PREFIX.format(graph_hash)

        g = Graph()
        g.parse(data=raw_graph, format=graph_format)

        sparql_query = SPARQLWrapper(self._ts_url, self._ts_url)

        query = GraphStore.INSERT_GRAPH_QUERY_TEMPLATE.format(ihash, g.serialize(format='nt').decode())
        sparql_query.setQuery(query)
        sparql_query.setMethod('POST')
        sparql_query.query()

