import requests
from SPARQLWrapper import SPARQLWrapper
from rdflib import Graph
from stp_core.common.log import getlogger

logger = getlogger()


IHASH_PREFIX = "http://lei.info/{}"

INSERT_GRAPH_QUERY_TEMPLATE = """
INSERT DATA {{
  GRAPH <{}> {{
    {}
  }}
}}
"""


class GraphStore:
    # TODO: Move endpoint address and credentials to the configuration
    _endpoint_address = "http://192.168.56.101:5820"
    _user = 'node'
    _pass = 'node'

    _create_new_database_json = """
    {{
      'dbname': '{}',
      'options': {{}}
    }}
    """.strip()

    def __init__(self, node_name: str):
        logger.info("Creating a new GraphStore with the node name '{}'."
                    .format(node_name))

        self.node_name = node_name
        self.node_endpoint = GraphStore._endpoint_address + "/" + node_name

    def check_whether_db_exists(self):
        logger.info("Checking whether db exists for the '{}' node..."
                    .format(self.node_name))

        url = self._get_node_endpoint_address()
        r = requests.get(url, auth=(self._user, self._pass))
        status_code = r.status_code
        logger.info("Status type of response whether db exists: {}."
                    .format(status_code))

        return status_code == 200

    def create_db(self):
        logger.info("Creating db for the '{}' node...".format(self.node_name))

        url = self._get_endpoint_address() + "/admin/databases"
        request_json = self._create_new_database_json.format(self.node_name)
        post_body = [
            ("root", (None, request_json, None)),
        ]
        r = requests.post(url, files=post_body, auth=(self._user, self._pass))

        success = r.status_code == 201
        if success:
            reasons = None
        else:
            reasons = r.text

        return success, reasons

    def add_lei(self, raw_graph, graph_format, graph_hash):
        ihash = IHASH_PREFIX.format(graph_hash)

        g = Graph()
        g.parse(data=raw_graph, format=graph_format)

        sparql_query = SPARQLWrapper(
            self._get_sparql_endpoint_for_query(),
            self._get_sparql_endpoint_for_update())

        query = INSERT_GRAPH_QUERY_TEMPLATE\
            .format(ihash, g.serialize(format='nt').decode())
        sparql_query.setQuery(query)
        sparql_query.method = 'POST'
        sparql_query.setCredentials(self._user, self._pass)
        sparql_query.query()

    def _get_endpoint_address(self):
        return self._endpoint_address

    def _get_node_endpoint_address(self):
        return self.node_endpoint

    def _get_sparql_endpoint_for_query(self):
        return self._get_endpoint_address() + "/" + self.node_name + "/query"

    def _get_sparql_endpoint_for_update(self):
        return self._get_endpoint_address() + "/" + self.node_name + "/update"

