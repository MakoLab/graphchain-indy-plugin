from rdflib import Graph


class FormatValidator:

    def __init__(self):
        self._handled_formats = ['n3', 'nquads', 'nt', 'trix', 'turtle', 'xml']

    def validate_format(self, graph_format: str, required: bool = True):
        if (graph_format is None or len(graph_format) == 0) and required:
            return False, "Graph format is empty."

        if required:
            if graph_format in self._handled_formats:
                return True, None
            else:
                return False, "Graph format '{}' not supported.".format(graph_format)
        else:
            return True, None


class GraphValidator:

    def __init__(self):
        pass

    def validate_graph(self, graph: str, graph_format: str):
        # We try to load graph: if there aren't any exceptions, we assume that
        # graph is syntactically valid; if an exception has been raised, we
        # return the False and an reason why parsing failed.
        try:
            g = Graph()
            g.parse(data=graph, format=graph_format)
            return True, None
        except Exception as ex:
            return False, str(ex)
