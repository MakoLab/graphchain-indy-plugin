from plenum.server.client_authn import CoreAuthNr

from plenum.server.plugin.graphchain.graph_req_handler import \
    GraphchainReqHandler


class GraphchainAuthNr(CoreAuthNr):
    write_types = CoreAuthNr.write_types.union(GraphchainReqHandler.write_types)
    query_types = CoreAuthNr.write_types.union(GraphchainReqHandler.query_types)
