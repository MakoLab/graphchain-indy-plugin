from plenum.server.plugin.graphchain.transactions import GraphTransactions

GRAPHCHAIN_LEDGER_ID = 785

ADD_LEI = GraphTransactions.ADD_LEI.value
GET_LEI = GraphTransactions.GET_LEI.value
GET_SIGN = GraphTransactions.GET_SIGN.value
SIGN_LEI = GraphTransactions.SIGN_LEI.value


LEI_FIELD = "lei"
GRAPH_CONTENT_FIELD = "content"
GRAPH_FORMAT_FIELD = "format"
GRAPH_HASH_FIELD = "ihash"
GRAPH_SIGN_FIELD = "sign"
SIGN_IHASH_FIELD = "sign_ihash"
