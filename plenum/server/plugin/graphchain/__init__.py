from plenum.server.plugin.graphchain.constants import GRAPHCHAIN_LEDGER_ID
from plenum.server.plugin.graphchain.transactions import GraphTransactions

LEDGER_IDS = {GRAPHCHAIN_LEDGER_ID, }

AcceptableWriteTypes = {
    GraphTransactions.ADD_LEI.value,
}

AcceptableQueryTypes = {
    GraphTransactions.GET_LEI.value,
}