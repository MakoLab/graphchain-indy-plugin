from stp_core.common.log import getlogger

from plenum.server.plugin.graphchain import GRAPHCHAIN_LEDGER_ID
from plenum.server.plugin.graphchain.client_authnr import GraphchainAuthNr
from plenum.server.plugin.graphchain.config import get_config
from plenum.server.plugin.graphchain.graph_req_handler import \
    GraphchainReqHandler
from plenum.server.plugin.graphchain.exceptions import \
    UnableToInitializeGraphStore
from plenum.server.plugin.graphchain.graph_store import GraphStore
from plenum.server.plugin.graphchain.storage import get_lei_hash_store, \
    get_lei_ledger, get_lei_state
from plenum.common.constants import DOMAIN_LEDGER_ID

logger = getlogger()


def integrate_plugin_in_node(node):
    node_name = node.name
    start_msg = "Integrating the GraphChain plugin into the '{}' node.".format(
        node_name)
    logger.info(start_msg)

    _print_node_debug_info(node)

    node.config = get_config(node.config)

    hash_store = get_lei_hash_store(node.dataLocation)
    ledger = get_lei_ledger(node.dataLocation,
                            node.config.leiTransactionsFile,
                            hash_store,
                            node.config)
    if GRAPHCHAIN_LEDGER_ID not in node.ledger_ids:
        node.ledger_ids.append(GRAPHCHAIN_LEDGER_ID)
    node.ledgerManager.addLedger(GRAPHCHAIN_LEDGER_ID,
                                 ledger,
                                 postTxnAddedToLedgerClbk=node.postTxnFromCatchupAddedToLedger)
    node.on_new_ledger_added(GRAPHCHAIN_LEDGER_ID)

    # Create a GraphStore and check connection to a TS
    logger.info("Initializing TS database for node '{}'...".format(node_name))
    node.graph_store = GraphStore(node_name)
    if node.graph_store.check_whether_db_exists():
        logger.info("Database within TS exists.")
    else:
        logger.info("Database within TS does not exists. Creating...")
        db_creation_result, reasons = node.graph_store.create_db()
        if db_creation_result:
            logger.info("Database created successfully."
                        .format(node_name))
        else:
            msg = "Unable to initialize graph store for " \
                  "node '{}'. Reasons: {}".format(node_name, reasons)
            raise UnableToInitializeGraphStore(msg)

    # Registering state
    state = get_lei_state(node.dataLocation,
                          node.config.leiStateDbName,
                          node.config)
    node.register_state(GRAPHCHAIN_LEDGER_ID, state)

    # Creating authentication mechanism for LEI requests
    lei_authnr = GraphchainAuthNr(node.states[DOMAIN_LEDGER_ID])
    node.clientAuthNr.register_authenticator(lei_authnr)

    # Creating request handler for LEI requests
    lei_req_handler = GraphchainReqHandler(ledger, state, node.graph_store)
    node.register_req_handler(GRAPHCHAIN_LEDGER_ID, lei_req_handler)

    return node


def _print_node_debug_info(node):
    pass
