from plenum.common.constants import DOMAIN_LEDGER_ID

from plenum.server.plugin.graphchain import GRAPHCHAIN_LEDGER_ID
from plenum.server.plugin.graphchain.client_authnr import GraphchainAuthNr
from plenum.server.plugin.graphchain.config import \
    update_nodes_config_with_plugin_settings
from plenum.server.plugin.graphchain.constants import STARDOG, NEPTUNE
from plenum.server.plugin.graphchain.exceptions import \
    NoDatabaseWithinTripleStore, TripleStoreTypeNotSupported
from plenum.server.plugin.graphchain.graph_req_handler import \
    GraphchainReqHandler
from plenum.server.plugin.graphchain.graph_store import GraphStoreType
from plenum.server.plugin.graphchain.logger import get_debug_logger
from plenum.server.plugin.graphchain.neptune_graph_store import NeptuneGraphStore
from plenum.server.plugin.graphchain.stardog_graph_store import StardogGraphStore
from plenum.server.plugin.graphchain.storage import get_graphchain_hash_store, \
    get_graphchain_ledger, get_graphchain_state

logger = get_debug_logger()


def integrate_plugin_in_node(node):
    start_msg = "Integrating the GraphChain plugin into the '{}' node.".format(node.name)
    logger.info(start_msg)

    _print_node_debug_info(node)

node.config = update_nodes_config_with_plugin_settings(node.config)

    hash_store = get_graphchain_hash_store(node.dataLocation)
    ledger = _prepare_ledger(node, hash_store)

    _prepare_graph_store(node)

    state = _prepare_and_register_state(node)

    _prepare_authnr(node)

    graphchain_req_handler = _prepare_request_handler(node, ledger, state)

    def post_txn_added_to_ledger_clbk(ledger_id, txn):
        graphchain_req_handler.handle_post_txn_added_to_ledger_clbk(txn)
        node.postTxnFromCatchupAddedToLedger(ledger_id, txn)

    _register_ledger(node, ledger, post_txn_added_to_ledger_clbk)

    logger.debug("Registering request handler with ID equal to '{}'...".format(GRAPHCHAIN_LEDGER_ID))
    node.register_req_handler(graphchain_req_handler, GRAPHCHAIN_LEDGER_ID)

    return node


def _print_node_debug_info(node):
    logger.debug("{}".format(node.collectNodeInfo()))


def _prepare_ledger(node, hash_store):
    logger.debug("Creating a new ledger with ID '{}'...".format(GRAPHCHAIN_LEDGER_ID))
    ledger = get_graphchain_ledger(node.dataLocation, node.config.graphchainTransactionsFile, hash_store, node.config)
    return ledger


def _prepare_graph_store(node):
    logger.info("Initializing TS database...")

    ts_type = _obtain_ts_type(node.config.ts_type)
    ts_url = node.config.ts_url
    ts_user = node.config.ts_user
    ts_pass = node.config.ts_pass
    ts_db_name = node.name + node.config.ts_db_name_suffix

    if ts_type == GraphStoreType.STARDOG:
        node.graph_store = StardogGraphStore(ts_db_name, ts_url, ts_user, ts_pass)
    elif ts_type == GraphStoreType.NEPTUNE:
        node.graph_store = NeptuneGraphStore(ts_db_name, ts_url)
    else:
        msg = "'{}' triple store type is not supported.".format(ts_type)
        raise TripleStoreTypeNotSupported(msg)

    if node.graph_store.check_whether_db_exists():
        logger.info("Database '{}' within TS exists.".format(ts_db_name))
    else:
        msg = "There is not a database '{}' in the triple store with URL '{}'.".format(ts_db_name, ts_url)
        raise NoDatabaseWithinTripleStore(msg)


def _obtain_ts_type(ts_type: str):
    if ts_type == STARDOG:
        return GraphStoreType.STARDOG
    elif ts_type == NEPTUNE:
        return GraphStoreType.NEPTUNE
    else:
        msg = "Triple store type '{}' is not supported.".format(ts_type)
        raise TripleStoreTypeNotSupported(msg)


def _prepare_and_register_state(node):
    state = get_graphchain_state(node.dataLocation,
                                 node.config.graphchainStateDbName,
                                 node.config)
    node.register_state(GRAPHCHAIN_LEDGER_ID, state)
    return state


def _prepare_authnr(node):
    graphchain_authnr = GraphchainAuthNr(node.states[DOMAIN_LEDGER_ID])
    node.clientAuthNr.register_authenticator(graphchain_authnr)


def _prepare_request_handler(node, ledger, state):
    logger.debug("Preparing request handler...")
    return GraphchainReqHandler(ledger, state, node.graph_store)


def _register_ledger(node, ledger, post_txn_added_to_ledger_clbk):
    logger.debug("Registering ledger...")
    if GRAPHCHAIN_LEDGER_ID not in node.ledger_ids:
        node.ledger_ids.append(GRAPHCHAIN_LEDGER_ID)
    node.ledgerManager.addLedger(GRAPHCHAIN_LEDGER_ID,
                                 ledger,
                                 postTxnAddedToLedgerClbk=post_txn_added_to_ledger_clbk)
    node.on_new_ledger_added(GRAPHCHAIN_LEDGER_ID)
