from SPARQLWrapper.SPARQLExceptions import SPARQLWrapperException
from common.serializers.json_serializer import JsonSerializer
from common.serializers.serialization import ledger_txn_serializer
from plenum.common.constants import TXN_TIME, TXN_TYPE
from plenum.common.exceptions import InvalidClientRequest
from plenum.common.request import Request
from plenum.common.txn_util import reqToTxn, append_txn_metadata
from plenum.common.types import f
from plenum.server.ledger_req_handler import LedgerRequestHandler
from rdflib import Graph

from plenum.server.plugin.graphchain.constants import ADD_LEI, GET_LEI, \
    LEI_FIELD, GRAPH_CONTENT_FIELD, GRAPH_FORMAT_FIELD, \
    GRAPH_IHASH_FIELD, TXN_FIELD, DATA_FIELD, TXN_METADATA_FIELD, SYNC_PAIR_GRAPH_CONTENT, SYNC_PAIR_GRAPH_FORMAT
from plenum.server.plugin.graphchain.graph_store_synchronizer import GraphStoreSynchronizer
from plenum.server.plugin.graphchain.graphs import FormatValidator, \
    GraphValidator
from plenum.server.plugin.graphchain.hashes import InterwovenHashCalculator
from plenum.server.plugin.graphchain.helpers import from_base64, dict_to_bytes, str_to_bytes, bytes_to_str, \
    bytes_to_dict
from plenum.server.plugin.graphchain.logger import get_debug_logger

logger = get_debug_logger()


UTF_8 = "utf-8"


class GraphchainReqHandler(LedgerRequestHandler):
    write_types = {ADD_LEI}
    query_types = {GET_LEI}

    def __init__(self, ledger, state, graph_store, graph_store_synchronizer: GraphStoreSynchronizer):
        super().__init__(ledger, state)

        self._format_validator = FormatValidator()
        self._graph_validator = GraphValidator()
        self._hash_calculator = InterwovenHashCalculator()
        self._graph_store = graph_store
        self._graph_store_synchronizer = graph_store_synchronizer

        self._graph_store_synchronizer.start(self._graph_store_sync_job)

        self.query_handlers = {
            GET_LEI: self.handle_get_lei
        }

    def get_query_response(self, req: Request):
        return self.query_handlers[req.operation[TXN_TYPE]](req)

    def handle_get_lei(self, req: Request, show_debug: bool = False):
        op = req.operation
        op_type = op.get(TXN_TYPE)
        logger.info("Handling '{}' read operation...".format(op_type))
        logger.debug("Request's details: {}".format(req))
        graph_hash = op.get(GRAPH_IHASH_FIELD)

        found_data = self.ledger.get(**{GRAPH_IHASH_FIELD: graph_hash})
        logger.debug("found_data: {}".format(found_data))

        if show_debug:
            self._print_debug_data(found_data)

        if found_data is not None:
            txn_fragment = found_data.get(TXN_FIELD)
            data_fragment = dict(txn_fragment.get(DATA_FIELD))
            lei_data = dict(data_fragment.get(LEI_FIELD))
            logger.debug("request:        {}".format(req))
            logger.debug("data_fragment:  {}".format(data_fragment))

            return {
                TXN_TYPE: txn_fragment.get(TXN_TYPE),
                f.IDENTIFIER.nm: req.identifier,
                f.REQ_ID.nm: req.reqId,

                f.SEQ_NO.nm: found_data.get(TXN_METADATA_FIELD).get(f.SEQ_NO.nm),
                TXN_TIME: found_data.get(TXN_METADATA_FIELD).get(TXN_TIME),

                GRAPH_IHASH_FIELD: found_data.get(GRAPH_IHASH_FIELD),
                LEI_FIELD: {
                    GRAPH_CONTENT_FIELD: lei_data.get(GRAPH_CONTENT_FIELD),
                    GRAPH_FORMAT_FIELD: lei_data.get(GRAPH_FORMAT_FIELD)
                },

                # TARGET_NYM: data_fragment.get(TARGET_NYM)  # Should this be returned?
            }
        else:
            logger.info("Data for '{}' not found in the ledger.".format(graph_hash))
            return {
                f.IDENTIFIER.nm: req.identifier,
                f.REQ_ID.nm: req.reqId,
                LEI_FIELD: None
            }

    def doStaticValidation(self, request: Request):
        identifier, req_id, op = request.identifier, request.reqId, \
                                 request.operation
        op_type = op.get(TXN_TYPE)
        logger.debug("Static validation for the '{}' operation type: \n"
                     "   identifier = {},\n"
                     "   reqId = {},\n"
                     "   operation = {}"
                     .format(op_type, identifier, req_id, op))

        if op_type == ADD_LEI:
            logger.debug("Static validation of ADD_LEI op type...")
            lei = op.get(LEI_FIELD)

            if not isinstance(lei, dict):
                msg = "{} attribute is missing or not in proper format: '{}'".format(LEI_FIELD, lei)
                raise InvalidClientRequest(identifier, req_id, msg)

            self._validate_add_lei_request(identifier, req_id, lei)

        elif op_type == GET_LEI:
            logger.debug("Static validation of GET_LEI op type: nothing for now")

        logger.info("Static validation finished without errors.")

    def validate(self, request: Request):
        op = request.operation
        op_type = op.get(TXN_TYPE)
        # lei = op.get(LEI_FIELD)
        logger.debug("Validation request '{}': operation = {}".format(op_type, op))

        if op_type == ADD_LEI:
            logger.debug("There is not any dynamic validation for '{}' op.".format(op_type))
            # We don't need to do anything here for now, but in the future
            # we may want to validate whether the client from whom this request
            # came (LOU) is permissioned to handle this specific LEI.

    def apply(self, req: Request, cons_time: int):
        op = req.operation
        op_type = op.get(TXN_TYPE)
        logger.info("Applying op '{}' type...".format(op_type))

        if op_type == ADD_LEI:
            lei = op.get(LEI_FIELD)
            graph_hash = self._calculate_hash(lei)
            logger.debug("Calculated hash: {}".format(graph_hash))

            txn = self._req_to_txn(req)
            txn = append_txn_metadata(txn, txn_id=self._gen_txn_path(txn))

            self.ledger.append_txns_metadata([txn], cons_time)
            (start, end), _ = self.ledger.appendTxns([self._transform_txn_for_ledger(txn, graph_hash)])
            self.updateState([txn])

            graph_raw_content = from_base64(lei.get(GRAPH_CONTENT_FIELD))
            graph_format = lei.get(GRAPH_FORMAT_FIELD)

            logger.debug("Attempting to add a new pair to synchronizer...")

            self._graph_store_synchronizer.add(
                str_to_bytes(graph_hash),
                dict_to_bytes({SYNC_PAIR_GRAPH_CONTENT: bytes_to_str(graph_raw_content),
                               SYNC_PAIR_GRAPH_FORMAT: graph_format}))

            self.update_graph_store_with_sync(graph_hash, graph_raw_content, graph_format)

            return start, txn

        else:
            logger.info("Not supported op type: '{}'.".format(op_type))

    def updateState(self, txns, isCommitted=False):
        logger.debug("Updating state for a new txns:")
        for txn in txns:
            logger.debug("  {}".format(txn))
            self._updateStateWithSingleTxn(txn, isCommitted=isCommitted)

    def update_graph_store_with_sync(self, graph_hash, graph_raw_content, graph_format):
        logger.debug("Updating graph store (with sync) for graph with hash '{}'.".format(graph_hash))

        try:
            self._update_graph_store(graph_hash, graph_raw_content, graph_format)

            if self._check_whether_hash_is_already_in_ts(graph_hash):
                logger.debug("Graph with hash '{}' successfully added to TS. Removing from synchronizer..."
                             .format(graph_hash))
                self._graph_store_synchronizer.remove(str_to_bytes(graph_hash))
            else:
                logger.warn("Graph with hash '{}' was not added to the TS for some reasons.")
        except RecursionError as ex:
            logger.warn("RecursionError was thrown while updating graph store. Details: {}".format(ex))
        except SPARQLWrapperException as ex:
            logger.warn("Exception thrown while updating graph store. "
                        "graph_hash = '{}', graph_raw_content = '{}', graph_format = '{}'\nDetails: {}"
                        .format(graph_hash, graph_raw_content, graph_format, ex))
        except Exception as ex:
            logger.warn("Unspecified exception was thrown while updating graph store with sync. "
                        "graph_hash = '{}', graph_raw_content = '{}', graph_format = '{}'\nDetails: {}"
                        .format(graph_hash, graph_raw_content, graph_format, ex))

    def _update_graph_store(self, graph_hash, graph_raw_content, graph_format):
        self._graph_store.add_graph(graph_raw_content, graph_format, graph_hash)

    def _updateStateWithSingleTxn(self, txn, isCommitted=False):
        # MAYBE: Something to do here?
        pass

    def _validate_add_lei_request(self, identifier, req_id, lei):
        graph_base64 = lei.get(GRAPH_CONTENT_FIELD)
        if graph_base64 is None or len(graph_base64) == 0:
            msg = "'{}' field within '{}' must be present and " \
                  "should not be empty.".format(GRAPH_CONTENT_FIELD, LEI_FIELD)
            raise InvalidClientRequest(identifier, req_id, msg)

        graph_format = lei.get(GRAPH_FORMAT_FIELD)
        supported, reason = self._format_validator.validate_format(graph_format, False)
        if not supported:
            raise InvalidClientRequest(identifier, req_id, reason)

        graph = from_base64(graph_base64)
        graph_valid, reason = self._graph_validator.validate_graph(graph, graph_format)
        if not graph_valid:
            msg = "Content of graph is invalid. Details: {}".format(reason)
            raise InvalidClientRequest(identifier, req_id, msg)

        ihash = self._calculate_hash(lei)
        if self._check_whether_hash_is_already_in_ts(ihash):
            msg = "Graph with hash '{}' already added to the ledger".format(ihash)
            raise InvalidClientRequest(identifier, req_id, msg)

    def _calculate_hash(self, lei):
        graph = lei.get(GRAPH_CONTENT_FIELD)
        graph_format = lei.get(GRAPH_FORMAT_FIELD)
        g = Graph()
        g.parse(data=from_base64(graph), format=graph_format)
        return self._hash_calculator.calculate_hash(g)

    def _check_whether_hash_is_already_in_ledger(self, graph_hash):
        found_data = self.ledger.get(**{GRAPH_IHASH_FIELD: graph_hash})
        result = found_data is not None
        logger.debug("Hash of graph ({}) already stored? {}".format(graph_hash, result))
        return result

    def _check_whether_hash_is_already_in_ts(self, graph_hash):
        try:
            result = self._graph_store.check_if_graph_is_already_stored(graph_hash)
            logger.debug("Hash of graph ({}) already stored in TS? {}".format(graph_hash, result))
            return result
        except Exception as ex:
            logger.warn("Exception thrown while checking whether hash is already in TS. Details: {}".format(ex))
            result = self._check_whether_hash_is_already_in_ledger(graph_hash)
            logger.debug("Hash of graph ({}) already stored ledger? {}".format(graph_hash, result))
            return result

    def _gen_txn_path(self, txn):
        return None

    def _req_to_txn(self, req):
        return reqToTxn(req)

    def _graph_store_sync_job(self):
        logger.debug("Graph store sync job starts...")

        counter = 0

        for pair in self._graph_store_synchronizer.list_all():
            logger.debug("Handling sync pair '{}'...".format(pair))
            graph_hash = bytes_to_str(pair[0])
            graph_dict = bytes_to_dict(pair[1])
            graph_raw_content = graph_dict[SYNC_PAIR_GRAPH_CONTENT]
            graph_format = graph_dict[SYNC_PAIR_GRAPH_FORMAT]

            self.update_graph_store_with_sync(graph_hash, graph_raw_content, graph_format)

            counter += 1

        logger.debug("Graph store sync job finished with {} handled items.".format(counter))

    def handle_post_txn_added_to_ledger_clbk(self, txn):
        logger.debug("Handling callback: post_txn_added_to_ledger_clbk. Txn details: {}".format(txn))
        data_element = txn.get(TXN_FIELD).get(DATA_FIELD)
        graph_format = data_element.get(LEI_FIELD).get(GRAPH_FORMAT_FIELD)
        decoded_content = data_element.get(LEI_FIELD).get(GRAPH_CONTENT_FIELD)
        graph_raw_content = from_base64(decoded_content)
        graph_hash = txn.get(GRAPH_IHASH_FIELD)

        logger.debug("Adding graph to graph store. graph_raw_content='{}', graph_format='{}', graph_hash='{}'"
                     .format(graph_raw_content, graph_format, graph_hash))

        self._graph_store_synchronizer.add(
            str_to_bytes(graph_hash),
            dict_to_bytes({SYNC_PAIR_GRAPH_CONTENT: bytes_to_str(graph_raw_content),
                           SYNC_PAIR_GRAPH_FORMAT: graph_format}))

        self.update_graph_store_with_sync(graph_hash, graph_raw_content, graph_format)

    @staticmethod
    def _transform_txn_for_ledger(txn, graph_hash):
        logger.debug("Adding graph hash to the transaction: {}".format(txn))
        # MAYBE: Remove GRAPH_CONTENT?
        txn[GRAPH_IHASH_FIELD] = graph_hash
        return txn

    @staticmethod
    def _print_debug_data(found_data):
        serializer = JsonSerializer()

        txn = ledger_txn_serializer.deserialize(found_data)
        txn = serializer.serialize(txn, toBytes=False)
        logger.debug("txn: {}".format(txn))
