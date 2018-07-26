from plenum.common.constants import TXN_TIME, TXN_TYPE, TARGET_NYM
from plenum.common.exceptions import InvalidClientRequest
from plenum.common.request import Request
from plenum.common.types import f
from plenum.persistence.util import txnsWithSeqNo
from plenum.server.req_handler import RequestHandler
from rdflib import Graph
from stp_core.common.log import getlogger

from plenum.server.plugin.graphchain.constants import ADD_LEI, GET_LEI, \
    LEI_FIELD, GRAPH_CONTENT_FIELD, GRAPH_FORMAT_FIELD, \
    GRAPH_HASH_FIELD
from plenum.server.plugin.graphchain.graphs import FormatValidator, \
    GraphValidator
from plenum.server.plugin.graphchain.hashes import InterwovenHashCalculator
from plenum.server.plugin.graphchain.helpers import from_base64, req_to_txn

logger = getlogger()


class GraphchainReqHandler(RequestHandler):
    write_types = {ADD_LEI}
    query_types = {GET_LEI}

    def __init__(self, ledger, state, graph_store):
        super().__init__(ledger, state)

        self._format_validator = FormatValidator()
        self._graph_validator = GraphValidator()
        self._hash_calculator = InterwovenHashCalculator()
        self._graph_store = graph_store

        self.query_handlers = {
            GET_LEI: self.handle_get_lei
        }

    def get_query_response(self, req: Request):
        return self.query_handlers[req.operation[TXN_TYPE]](req)

    def handle_get_lei(self, req: Request):
        op = req.operation
        op_type = op.get(TXN_TYPE)
        logger.info("Handling '{}' read operation...".format(op_type))
        graph_hash = op.get(GRAPH_HASH_FIELD)

        raw_data = self.ledger.get(**{GRAPH_HASH_FIELD: graph_hash})
        if raw_data is not None:
            data = dict(raw_data)
            lei_data = dict(data.get(LEI_FIELD))
            logger.info("request:  {}".format(req))
            logger.info("data:     {}".format(data))

            return {
                TXN_TYPE: data.get(TXN_TYPE),
                f.IDENTIFIER.nm: req.identifier,
                f.REQ_ID.nm: req.reqId,

                f.SEQ_NO.nm: data.get(f.SEQ_NO.nm),
                TXN_TIME: data.get(TXN_TIME),

                GRAPH_HASH_FIELD: data.get(GRAPH_HASH_FIELD),
                LEI_FIELD: {
                    GRAPH_CONTENT_FIELD: lei_data.get(GRAPH_CONTENT_FIELD),
                    GRAPH_FORMAT_FIELD: lei_data.get(GRAPH_FORMAT_FIELD)
                },

                TARGET_NYM: data.get(TARGET_NYM)
            }
        else:
            logger.info("Data for '{}' not found.".format(graph_hash))
            return {}

    def doStaticValidation(self, request: Request):
        identifier, req_id, op = request.identifier, request.reqId, \
                                 request.operation
        op_type = op.get(TXN_TYPE)
        logger.info("Static validation for the '{}' operation type: "
                    "identifier = {}, "
                    "reqId = {}, "
                    "operation = {}"
                    .format(op_type, identifier, req_id, op))

        if op_type == ADD_LEI:
            logger.info("Static validation of ADD_LEI op type...")
            lei = op.get(LEI_FIELD)

            if not isinstance(lei, dict):
                msg = "{} attribute is missing or not in proper format: '{}'" \
                    .format(LEI_FIELD, lei)
                raise InvalidClientRequest(identifier, req_id, msg)

            self._validate_add_lei_request(identifier, req_id, lei)

        elif op_type == GET_LEI:
            logger.info("Static validation of GET_LEI op type: nothing for now")

        logger.info("Static validation finished without errors.")

    def validate(self, request: Request):
        op = request.operation
        op_type = op.get(TXN_TYPE)
        # lei = op.get(LEI_FIELD)
        logger.info("Validation request '{}': operation = {}"
                    .format(op_type, op))
        logger.info("There is not any dynamic validation for '{}' op."
                    .format(op_type))

        if op_type == ADD_LEI:
            pass
            # We don't need to do anything here for now, but in the future
            # we may want to validate whether the client from whom this request
            # came (LOU) is permissioned to handle this specific LEI.

    def apply(self, req: Request, cons_time: int):
        op = req.operation
        op_type = op.get(TXN_TYPE)
        logger.info("Applying op '{}' type...".format(op_type))

        if op_type == ADD_LEI:
            lei = op.get(LEI_FIELD)
            ihash = self._calculate_hash(lei)
            logger.info("Calculated hash: {}".format(ihash))

            txn = req_to_txn(req, cons_time)
            txn = self._transform_txn_for_ledger(txn, ihash)
            logger.info("txn after transformation: {}".format(txn))
            (start, end), _ = self.ledger.appendTxns([txn])

            self.updateState(txnsWithSeqNo(start, end, [txn]))

            self.update_graph_store(lei, ihash)

            return start, txn

        else:
            logger.info("Not supported op type: '{}'.".format(op_type))

    def updateState(self, txns, isCommitted=False):
        for txn in txns:
            self._updateStateWithSingleTxn(txn, isCommitted=isCommitted)

    def update_graph_store(self, lei, graph_hash):
        raw_graph = from_base64(lei.get(GRAPH_CONTENT_FIELD))
        graph_format = lei.get(GRAPH_FORMAT_FIELD)
        self._graph_store.add_lei(raw_graph, graph_format, graph_hash)

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
        supported, reason = self._format_validator \
            .validate_format(graph_format, False)
        if not supported:
            raise InvalidClientRequest(identifier, req_id, reason)

        graph = from_base64(graph_base64)
        graph_valid, reason = self._graph_validator \
            .validate_graph(graph, graph_format)
        if not graph_valid:
            msg = "Content of graph is invalid. Details: {}".format(reason)
            raise InvalidClientRequest(identifier, req_id, msg)

    def _calculate_hash(self, lei):
        graph = lei.get(GRAPH_CONTENT_FIELD)
        graph_format = lei.get(GRAPH_FORMAT_FIELD)
        g = Graph()
        g.parse(data=from_base64(graph), format=graph_format)
        return self._hash_calculator.calculate_hash(g)

    @staticmethod
    def _transform_txn_for_ledger(txn, graph_hash):
        logger.info("Transforming TXN for type '{}'.".format(txn.get(ADD_LEI)))
        # MAYBE: Remove GRAPH_CONTENT?
        txn[GRAPH_HASH_FIELD] = graph_hash
        return txn

    @staticmethod
    def _transform_txn_for_ledger_sign_lei(txn, ihash):
        # txn[GRAPH_HASH_FIELD]
        return txn
