from plenum.common.transactions import Transactions

PREFIX = '7850'


class GraphTransactions(Transactions):
    ADD_LEI = PREFIX + '0'
    GET_LEI = PREFIX + '1'
