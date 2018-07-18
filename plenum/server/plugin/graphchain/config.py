from plenum.common.constants import KeyValueStorageType


def get_config(config):
    config.leiTransactionsFile = 'lei_transactions'
    config.leiStateStorage = KeyValueStorageType.Leveldb
    config.leiStateDbName = 'lei_state'
    return config
