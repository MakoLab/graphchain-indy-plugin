from plenum.common.constants import KeyValueStorageType


def update_nodes_config_with_plugin_settings(config):
    config.graphchainTransactionsFile = 'graphchain_transactions'
    config.graphchainStateStorage = KeyValueStorageType.Rocksdb
    config.graphchainStateDbName = 'graphchain_state'
    return config
