from ledger.compact_merkle_tree import CompactMerkleTree
from plenum.common.constants import HS_LEVELDB
from plenum.common.ledger import Ledger
from plenum.persistence.db_hash_store import DbHashStore
from state.pruning_state import PruningState
from storage.helper import initKeyValueStorage
from stp_core.common.log import getlogger

from plenum.server.plugin.graphchain.constants import GRAPHCHAIN_HASH_STORE_NAME

logger = getlogger()


def get_graphchain_hash_store(data_dir):
    logger.info("Creating LEI hash store in '{}'.".format(data_dir))

    return DbHashStore(dataDir=data_dir,
                       fileNamePrefix=GRAPHCHAIN_HASH_STORE_NAME,
                       db_type=HS_LEVELDB)


def get_graphchain_ledger(data_dir, name, hash_store, config):
    logger.info("Creating LEI ledger store with '{}' name in the '{}' dir.".format(name, data_dir))

    return Ledger(CompactMerkleTree(hashStore=hash_store),
                  dataDir=data_dir,
                  fileName=name,
                  ensureDurability=config.EnsureLedgerDurability)


def get_graphchain_state(data_dir, name, config):
    logger.info("Creating LEI state with name '{}' in the '{}' dir and storage type equal to '{}'."
                .format(name, data_dir, config.graphchainStateStorage))

    return PruningState(initKeyValueStorage(config.graphchainStateStorage, data_dir, name))
