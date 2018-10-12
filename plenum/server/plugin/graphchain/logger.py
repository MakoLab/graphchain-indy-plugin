from stp_core.common.log import getlogger

GRAPHCHAIN_LOGGER_NAME = 'GC'


def get_debug_logger():
    logger = getlogger(GRAPHCHAIN_LOGGER_NAME)
    logger.setLevel('DEBUG')
    return logger
