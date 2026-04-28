
logger = None  # 초기값


def set_logger(external_logger):
    global logger
    logger = external_logger


class BatchGooses4Timer:

    def __init__(self, isStartGooses, dictGooseRunTime, gooseRunningCnt, hasStartedToday):
        self.isStartGooses = isStartGooses
        self.dictGooseRunTime = dictGooseRunTime
        self.gooseRunningCnt = gooseRunningCnt
        self.hasStartedToday = hasStartedToday
