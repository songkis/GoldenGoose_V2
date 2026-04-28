logger = None  # 초기값


def set_logger(external_logger):
    global logger
    logger = external_logger


class CPluginManager:
    gooses = None

    def __init__(self, main_window):
        self.parent = main_window  # MainWindow 인스턴스 저장
        # print(f"__init__ self.main_window: {self.parent}")

    # @classmethod
    def plugin_loader(self):
        # print(f"self : {self}, self.__base__: {self.__hash__}, self.main_window: {self.main_window}")
        result = {}
        gooses = []

        ##1. Load gooses From files
        # path = "gooses/"
        # sys.path.insert(0, path)
        # for f in os.listdir(path):
        #    fname, ext = os.path.splitext(f)
        #    print(f"load {fname} _ {'_' not in fname} ")
        #    #배포할때 ui_로 시작하는 화면파일이 생성되서 회피가 필요.
        #    #if ("_" not in fname and "Goose" in fname)and ext == '.py':
        #    if  ext == '.py':
        #        #mod = __import__(fname)
        #        mod = importlib.import_module(fname)
        #        # goose = mod.goose_loader()
        #        # if goose is not None:
        #        #    result[goose.Name] = goose
        # sys.path.pop(0)

        # 2.정적처리
        # import gooses.AIGoose as AIGoose  # 명시적으로 import
        # import gooses.GuardianGoose as GuardianGoose  # 명시적으로 import
        # aiGoose = AIGoose.goose_loader()
        # result[aiGoose.Name] = aiGoose
        # guardianGoose = GuardianGoose.goose_loader()
        # result[guardianGoose.Name] = guardianGoose
        # gooses.append( aiGoose )
        # gooses.append( guardianGoose )
        # self.parent.gooses = gooses
        # self.parent.SaveGoosesToDb()

        ##3. DB에서 읽어서 처리
        # Assuming parent has GetGoosesFromDb.
        # For type safety we might want to check attr, but this logic is tightly coupled to MainWindow.
        if hasattr(self.parent, "GetGoosesFromDb"):
            try:
                # logger.info("Calling GetGoosesFromDb...")
                gooses = self.parent.GetGoosesFromDb()
                # logger.info("Returned from GetGoosesFromDb")
            except Exception as e:
                logger.exception(f"Error calling GetGoosesFromDb: {e}")
                gooses = []
        else:
            logger.error("Parent window does not have GetGoosesFromDb method")
            gooses = []

        logger.info(
            f"[{self.__class__.__name__}] plugin_loader GetGoosesFromDb gooses : {gooses}"
        )
        # gooses가 None이면 빈 딕셔너리로 처리
        if gooses is None:
            gooses = []
        # 딕셔너리 컴프리헨션
        result = {obj.__class__.__name__: obj for obj in gooses}
        logger.info(
            f"[{self.__class__.__name__}] 딕셔너리 컴프리헨션 self.main_window.result: {result}"
        )

        logger.info(f"[{self.__class__.__name__}] gooses_loader : {result}")
        CPluginManager.gooses = result
        return result
