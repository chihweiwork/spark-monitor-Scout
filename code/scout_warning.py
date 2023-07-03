class UndefinedOutputMode(Exception):
    """
    Exception raised for undefined output mode
    """
    def __init__(self, mode):
        self.message = f"Undefined output mode {mode}"
        super().__init__(self.message)

class UndefinedGuardian(Exception):
    """
    Exception raised for undefined Monitor Guardian
    """
    def __init__(self, guardian_type):
        self.message = f"Undefined guardian type: {guardian_type}, please check your configer available guradian: type-I"
        super().__init__(self.message)
