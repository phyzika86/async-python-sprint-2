class EarlyStartError(Exception):
    def __init__(self, message):
        self.message = message

    def __repr__(self) -> str:
        class_name = self.__class__.__name__
        return f"{class_name}(message={self.message!r})"
