from enum import Enum
import inspect


class ManipulationType(Enum):
    TRANSFORMATION = "transformation"
    MERGE = "merge"
    ACTION = "action"


class Manipulation:

    def __init__(
        self,
        type: ManipulationType,
        func: callable,
        data_id: int = None
    ):
        self.type = type
        self.func = func
        if data_id is None:
            if _accepts_arguments(self.func):
                self.data_id = -1
            else:
                self.data_id = None
        else:
            self.data_id = data_id


def _accepts_arguments(func):
    try:
        signature = inspect.signature(func)
        return len(signature.parameters) > 0
    except (ValueError, TypeError):
        return False
