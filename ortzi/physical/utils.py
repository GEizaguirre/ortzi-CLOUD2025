import math
import threading


class ThreadSafeStageCounter:
    def __init__(
        self,
        final_value: int,
        initial_value: int = 0,
        asynchronous_perc: int = 20
    ):
        self._value = initial_value
        self._goal = final_value
        self._async_goal = math.ceil(
            final_value * (asynchronous_perc / 100)
        )
        self._lock = threading.Lock()

    def increment(self, amount=1):
        with self._lock:
            self._value += amount

    def end(self) -> bool:
        with self._lock:
            return self._value == self._goal

    def async_end(self) -> bool:
        with self._lock:
            return self._value >= self._async_goal


class ThreadSafeCounter:
    def __init__(
        self,
        initial_value: int = 0
    ):
        self._value = initial_value
        self._lock = threading.Lock()

    def increment(self, amount=1):
        with self._lock:
            self._value += amount

    def decrement(self, amount=1):
        with self._lock:
            self._value -= amount
