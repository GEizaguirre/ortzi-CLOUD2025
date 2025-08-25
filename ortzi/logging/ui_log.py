import datetime
import logging
from logging.handlers import RotatingFileHandler
import random
import string
from time import sleep
import multiprocessing

from ortzi.config import config

_log_queue: multiprocessing.Queue = None
_finish_event = None


class OrtziLogEmitter:

    def __init__(
            self,
            emitter_name: str = ""
    ):
        global _log_queue
        global _finish_event

        if _log_queue is None:
            _log_queue = multiprocessing.Queue(-1)
            self.queue = _log_queue
        if _finish_event is None:
            _finish_event = multiprocessing.Event()
        self.queue = _log_queue
        self.finish_event = _finish_event
        self.emitter_name = emitter_name
        self.innerlogger = logging.getLogger(__name__)
        self.logger_name = {"logger_name": self.emitter_name}
        self.innerlogger = logging.LoggerAdapter(
            self.innerlogger,
            self.logger_name
        )

    def info(self, s: str):

        self.innerlogger.info(s)


class OrtziLogEmitterStdout(OrtziLogEmitter):

    def __init__(
        self,
        emitter_name: str = "",
        log_queue: multiprocessing.Queue = _log_queue,
        finish_event: multiprocessing.Event = _finish_event
    ):
        pass

    def info(self, s: str):
        print(s)


class OrtziLogReceiver:

    def __init__(
        self,
        log_queue: multiprocessing.Queue = _log_queue,
        finish_event: multiprocessing.Event = _finish_event
    ):
        self.queue = log_queue
        self.finish_event = finish_event

    def start(self):
        manager = multiprocessing.Manager()
        self.results_dict = manager.dict()
        self.listener = multiprocessing.Process(
            target=listener_process,
            args=(self.queue, self.finish_event, self.results_dict, )
        )
        self.listener.start()

    def stop(self):
        self.finish_event.set()
        self.listener.join()
        fname = self.results_dict["fname"]
        print(f"Log results at: {fname}")
        return fname


def configure_log_listener():
    root = logging.getLogger()
    random_sufix = ''.join(
        random.choice(string.ascii_lowercase)
        for _ in range(3)
    )
    current_time = datetime.datetime.now().strftime("%H-%M-%S-%f")[:-3]
    file_handler = RotatingFileHandler(
        f'ortzi-logging-{random_sufix}-{current_time}.log', 'a'
    )
    console_handler = logging.StreamHandler()
    formatter = logging.Formatter(
        '%(asctime)s.%(msecs)03d %(logger_name)s %(levelname)-4s %(message)s',
        "%H:%M:%S"
    )
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    root.addHandler(file_handler)
    root.addHandler(console_handler)
    root.setLevel(logging.DEBUG)
    return file_handler.stream.name


def listener_process(
    queue: multiprocessing.Queue,
    finish_event: multiprocessing.Event,
    results: dict
):
    fname = configure_log_listener()
    results["fname"] = fname
    while True:
        while not queue.empty():
            record = queue.get()
            logger = logging.getLogger(record.name)
            logger.handle(record) 
        if finish_event.is_set():
            return None
        sleep(0.5)


def get_logger(s: str = "", root: bool = False) -> OrtziLogEmitter:
    if config.get("local_mode"):
        logger = OrtziLogEmitter(s)
    else:
        logger = OrtziLogEmitterStdout(s)

    return logger


def root_configurer(queue: multiprocessing.Queue = _log_queue):
    h = logging.handlers.QueueHandler(queue)
    root = logging.getLogger()
    root.addHandler(h)
    root.setLevel(logging.DEBUG)


def start_logging() -> OrtziLogReceiver:  
    logging.shutdown()
    from importlib import reload
    reload(logging)
    receiver = OrtziLogReceiver()
    receiver.start()
    root_configurer()
    return receiver
