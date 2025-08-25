from multiprocessing import Pipe
from multiprocessing import Queue as MPQueue
from typing import Any
import queue

import asyncio


class Queue:

    def __init__(self):
        pass

    def put(
        self,
        item: Any,
        block: bool = True,
        timeout: float | None = None
    ) -> None:
        pass

    def get(
        self,
        block: bool = True,
        timeout: float | None = None
    ) -> Any:
        pass

    async def get_async(self) -> Any:
        pass

    def empty(self) -> bool:
        pass


class SMQueue(Queue):

    def __init__(self):
        self.queue = MPQueue()

    def put(
        self,
        item: Any,
        block: bool = True,
        timeout: float | None = None
    ) -> None:
        self.queue.put(item, block, timeout)

    def get(
        self,
        block: bool = True,
        timeout: float | None = None
    ) -> Any:
        return self.queue.get(block, timeout)

    def empty(self):
        return self.queue.empty()

    async def get_async(self):
        return await asyncio.to_thread(self.queue.get)


class CFQueue(Queue):

    def __init__(self):
        self.parent_conn, self.child_conn = Pipe()
        # Not implemented in AWS Lambda
        # self.read_lock = Lock()
        # self.write_lock = Lock()

    def put(
        self,
        item: Any,
        block: bool = True,
        timeout: float | None = None
    ) -> None:
        # Not implemented in AWS Lambda
        # with self.write_lock:
        self.child_conn.send(item)

    def get(
        self,
        block: bool = True,
        timeout: float | None = None
    ) -> Any:
        if timeout is not None:
            if self.parent_conn.poll(timeout):
                return self.parent_conn.recv()
            else:
                raise queue.Empty
        return self.parent_conn.recv()
    
    def empty(self):
        return not self.parent_conn.poll(0)

    async def get_async(self):
        return await asyncio.to_thread(self.parent_conn.recv)


async def get_async(queue: Queue, timeout=None):
    """Polls the pipe and returns data when available."""
    if isinstance(queue, CFQueue):
        while not queue.parent_conn.poll(timeout):
            await asyncio.sleep(0.05)  # Non-blocking wait
        return queue.get()
    else:
        return await queue.get_async()
