"""
A very abstract implementation for steaming client like Kafka, Redis etc.
"""
from collections import deque, defaultdict
from abc import ABC, abstractmethod


class ClientAbstract(ABC):
    @abstractmethod
    def push(self, topic, value):
        pass

    @abstractmethod
    def pull(self):
        pass

    @abstractmethod
    async def message_iterator(self):
        pass


class ClientBase(ClientAbstract):
    def __init__(self):
        self.queue = defaultdict(deque)

    async def connect(self):
        pass

    async def disconnect(self):
        pass

    async def subscribe(self, topics):
        pass

    def push(self, topic, value):
        self.queue[None].append(value)

    def pull(self):
        return self.queue[None].popleft()

    async def message_iterator(self):
        while self.queue:
            yield self.queue[None].popleft()
