"""
Very abstract implementation for steaming client like Kafka, Redis etc.
"""


class ClientBase:
    def push(self, **kwargs):
        pass

    def pull(self, **kwargs):
        pass
