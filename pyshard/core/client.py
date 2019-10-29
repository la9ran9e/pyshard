import abc
from typing import Any
import json

from .connect import ConnectionABC, TCPConnection, AsyncTCPConnection


Payload = dict
JsonStr = str
Response = JsonStr


class SerialyzerError(Exception): ...


class Serialyzer:
    serialyzer = json

    @classmethod
    def dump(cls, dict_obj):
        return cls.serialyzer.dumps(dict_obj)

    @classmethod
    def load(cls, json_obj):
        return cls.serialyzer.loads(json_obj)


class ClientABC(abc.ABC):
    @abc.abstractmethod
    def _serialize(self, payload: Payload) -> str: ...
    @abc.abstractmethod
    def _deserialize(self, response: str) -> dict: ...
    @abc.abstractmethod
    def _execute(self, method: str, *args, **kwargs) -> Response: ...
    @abc.abstractmethod
    def _handle_response(self, response: dict) -> Any: ...


class ClientError(Exception): ...


class ClientBase(ClientABC):
    def __init__(self, host, port, transport_class=TCPConnection,
                 serialyzer=Serialyzer, **conn_kwargs):
        self.addr = (host, port)
        self._serialyzer = serialyzer
        self._transport = transport_class(host, port, **conn_kwargs)
        self._transport.connect()

    def _serialize(self, payload):
        return self._serialyzer.dump(payload)

    def _deserialize(self, response):
        return self._serialyzer.load(response)

    def _execute(self, method, *args, **kwargs):
        payload = {'endpoint': method,
                   'args': args,
                   'kwargs': kwargs}

        self._transport.send(self._serialize(payload))
        return self._handle_response(self._deserialize(self._transport.recv()))

    def _handle_response(self, response):
        if response['type'] == 'error':
            err = response['message']
            raise ClientError(f'Couldn\'t execute: {err}')

        return response['message']

    def getsockname(self):
        return self._transport.getsockname()

    def close(self) -> None:
        self._transport.close()


class AsyncClientBase(ClientABC):
    def __init__(self, host, port, connector_class=AsyncTCPConnection,
                 serialyzer=Serialyzer, **conn_kwargs):
        self.addr = (host, port)
        self._serialyzer = serialyzer
        self._conn = connector_class(host, port, **conn_kwargs)
        self._conn.connect()

    def _serialize(self, payload):
        return self._serialyzer.dump(payload)

    def _deserialize(self, response):
        return self._serialyzer.load(response)

    async def _execute(self, method, *args, **kwargs):
        payload = {'endpoint': method,
                   'args': args,
                   'kwargs': kwargs}

        await self._conn.send(self._serialize(payload))
        return await self._conn.recv()

    def _handle_response(self, response):
        if response['type'] == 'error':
            err = response['message']
            raise ClientError(f'Couldn\'t execute: {err}')

        return response['message']

    def getsockname(self):
        return self._conn.getsockname()

    def close(self) -> None:
        self._conn.close()
