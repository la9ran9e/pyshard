from core.typing import Codec


def to_bytes(str_obj: str, codec: Codec) -> bytes:
    return bytes(str_obj, encoding=codec)


def from_bytes(bytes_obj: bytes, codec: Codec) -> str:
    return bytes_obj.decode(codec)
