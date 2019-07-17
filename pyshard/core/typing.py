from typing import *

Key = Union[int, float, str]
Doc = Union[int, float, str, dict, list, tuple]
Hash = float
Offset = int
Addr = Tuple[str, int]

Codec = str

Request = Tuple[str, list, dict]
Response = str
rRequest = str
rResponse = str
