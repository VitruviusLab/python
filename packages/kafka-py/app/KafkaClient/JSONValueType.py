from typing import Union, List, Dict


JSONValueType = Union[
    None, bool, int, float, str, List["JSONValueType"], Dict[str, "JSONValueType"]
]
