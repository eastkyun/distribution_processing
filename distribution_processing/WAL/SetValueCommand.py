import pickle
from dataclasses import dataclass
from typing import  Any


@dataclass
class SetValueCommand:
    key: str
    value: Any

    def serialize(self) -> bytes:
        return pickle.dumps({'key': self.key, 'value': self.value})

    @staticmethod
    def deserialize(data: bytes) -> 'SetValueCommand':
        obj = pickle.loads(data)
        return SetValueCommand(obj['key'], obj['value'])