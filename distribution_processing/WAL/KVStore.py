from typing import AnyStr, Optional, Dict, Any

from distribution_processing.WAL.SetValueCommand import SetValueCommand
from distribution_processing.WAL.WAL import WAL


class KVStore:
    def __init__(self, wal: WAL):
        self.kv: Dict[str, Any] = {}
        self.wal = wal

    def get(self, key: str) -> Optional[Any]:
        return self.kv.get(key)

    def set(self, key: str, value: AnyStr) -> None:
        self.append_log(key, value)
        self.kv[key] = value

    def append_log(self, key: str, value: Any) -> int:
        cmd = SetValueCommand(key, value)
        return self.wal.writeEntry(cmd.serialize())