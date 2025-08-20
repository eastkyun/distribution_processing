from typing import AnyStr, Optional, Dict, Any

from distribution_processing.WAL.SetValueCommand import SetValueCommand
from distribution_processing.WAL.WAL import WAL


class KVStore:
    def __init__(self, wal: WAL):
        self.kv = {}
        self.wal = wal
        self.recover_from_wal()

    def get(self, key):
        return self.kv.get(key)

    def set(self, key, value):
        cmd = SetValueCommand(key, value)
        self.wal.write_entry(cmd.serialize())
        self.kv[key] = value

    def recover_from_wal(self):
        for entry in self.wal.entries:
            cmd = SetValueCommand.deserialize(entry.commandData)
            self.kv[cmd.key] = cmd.value