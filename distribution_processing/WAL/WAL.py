import pickle
import os
from datetime import datetime

from distribution_processing.WAL.WALEntry import WALEntry

class WAL:
    def __init__(self, filepath:str):
        self.filepath = filepath
        self.entries = []
        self.counter = 0
        self.load()

    def write_entry(self, command_bytes: bytes) -> int:
        self.counter += 1
        entry = WALEntry(entryIndex=self.counter,
                         commandData=command_bytes,
                         timestamp=datetime.now())
        self.entries.append(entry)
        self._save_entry(entry)
        return self.counter

    def _save_entry(self, entry: WALEntry) -> None:
        with open(self.filepath, 'ab') as f:
            pickle.dump(entry, f)

    def load(self) -> None:
        self.entries = []
        self.counter = 0
        if not os.path.exists(self.filepath):
            return
        with open(self.filepath, 'rb') as f:
            try:
                while True:
                    entry: WALEntry = pickle.load(f)
                    self.entries.append(entry)
                    self.counter = max(self.counter, entry.entryIndex)
            except EOFError:
                pass