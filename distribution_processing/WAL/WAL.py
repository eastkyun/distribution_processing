from datetime import datetime

from distribution_processing.WAL.WALEntry import WALEntry

class WAL:
    def __init__(self):
        self.entries = []
        self.counter = 0

    def writeEntry(self, command_bytes: bytes) -> int:
        self.counter += 1
        entry = WALEntry(entryIndex=self.counter,
                         commandData=command_bytes,
                         timestamp=datetime.now())
        self.entries.append(entry)
        return self.counter