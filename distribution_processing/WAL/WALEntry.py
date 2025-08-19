from dataclasses import dataclass
from typing import Optional, Any
from datetime import datetime

@dataclass
class WALEntry:
    entryIndex: int
    commandData: bytes
    timestamp: datetime
