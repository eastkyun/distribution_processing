
import pickle
import zlib
import struct
import threading
import os
from datetime import datetime

from distribution_processing.WAL.SetValueCommand import SetValueCommand
from distribution_processing.WAL.WALEntry import WALEntry


class WAL:
    def __init__(self, filepath: str,
                 batch_size: int = 100,
                 flush_interval: float = 0.01,
                 durability: str = 'periodic'):
        """
        durability: 'strong' | 'periodic' | 'async'
        """
        self.filepath = filepath
        self.entries = []
        self.counter = 0

        # flush 정책 관련
        self.buffer = []
        self.lock = threading.Lock()
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        self.durability = durability
        self._stop = threading.Event()
        self._flusher = threading.Thread(target=self._flusher_loop, daemon=True)
        self._flusher.start()

        # 기존 로그 복구
        self.load()

    def write_entry(self, command_bytes: bytes) -> int:
        self.counter += 1
        entry = WALEntry(entryIndex=self.counter,
                         commandData=command_bytes,
                         timestamp=datetime.now())
        self.entries.append(entry)
        # serialize + crc
        payload = pickle.dumps(entry)
        crc = zlib.crc32(payload) & 0xffffffff
        # [4바이트 길이][payload 데이터][4바이트 CRC32]
        record = struct.pack(">I", len(payload)) + payload + struct.pack(">I", crc)

        self._append(record)
        return self.counter

    def _append(self, record: bytes):
        if self.durability == 'strong':
            with open(self.filepath, 'ab') as f:
                f.write(record)
                f.flush()
                os.fsync(f.fileno())
        else:
            with self.lock:
                self.buffer.append(record)
                if len(self.buffer) >= self.batch_size:
                    self._flush_locked()

    def _flusher_loop(self):
        while not self._stop.wait(self.flush_interval):
            with self.lock:
                if self.buffer:
                    self._flush_locked()

    def _flush_locked(self):
        with open(self.filepath, 'ab') as f:
            for rec in self.buffer:
                f.write(rec)
            f.flush()
            if self.durability == 'periodic':
                os.fsync(f.fileno())
        self.buffer.clear()

    def close(self):
        self._stop.set()
        self._flusher.join()
        # flush remaining
        with self.lock:
            if self.buffer:
                self._flush_locked()

    def load(self) -> None:
        self.entries = []
        self.counter = 0
        if not os.path.exists(self.filepath):
            return
        with open(self.filepath, 'rb') as f:
            while True:
                hdr = f.read(4)
                if len(hdr) < 4:
                    break
                length = struct.unpack(">I", hdr)[0]
                payload = f.read(length)
                if len(payload) < length:
                    break
                crc_bytes = f.read(4)
                if len(crc_bytes) < 4:
                    break
                crc = struct.unpack(">I", crc_bytes)[0]
                if zlib.crc32(payload) & 0xffffffff != crc:
                    print("CRC mismatch! truncated log?")
                    break
                entry: WALEntry = pickle.loads(payload)
                self.entries.append(entry)
                self.counter = max(self.counter, entry.entryIndex)