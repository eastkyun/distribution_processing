import os

from distribution_processing.WAL.KVStore import KVStore
from distribution_processing.WAL.WAL import WAL

# ---  데이터 테스트 ---
wal_file = 'wal.log'

if os.path.exists(wal_file):
    os.remove(wal_file)

wal = WAL(wal_file)
store = KVStore(wal)

for i in range(2000):
    store.set(f"key{i}", f"value{i}")

print("KVStore size:", len(store.kv))

# --- 재시작 후 복구 테스트 ---
wal2 = WAL(wal_file)
store2 = KVStore(wal2)
print("Recovered KVStore size:", len(store2.kv))

print(store2.get("key0"))
print(store2.get("key1999"))
