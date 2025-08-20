import os

from distribution_processing.WAL.KVStore import KVStore
from distribution_processing.WAL.WAL import WAL

def test_strong_flush():
    wal_file = "wal_strong.log"
    if os.path.exists(wal_file):
        os.remove(wal_file)

    wal = WAL(wal_file, durability='strong')
    store = KVStore(wal)

    store.set("name", "Alice")
    store.set("age", 30)
    wal.close()

    wal2 = WAL(wal_file, durability='strong')
    store2 = KVStore(wal2)
    wal2.close()

    assert store2.get("name") == "Alice"
    assert store2.get("age") == 30
    assert len(store2.kv) == 2


def test_periodic_flush():
    wal_file = "wal_periodic.log"
    if os.path.exists(wal_file):
        os.remove(wal_file)

    wal = WAL(wal_file, durability='periodic', batch_size=10, flush_interval=0.1)
    store = KVStore(wal)

    for i in range(1000):
        store.set(f"key{i}", f"value{i}")
    wal.close()

    wal2 = WAL(wal_file, durability='periodic')
    store2 = KVStore(wal2)
    wal2.close()

    assert len(store2.kv) == 1000
    assert store2.get("key0") == "value0"
    assert store2.get("key999") == "value999"


def test_async_flush():
    wal_file = "wal_async.log"
    if os.path.exists(wal_file):
        os.remove(wal_file)

    wal = WAL(wal_file, durability='async')
    store = KVStore(wal)

    for i in range(5):
        store.set(f"key{i}", f"value{i}")
    wal.close()

    wal2 = WAL(wal_file, durability='async')
    store2 = KVStore(wal2)
    wal2.close()

    assert len(store2.kv) == 5
    assert store2.get("key4") == "value4"


def test_crc_detection():
    wal_file = "wal_crc.log"
    if os.path.exists(wal_file):
        os.remove(wal_file)

    wal = WAL(wal_file, durability='strong')
    store = KVStore(wal)
    store.set("safe", "data")
    store.set("unsafe", "data2")
    wal.close()

    with open(wal_file, "rb+") as f:
        f.seek(-2, os.SEEK_END)
        f.truncate()

    wal2 = WAL(wal_file, durability='strong')
    store2 = KVStore(wal2)
    wal2.close()
    # 손상된 엔트리는 무시되어도 안전 데이터는 살아있음
    assert store2.get("unsafe") != "data2"
    assert store2.get("safe") == "data"
    assert len(store2.kv) == 1


if __name__ == "__main__":
    test_strong_flush()
    test_periodic_flush()
    test_async_flush()
    test_crc_detection()
    print("✅ All tests passed!")
