from distribution_processing.WAL.KVStore import KVStore
from distribution_processing.WAL.SetValueCommand import SetValueCommand
from distribution_processing.WAL.WAL import WAL

wal = WAL()
store = KVStore(wal)

store.set("name", "Alice")
store.set("age", 30)

print(store.get("name"))  # Alice
print(store.get("age"))   # 30

print("WAL Entries:")

for entry in wal.entries:
    cmd = SetValueCommand.deserialize(entry.commandData)
    print(entry.entryIndex, cmd.key, cmd.value, entry.timestamp)