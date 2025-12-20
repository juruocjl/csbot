import sqlite3
import msgpack

conn = sqlite3.connect('main.db')
cursor = conn.cursor()

cursor.execute("SELECT id, sid, timestamp, data FROM groupmsg")

res = {}

for row in cursor.fetchall():
    id, sid, timestamp, data = row
    unpacked_data = msgpack.unpackb(data, raw=False)
    res[id] = {
        'sid': sid,
        'timestamp': timestamp,
        'data': unpacked_data
    }

with open("exported_messages.msgpack", "wb") as f:
    f.write(msgpack.packb(res, use_bin_type=True))