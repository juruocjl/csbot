import msgpack
import sqlite3

conn = sqlite3.connect('main.db')
cursor = conn.cursor()
cursor.execute('SELECT * FROM groupmsg')
rows = cursor.fetchall()
for row in rows:
    id = row[0]
    packed_msg = row[4]
    unpacked_msg = msgpack.unpackb(packed_msg)
    newmsg = []
    for segment in unpacked_msg:
        if segment[0] != 'text' or len(segment) != 1:
            newmsg.append(segment)
        else:
            print('gg')
    cursor.execute('UPDATE groupmsg SET data = ? WHERE id = ?', (msgpack.packb(newmsg), id))
conn.commit()