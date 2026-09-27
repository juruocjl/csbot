"""Pure local regression checks: no bot startup, network, or credentials."""
from io import BytesIO
from pathlib import Path
import sqlite3
import sys
import tempfile
import unittest

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from PIL import Image
from ai_runtime.media import MediaCache
from ai_runtime.queries import compile_query,TEMPLATES
from ai_runtime.data import DataBroker
from ai_runtime.runner import scope_key
from ai_runtime.store import StateStore


class BoundaryChecks(unittest.TestCase):
    def test_templates_and_sql_denials(self):
        params={"uid":"123","steamid":"76561190000000000","record_id":1,"mid":"test","season":"S21"}
        for name,(source,sql,defaults,_) in TEMPLATES.items():
            with self.subTest(name=name):
                self.assertIn("LIMIT 501",compile_query(sql,defaults|params,"123",source))
        for sql in ["DELETE FROM messages", "SELECT 1; SELECT 2", "SELECT pg_read_file('/etc/passwd')",
                    "SELECT * FROM auth_sessions", "SELECT * FROM public.messages", "SELECT * INTO x FROM messages",
                    "SELECT * FROM messages FOR UPDATE", "SELECT 'auth_sessions'::regclass",
                    "WITH x AS (DELETE FROM messages RETURNING *) SELECT * FROM x", "SELECT pg_sleep(30)",
                    "SELECT * FROM generate_series(1,10000000)","PRAGMA writable_schema=ON",
                    "SELECT current_setting('password')","SELECT dblink('host=example','select 1')"]:
            with self.subTest(sql=sql),self.assertRaises((ValueError,Exception)):
                compile_query(sql,{},"123")
        query=compile_query("WITH q AS (SELECT * FROM messages) SELECT * FROM q UNION ALL SELECT * FROM messages",{},"123")
        self.assertEqual(query.count("group_id = '123'"),2)
        query=compile_query("SELECT * FROM messages WHERE plain_text=:q",{"q":"'; DELETE FROM groupmsg; --"},"123")
        self.assertIn("''; DELETE",query)

    def test_game_scope_and_authorizer(self):
        with tempfile.TemporaryDirectory() as tmp:
            path=Path(tmp)/"game.sqlite3"
            with sqlite3.connect(path) as db:
                db.executescript("CREATE TABLE friend_game_history(id INTEGER,user_id TEXT,game_id TEXT,changed_at TEXT);CREATE TABLE game_name_map(game_id TEXT,game_name TEXT,icon_url TEXT,updated_at TEXT);INSERT INTO friend_game_history VALUES(1,'one','730','2026-01-01T00:00:00Z'),(2,'other','','2026-01-02T00:00:00Z');")
            broker=DataBroker(None,"123",path)
            result=broker._game(compile_query("SELECT * FROM game_history",{},"123","game",("one",)))
            self.assertEqual([row["user_id"] for row in result["rows"]],["one"])
            self.assertEqual(broker._game(compile_query("SELECT * FROM game_history",{},"123","game"))["rows"],[])
            with self.assertRaises(sqlite3.DatabaseError):broker._game("DELETE FROM friend_game_history")

    def test_original_lru_and_thumbnail_survival(self):
        def png(color):
            buffer=BytesIO();Image.new("RGB",(96,96),color).save(buffer,format="PNG");return buffer.getvalue()
        images=[png(color) for color in ("red","green","blue")]
        with tempfile.TemporaryDirectory() as tmp:
            cache=MediaCache(Path(tmp),sum(map(len,images[:2])))
            first=cache.put(images[0]);second=cache.put(images[1])
            with cache.lease(first):
                third=cache.put(images[2])
                self.assertEqual(cache.info(first)["full_status"],"available")
                self.assertEqual(cache.info(second)["full_status"],"evicted")
                self.assertLessEqual(cache.usage()["original_bytes"],cache.budget)
            for key in (first,second,third):
                self.assertTrue(Path(cache.info(key)["thumbnail_path"]).is_file())
            with self.assertRaises(ValueError):cache.info("../../etc/passwd")
            existing=cache.info(first)["full_path"]
            Path(existing).unlink()
            self.assertEqual(cache.info(first)["full_status"],"missing")
            cache.db.close()

    def test_scope_and_record_ownership(self):
        self.assertEqual(scope_key("qq","1","2"),scope_key("qq","1","3"))
        self.assertNotEqual(scope_key("qq","1","2"),scope_key("qq","4","2"))
        self.assertNotEqual(scope_key("web","1","2"),scope_key("web","1","3"))
        self.assertNotEqual(scope_key("web","1","2"),scope_key("qq","1","2"))
        with tempfile.TemporaryDirectory() as tmp:
            store=StateStore(Path(tmp)/"state.sqlite3")
            store.register_run("personal","scope","1","2","web","test")
            self.assertTrue(store.can_read("personal","1","2"))
            self.assertFalse(store.can_read("personal","1","3"))
            self.assertFalse(store.can_read("personal","4","2"))
            store.register_run("group","scope2","1","2","qq","test")
            self.assertTrue(store.can_read("group","1","3"))
            self.assertFalse(store.can_read("missing","1","2"))
            store.close()


if __name__=="__main__":unittest.main()
