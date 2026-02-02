import os, json, time
import sqlite3 as sql
from pathlib import Path
from pprint import pprint

path = Path(os.path.abspath(os.path.dirname(__file__)))
db_path = path / ".." / "data" / "games.db"
print(os.path.abspath(db_path))
conn = sql.connect(db_path)
cur = conn.cursor()


print(f"Testing queries")

pprint(cur.execute("""
                    SELECT DISTINCT genres FROM games
                  """).fetchall())

conn.close()