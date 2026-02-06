import os, json, time
import sqlite3 as sql
from pathlib import Path
from pprint import pprint
import database as db

path = Path(os.path.abspath(os.path.dirname(__file__)))
db_path = path / ".." / "data" / "games.db"
print(os.path.abspath(db_path))
conn = sql.connect(db_path)
cur = conn.cursor()



print(f"Testing queries")

pprint(cur.execute("""
                    SELECT COUNT(*) FROM game
                  """).fetchall())
print("NEW QUERY")
pprint(cur.execute("""
                    SELECT g.id_game, g.name as games, GROUP_CONCAT(gen.name, ', '), g.metacritic as genres FROM 
                    genre gen NATURAL JOIN has_genre x
                    JOIN game g ON x.id_game == g.id_game
                    WHERE   g.metacritic > 80 AND
                            g.released > "2010/01/01" AND
                            g.released < "2024/01/01"
                    GROUP BY g.name
                    ORDER BY g.metacritic DESC
                    LIMIT 10
                  """).fetchall())
print("NEW QUERY")
pprint(cur.execute("""
                    SELECT d.name, AVG(g.rating) 
                    FROM game g NATURAL JOIN developed
                    JOIN company d ON d.id_company == developed.id_company
                    GROUP BY d.name
                    ORDER BY AVG(g.rating) DESC
                    LIMIT 10
                  """).fetchall())


conn.close()