import os, json, time
import sqlite3 as sql
from pathlib import Path
from pprint import pprint

def insert_into(tup: tuple):
    cur.execute("""
    INSERT INTO games
    (id, name, released, genres, developers, publishers, metacritic, rating, ratings_count, description)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, tup)


if input("\033[31mWARNING: This action will remove and reset the database. Do you REALLY want to do it?(yes/no)\033[0m") != "yes":
    raise(KeyboardInterrupt("Execution was manually interrupted."))

path = Path(os.path.abspath(os.path.dirname(__file__)))
db_path = path / ".." / "data" / "games.db"
if os.path.exists(db_path):
    os.remove(db_path)
conn = sql.connect(db_path)
cur = conn.cursor()

cur.execute("""
CREATE TABLE games (
    id INT PRIMARY KEY,
    name VARCHAR,
    released VARCHAR,
    genres VARCHAR,
    developers VARCHAR,
    publishers VARCHAR,
    metacritic INT,
    rating FLOAT,
    ratings_count INT,
    description VARCHAR
)
""")

json_db = json.load(open(path / ".." / "data" / "db.json"))

for k in json_db["games"].keys():
    insert_into(tuple( [k] + list(json_db["games"][k].values())))

print(f"DATABASE CREATED")


print(f"Testing queries")
# Select the name, the developers, the metacritic score and the release date from games that released after 2019 that have >90 metacritic score ordered by release date (from newest to oldest)
t = time.time()
pprint(cur.execute("""
                    SELECT name, developers, metacritic, released FROM games
                    WHERE   released > "2020-01-01" AND
                            metacritic > 90
                    ORDER BY released DESC
                  """).fetchall())
print(f"That query took: {time.time() - t} seconds\n")

# Select the name, the developers, the metacritic score and the release date from rpg games that released between 2000 and 2015 (non inclusive) that have >93 metacritic score ordered by metacritic score in descendent order
t = time.time()
pprint(cur.execute("""
                    SELECT name, developers, metacritic, released FROM games
                    WHERE   released < "2015-01-01" AND
                            released > "2000-01-01" AND
                            metacritic > 93 AND
                            genres LIKE "% rpg%"
                    ORDER BY metacritic DESC
                  """).fetchall())
print(f"That query took: {time.time() - t} seconds\n")
conn.commit()
conn.close()