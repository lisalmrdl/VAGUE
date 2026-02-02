import requests, os, json, datetime, time
import sqlite3 as sql
from tqdm import tqdm
from pathlib import Path
from pprint import pprint

def insert_into_games(tup: tuple):
    cur.execute("""
    INSERT INTO games
    (id, name, released, genres, developers, publishers, metacritic, rating, ratings_count, description)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, tup)

if input("\033[31mWARNING: This action will remove and reset the database. Do you REALLY want to do it?(yes/no)\033[0m") != "yes":
    raise(KeyboardInterrupt("Execution was manually interrupted."))

path = Path(os.path.abspath(os.path.dirname(__file__)))
API_KEY = json.load(open(path / ".." / "apikeys.json", "r"))["RAWG"]
BASE_URL = "https://api.rawg.io/api/games"
n_items = 10000 # TODO: Change this
items_per_page = 40 # 40 is tha maximum
try:
    metadata = json.load(open(path / ".." / "data" / "db.json", "r"))["metadata"]
    if "games "not in json.load(open(path / ".." / "data" / "db.json", "r"))["games"].keys(): raise KeyError()
except:
    json.dump({
        "metadata" : {
            "date": str(datetime.date.today()),
            "pages": 0,
            "length": 0
        },
        "games" : {}
    }, open(path / ".." / "data" / "db.json", "w"))
    metadata = json.load(open(path / ".." / "data" / "db.json", "r"))["metadata"]

params = {
    "key": API_KEY,
    "page": 1,
    "page_size": items_per_page,
    #"ordering": "-metacritic", # Default order seems to be popularity
    "esrb_rating": "1,2,3,4"
}


######### SCHEMA DEFINITION #############
# This time it's gonna push directly into the DB
db_path = path / ".." / "data" / "games.db"
if os.path.exists(db_path):
    os.remove(db_path)
conn = sql.connect(db_path)
cur = conn.cursor()
# Entities
cur.execute("""
CREATE TABLE game (
    id_game INT PRIMARY KEY,
    name VARCHAR,
    released VARCHAR,
    metacritic INT,
    rating FLOAT,
    ratings_count INT,
    description VARCHAR,
    image_url VARCHAR, 
    units_sold INT
);
""")

#    genres VARCHAR,
#    developers VARCHAR,
#    publishers VARCHAR,

cur.execute("""
CREATE TABLE company (
    id_company INT PRIMARY KEY,
    name VARCHAR,
    logo_url VARCHAR
);
""")

cur.execute("""
CREATE TABLE genre (
    id_genre INT PRIMARY KEY,
    name VARCHAR,
    description VARCHAR
);
""")
# Relations
cur.execute("""
CREATE TABLE has_genre (
    id_game INT PRIMARY KEY,
    id_genre INT PRIMARY KEY,
    FOREIGN KEY (id_game) REFERENCES game(id_game),
    FOREIGN KEY (id_genre) REFERENCES genre(id_genre),
    PRIMARY KEY (id_game, id_genre)
);
""")
cur.execute("""
CREATE TABLE developed (
    id_game INT PRIMARY KEY,
    id_company INT PRIMARY KEY,
    FOREIGN KEY (id_company) REFERENCES company(id_company),
    FOREIGN KEY (id_game) REFERENCES game(id_game),
    PRIMARY KEY (id_game, id_company)
)
""")
cur.execute("""
CREATE TABLE published (
    id_game INT PRIMARY KEY,
    id_company INT PRIMARY KEY,
    FOREIGN KEY (id_company) REFERENCES company(id_company),
    FOREIGN KEY (id_game) REFERENCES game(id_game),
    PRIMARY KEY (id_game, id_company)
)
""")

######### END OF SCHEMA DEFINITION #############
#seen_developers = [] # Store the IDs of the seen developers to avoid UNIQUE constraint violation

for page in tqdm(range(1, int(n_items/items_per_page)+1)):
    games = {}
    params["page"] = page
    response = requests.get(BASE_URL, params=params)
    data = response.json()
    for i in data["results"]:
        try:
            # Json
            id = i["id"]
            game = requests.get(
                f"https://api.rawg.io/api/games/{id}",
                params={"key": API_KEY}
            ).json()
            games[id] = {
                "name" : game["name"],
                "released": game["released"],
                "genres": ", ".join([x["name"].lower() for x in game["genres"]]), # To table Genres
                "developers": ", ".join([x["name"] for x in game["developers"]]), # To table company
                "publishers": ", ".join([x["name"] for x in game["publishers"]]), # To table company
                "metacritic":game["metacritic"],
                "rating": game["rating"],
                "ratings_count": game["ratings_count"],
                "description_raw": game["description_raw"],
                "image_url": game["background_image"],
                "units_sold": game["added"]
            }

            # DataBase
            # Add game
            cur.execute("""
                INSERT INTO games
                (id_company, name, released, metacritic, rating, ratings_count, description, image_url, units_sold)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                """, (
                    id,
                    game["name"],
                    game["released"],
                    game["metacritic"],
                    game["rating"],
                    game["ratings_count"],
                    game["description_raw"],
                    game["background_image"],
                    game["added"]
                ))
            for dev in game["developers"]:
                # Add developer if necessary
                cur.execute("""
                INSERT OR IGNORE INTO company
                (id_company, name, logo_url)
                VALUES (?, ?, ?);
                """, (
                    dev["id"],
                    dev["name"],
                    dev["image_background"],
                ))
                # Add developer relation
                cur.execute("""
                INSERT INTO developed
                (id_company, id_game)
                VALUES (?, ?);
                """, (
                    dev["id"],
                    id
                ))
            for dev in game["publishers"]:
                # Add publisher if necessary
                cur.execute("""
                INSERT OR IGNORE INTO company
                (id_company, name, logo_url)
                VALUES (?, ?, ?);
                """, (
                    dev["id"],
                    dev["name"],
                    dev["image_background"],
                ))
                # Add developer relation
                cur.execute("""
                INSERT INTO published
                (id_company, id_game)
                VALUES (?, ?);
                """, (
                    dev["id"],
                    id
                ))
            for gen in game["genres"]:
                # Add genre if necessary
                cur.execute("""
                INSERT OR IGNORE INTO genre
                (id_genre, name, description)
                VALUES (?, ?, ?);
                """, (
                    gen["id"],
                    gen["name"],
                    gen["description"],
                ))
                # Add has_genre relation
                cur.execute("""
                INSERT INTO has_genre
                (id_genre, id_game)
                VALUES (?, ?);
                """, (
                    gen["id"],
                    id
                ))
        except: pass
    metadata["pages"] += 1
    metadata["length"] += items_per_page
    db = json.load(open(path / ".." / "data" / "db.json", "r"))
    db["metadata"] = metadata
    for k, v in games.items():
        db["games"][k] = v
    json.dump(db, open(path / ".." / "data" / "db.json", "w"))
    conn.commit()
    del db

t = time.time()
pprint(cur.execute("""
                    SELECT name, metacritic, released FROM game
                    WHERE   released > "2020-01-01" AND
                            metacritic > 90
                    ORDER BY released DESC
                  """).fetchall())
print(f"That query took: {time.time() - t} seconds\n")

t = time.time()
pprint(cur.execute("""
                    SELECT name FROM genre
                  """).fetchall())
print(f"That query took: {time.time() - t} seconds\n")

t = time.time()
pprint(cur.execute("""
                    SELECT g.name, d.name, g.metacritic, released 
                    FROM game g NATURAL JOIN developer d
                    WHERE   released > "2020-01-01" AND
                            released < "2021-01-01" AND
                            metacritic > 70
                    ORDER BY released DESC
                  """).fetchall())
print(f"That query took: {time.time() - t} seconds\n")
conn.close()
