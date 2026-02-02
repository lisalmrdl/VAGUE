import requests, os, json, datetime, time
import sqlite3 as sql
from tqdm import tqdm
from pathlib import Path
from pprint import pprint

if input("\033[31mWARNING: This action will remove and reset the database. Do you REALLY want to do it?(yes/no)\033[0m") != "yes":
    raise(KeyboardInterrupt("Execution was manually interrupted."))

session = requests.Session()
path = Path(os.path.abspath(os.path.dirname(__file__)))
API_KEY = json.load(open(path / ".." / "apikeys.json", "r"))["RAWG2"]
BASE_URL = "https://api.rawg.io/api/games"
n_items = 10000 # NOTE: Amount of games to fetch
items_per_page = 40 # 40 is tha maximum
error_logs = ""
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
    website_url VARCHAR, 
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
    name VARCHAR
);
""")
# Relations
cur.execute("""
CREATE TABLE has_genre (
    id_game INT,
    id_genre INT,
    FOREIGN KEY (id_game) REFERENCES game(id_game) DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (id_genre) REFERENCES genre(id_genre) DEFERRABLE INITIALLY DEFERRED,
    PRIMARY KEY (id_game, id_genre)
);
""")
cur.execute("""
CREATE TABLE developed (
    id_game INT,
    id_company INT,
    FOREIGN KEY (id_company) REFERENCES company(id_company) DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (id_game) REFERENCES game(id_game) DEFERRABLE INITIALLY DEFERRED,
    PRIMARY KEY (id_game, id_company)
)
""")
cur.execute("""
CREATE TABLE published (
    id_game INT,
    id_company INT,
    FOREIGN KEY (id_company) REFERENCES company(id_company) DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (id_game) REFERENCES game(id_game) DEFERRABLE INITIALLY DEFERRED,
    PRIMARY KEY (id_game, id_company)
)
""")

######### END OF SCHEMA DEFINITION #############
#seen_developers = [] # Store the IDs of the seen developers to avoid UNIQUE constraint violation

for page in tqdm(range(1, int(n_items/items_per_page)+1)):
    games = {}
    params["page"] = page
    response = session.get(BASE_URL, params=params)
    data = response.json()
    conn.execute("BEGIN")
    for i in data["results"]:
        try:
            # Json
            id = i["id"]
            game = session.get(
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
                "website": game["website"],
                "units_sold": game["added"]
            }
            # DataBase
            # Add game
            cur.execute("""
                INSERT INTO game
                (id_game, name, released, metacritic, rating, ratings_count, description, image_url, units_sold, website_url)
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
                    game["added"],
                    game["website"]
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
                (id_genre, name)
                VALUES (?, ?);
                """, (
                    gen["id"],
                    gen["name"]
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
        except sql.IntegrityError as e:
            error_logs += f"\nConstraint error: {e}. On {game["name"]} with id {game["id"]}"
        except sql.DatabaseError as e:
            error_logs += f"\nSQLite error: {e}. On {game["name"]} with id {game["id"]}"
        except Exception as e:
            error_logs += f"\nGeneric exception: {e}. On {game["name"]} with id {game["id"]}"
    metadata["pages"] += 1
    metadata["length"] += items_per_page
    db = json.load(open(path / ".." / "data" / "db.json", "r"))
    db["metadata"] = metadata
    for k, v in games.items():
        db["games"][k] = v
    json.dump(db, open(path / ".." / "data" / "db.json", "w"))
    conn.commit()
    open(path / ".." / "data" / "error_logs.txt", "w").write(error_logs)
    del db

t = time.time()
pprint(cur.execute("""
                    SELECT name, metacritic, released FROM game
                    WHERE   released > "2010-01-01" AND
                            metacritic > 95
                    ORDER BY released DESC
                  """).fetchall())
print(f"That query took: {time.time() - t} seconds\n")

t = time.time()
pprint(cur.execute("""
                    SELECT g.name as games, GROUP_CONCAT(gen.name, ', ') as genres FROM 
                    genre gen NATURAL JOIN has_genre x
                    JOIN game g ON x.id_game == g.id_game
                    WHERE   g.metacritic > 80 AND
                            g.released > "2010/01/01" AND
                            g.released < "2024/01/01"
                    GROUP BY g.name
                  """).fetchall())
print(f"That query took: {time.time() - t} seconds\n")

t = time.time()
pprint(cur.execute("""
                    SELECT g.name, d.name, g.metacritic, g.released 
                    FROM game g NATURAL JOIN developed
                    JOIN company d ON d.id_company == developed.id_company
                    WHERE   g.metacritic > 85 AND
                            g.released > "2014/01/01" AND
                            d.name LIKE "CD%"
                  """).fetchall())
print(f"That query took: {time.time() - t} seconds\n")
conn.close()
