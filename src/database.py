import os, pickle
import pandas as pd
import sqlite3 as sql
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from pprint import pprint
from flask import g

####### GLOBAL VARIABLES ######
path = Path(os.path.abspath(os.path.dirname(__file__)))
db_path = path / ".." / "data" / "games.db"

####### FUNCTIONS #######
def get_db():
    if "db" not in g:
        g.db = sql.connect(db_path)
        g.db.row_factory = sql.Row
    return g.db

def close_db(e=None):
    db = g.pop("db", None)
    if db is not None:
        db.close()

def deserialize(compressed_data):
    return pickle.loads(compressed_data)

def load_pickle_file(path):
    return pickle.load(open(path, "rb"))

def load_boolean_vectors() -> dict[str, pd.DataFrame]:
    return load_pickle_file(path / ".." / "data" / "boolean_vectors")

def load_tfidf_vectors() -> pd.DataFrame:
    return load_pickle_file(path / ".." / "data" / "tf_idf")

def get_text_gamedata(limit = 0, as_text = False):
    cur = get_db()
    idgame_join_genres = """SELECT
                              x.id_game,
                              GROUP_CONCAT(gen.name, ', ') AS genres
                          FROM has_genre x
                          JOIN genre gen ON gen.id_genre = x.id_genre
                          GROUP BY x.id_game"""
    idgame_join_developers = """SELECT
                                d.id_game,
                                GROUP_CONCAT(c.name, ', ') AS developers
                            FROM developed d
                            JOIN company c ON c.id_company = d.id_company
                            GROUP BY d.id_game"""
    idgame_join_publishers = """SELECT
                                p.id_game,
                                GROUP_CONCAT(c.name, ', ') AS publishers
                            FROM published p
                            JOIN company c ON c.id_company = p.id_company
                            GROUP BY p.id_game"""
    q = f"""SELECT
            g.id_game as id_game,
            g.name as name,
            gen.genres as genres,
            g.description as description,
            dev.developers as developers,
            pub.publishers as publishers
            FROM game g JOIN ({idgame_join_genres}) gen ON gen.id_game == g.id_game
            JOIN ({idgame_join_developers}) dev ON dev.id_game == g.id_game
            JOIN ({idgame_join_publishers}) pub ON pub.id_game == g.id_game
            ORDER BY g.metacritic DESC"""
    if limit > 0:
        q += f" LIMIT {limit}"
    if as_text:
        return q
    else:
        return cur.execute(q).fetchall()
    
def get_all_gamedata(limit = 0, as_text = False, ids=None):
    cur = get_db()
    idgame_join_genres = """SELECT
                              x.id_game,
                              GROUP_CONCAT(gen.name, ', ') AS genres
                          FROM has_genre x
                          JOIN genre gen ON gen.id_genre = x.id_genre
                          GROUP BY x.id_game"""
    idgame_join_developers = """SELECT
                                d.id_game,
                                GROUP_CONCAT(c.name, ', ') AS developers
                            FROM developed d
                            JOIN company c ON c.id_company = d.id_company
                            GROUP BY d.id_game"""
    idgame_join_publishers = """SELECT
                                p.id_game,
                                GROUP_CONCAT(c.name, ', ') AS publishers
                            FROM published p
                            JOIN company c ON c.id_company = p.id_company
                            GROUP BY p.id_game"""
    q = f"""SELECT
            g.id_game as id_game,
            g.name as name,
            gen.genres as genres,
            g.description as description,
            dev.developers as developers,
            pub.publishers as publishers,
            g.image_url as image_url,
            g.website_url as website_url,
            g.units_sold as units_sold,
            g.rating as rating,
            g.ratings_count as ratings_count,
            g.metacritic as metacritic,
            g.released as release_date
            FROM game g JOIN ({idgame_join_genres}) gen ON gen.id_game == g.id_game
            JOIN ({idgame_join_developers}) dev ON dev.id_game == g.id_game
            JOIN ({idgame_join_publishers}) pub ON pub.id_game == g.id_game
            """
    if ids != None:
        q += f"WHERE g.id_game IN {tuple(map(str, ids))}"
    if limit > 0:
        q += f" LIMIT {limit}"
    if as_text:
        return q
    else:
        return cur.execute(q).fetchall()


if __name__=="__main__":
    pprint(get_all_gamedata(limit=10, ids=[28, 28, 2509]))