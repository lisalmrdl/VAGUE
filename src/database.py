import os, pickle
import pandas as pd
import sqlite3 as sql
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor

####### GLOBAL VARIABLES ######
path = Path(os.path.abspath(os.path.dirname(__file__)))
db_path = path / ".." / "data" / "games.db"
conn = sql.connect(db_path)
cur = conn.cursor()

####### FUNCTIONS #######
def deserialize(compressed_data):
    return pickle.loads(compressed_data)
    
def remake_boolean_matrix(db_cur=cur) -> pd.DataFrame:
    result = db_cur.execute("""
        SELECT id_game, embedding FROM boolean_vector
    """).fetchall()
    indices = [i[0] for i in result]
    with ThreadPoolExecutor(max_workers=os.cpu_count()) as exe:
        values = list(exe.map(deserialize, [i[1] for i in result]))
    field_boolean_matrix = {}
    for field in values[0].keys():
        list_fieldvectors = []
        for i in range(len(values)):
            list_fieldvectors.append(values[i][field])
        field_boolean_matrix[field] = pd.DataFrame(list_fieldvectors, index=indices, dtype=pd.SparseDtype("int8", 0))
    return field_boolean_matrix

def remake_tfidf_matrix(db_cur=cur) -> pd.DataFrame:
    result = db_cur.execute("""
        SELECT id_game, embedding FROM tfidf
    """).fetchall()
    indices = [i[0] for i in result]
    values = [deserialize(i[1]) for i in result]
    return pd.DataFrame(values, indices, dtype=pd.SparseDtype("float32", 0))

def get_text_gamedata(limit = 0, as_text = False):
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
    
def get_all_gamedata(limit = 0, as_text = False):
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
            g.image_url as image_url
            g.website_url as website_url
            g.units_sold as units_sold
            g.rating as rating
            g.ratings_count as ratings_count
            g.metacritic as metacritic
            g.released as release_date
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


if __name__=="__main__":
    data = remake_boolean_matrix()
    for k, v in data.items():
        print(k)
        print(v.head())
        print(v.index)