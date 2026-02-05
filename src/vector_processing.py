import os, json, zlib
import pandas as pd
import numpy as np
import sqlite3 as sql
from pathlib import Path
from sklearn.feature_extraction.text import CountVectorizer
import database as db

def create_boolean_vectors():
    pass

def create_tfidf_vectors():
    pass

def batch_insert(db_cur:sql.Cursor, db_conn:sql.Connection, tuples:list[tuple]):
    #conn.execute("BEGIN")
    cur.executemany("""
    INSERT INTO boolean_vector (id_game, embedding)
    VALUES (?, ?)
    """, tuples)


"""if input("\033[31mWARNING: This action will remove and reset the vectors from the database. Do you REALLY want to do it?(yes/no)\033[0m") != "yes":
    raise(KeyboardInterrupt("Execution was manually interrupted."))"""

path = Path(os.path.abspath(os.path.dirname(__file__)))
db_path = path / ".." / "data" / "games.db"
json_path = path / ".." / "data" / "db.json"
conn = db.conn  #sql.connect(db_path)
cur = db.cur    #conn.cursor()

cur.execute("BEGIN") # Begin transaction

# Remove boolean table table
cur.execute("""
    DROP TABLE IF EXISTS boolean_vector
            """)

# Remove tf-idf table table
cur.execute("""
    DROP TABLE IF EXISTS tf_idf
            """)

# (re)create boolean table table and tfidf
cur.execute("""
    CREATE TABLE boolean_vector (
    id_game INT PRIMARY KEY,
    embedding BLOB,
    FOREIGN KEY (id_game) REFERENCES game(id_game) DEFERRABLE INITIALLY DEFERRED
    )
            """)
cur.execute("""
    CREATE TABLE tfidf (
    id_game INT PRIMARY KEY,
    embedding BLOB,
    FOREIGN KEY (id_game) REFERENCES game(id_game) DEFERRABLE INITIALLY DEFERRED
    )
            """)

sql_query = """SELECT 
                    g.id_game as id_game,
                    g.name as name,
                    GROUP_CONCAT(gen.name, ', ') as genres,
                    g.description,
                    GROUP_CONCAT(d.name, ', ') as developers,
                    GROUP_CONCAT(p.name, ', ') as publishers
                    FROM game g NATURAL JOIN developed
                    JOIN company d ON d.id_company == developed.id_company
                    JOIN has_genre x ON x.id_game == g.id_game
                    JOIN genre gen ON gen.id_genre == x.id_genre
                    JOIN published y ON y.id_game == g.id_game
                    JOIN company p ON p.id_company == y.id_company
                    GROUP BY g.id_game
                    ORDER BY g.metacritic DESC"""

data = pd.read_sql(sql_query, conn, index_col="id_game")
vector_matrices = {}

# Build binary term-document matrices per field
for col in data.columns:
    cv = CountVectorizer(lowercase=True, binary=True, token_pattern=r"\b\w+\b")
    X = cv.fit_transform(data[col])
    vector_matrices[col] = pd.DataFrame.sparse.from_spmatrix(
                X, 
                index=data.index, 
                columns=cv.get_feature_names_out()
                )
insert_data = {}

for id in range(5):
    insert_data[data.index[id]] = {}
    for field in vector_matrices.keys():
        insert_data[data.index[id]][field] = vector_matrices[field].loc[data.index[id]].to_json(orient="index")
    insert_data[data.index[id]] = zlib.compress(json.dumps(insert_data[data.index[id]]).encode("utf-8"))

tuples = [(int(k), v) for k, v in insert_data.items()]

batch_insert(cur, conn, tuples)



####### TESTING #######
result = cur.execute("""
    SELECT id_game, embedding FROM boolean_vector
""").fetchall()
data = db.remake_boolean_matrix(cur)
print("Sanity check")
for i in np.random.choice(data[list(data.keys())[0]].index, size=5, replace=False):
    print(f"\nFor id: {i}")
    for field in vector_matrices.keys():
        print(field.upper())
        truth = vector_matrices[field].loc[i]
        remade = data[field].loc[i]
        if not truth.eq(remade).all():
            raise ValueError("Original vectors don't match with vectors from the database.")
        if not truth.equals(remade):
            print("dtype:", truth.dtype, remade.dtype)
            print("index equal:", truth.index.equals(remade.index))
            print("name equal:", truth.name == remade.name)
            print("values equal:", np.all(truth.values == remade.values))
            print("Same class?", truth.__class__ == remade.__class__)

raise(ValueError)
#conn.commit()
conn.close()