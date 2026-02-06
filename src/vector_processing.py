import os, time, pickle
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
import pandas as pd
import numpy as np
import sqlite3 as sql
from pathlib import Path
from sklearn.feature_extraction.text import CountVectorizer, TfidfVectorizer
import database as db

def create_boolean_vectors():
    pass

def create_tfidf_vectors():
    pass

def batch_insert(tablename: str ,db_cur:sql.Cursor, db_conn:sql.Connection, tuples:list[tuple]):
    #conn.execute("BEGIN")
    cur.executemany(f"""
    INSERT INTO {tablename} (id_game, embedding)
    VALUES (?, ?)
    """, tuples)


if input("\033[31mWARNING: This action will remove and reset the vectors from the database. Do you REALLY want to do it?(yes/no)\033[0m") != "yes":
    raise(KeyboardInterrupt("Execution was manually interrupted."))

path = Path(os.path.abspath(os.path.dirname(__file__)))
db_path = path / ".." / "data" / "games.db"
json_path = path / ".." / "data" / "db.json"
conn = db.conn  #sql.connect(db_path)
cur = db.cur    #conn.cursor()
data_path = path / ".." / "data"

cur.execute("BEGIN") # Begin transaction

data = pd.read_sql(db.get_text_gamedata(as_text=True), conn, index_col="id_game")

####### BOOLEAN VECTORS ###########
vector_matrices = {}
# Build binary term-document matrices per field
for col in data.columns:
    cv = CountVectorizer(
        lowercase=True, 
        binary=True, 
        token_pattern=r"\b\w+\b",
        min_df=10,
        max_df=0.9,
        dtype=np.int8)
    X = cv.fit_transform(data[col])
    vector_matrices[col] = pd.DataFrame.sparse.from_spmatrix(
                X, 
                index=data.index,
                columns=cv.get_feature_names_out()
                )
pickle.dump(vector_matrices, open(data_path / "boolean_vectors", "wb"))

####### TF-IDF VECTORS ###########
docs = pd.Series([". ".join(data.loc[i]) for i in data.index], index=data.index)
vectorizer = TfidfVectorizer(
        lowercase=True,
        token_pattern=r"\b\w+\b",
        dtype=np.float32,
        min_df=5,
        max_df=0.8,
        ngram_range=(1, 1)  # using ngrams will make vectors huuuuuge
    )
X = vectorizer.fit_transform(docs)
tfidf_matrix = pd.DataFrame.sparse.from_spmatrix(
                    X, 
                    index=docs.index, 
                    columns=vectorizer.get_feature_names_out()
                    )
pickle.dump(tfidf_matrix, open(data_path / "tf_idf", "wb"))

print("tf-idf vectors done")

####### TESTING #######
t = time.time()
data = db.load_pickle_file(data_path / "boolean_vectors")
print(f"decompressing and remaking boolean vector matrix took {time.time()-t} seconds")
print("Sanity check")
for i in np.random.choice(data[list(data.keys())[0]].index, size=5, replace=False):
    for field in vector_matrices.keys():
        truth = vector_matrices[field].loc[i]
        remade = data[field].loc[i]
        if not truth.eq(remade).all():
            raise ValueError(f"Original vectors don't match with vectors from the database in id {i} field {field}.")
        if not truth.equals(remade):
            print(f"\nFor id: {i}")
            print(field.upper())
            print("dtype:", truth.dtype, remade.dtype)
            print("index equal:", truth.index.equals(remade.index))
            print("name equal:", truth.name == remade.name)
            print("values equal:", np.all(truth.values == remade.values))
            print("Same class?", truth.__class__ == remade.__class__)

# tf-idf testing
t = time.time()
data = db.load_pickle_file(data_path / "tf_idf")
print(f"decompressing and remaking tf-idf vector matrix took {time.time()-t} seconds")
print(data.head())
print("Sanity check")
for i in np.random.choice(data.index, size=5, replace=False):
    print(f"\nFor id: {i}")
    truth = tfidf_matrix.loc[i]
    remade = data.loc[i]
    for idx in remade.index:
        if truth[idx] != remade[idx]:
            print(f"Mismatch in {i} in token {idx}. Should be {truth[idx]} but is {remade[idx]}")
    if not truth.equals(remade):
        print("dtype:", truth.dtype, remade.dtype)
        print("index equal:", truth.index.equals(remade.index))
        print("name equal:", truth.name == remade.name)
        print("values equal:", np.all(truth.values == remade.values))
        print("Same class?", truth.__class__ == remade.__class__)

conn.commit()
conn.close()