import os, json, zlib
import pandas as pd
from io import StringIO
import sqlite3 as sql
from pathlib import Path
from pprint import pprint
from sklearn.feature_extraction.text import CountVectorizer

##### GLOBAL VARIABLES ######
path = Path(os.path.abspath(os.path.dirname(__file__)))
db_path = path / ".." / "data" / "games.db"
conn = sql.connect(db_path)
cur = conn.cursor()

def decompress_tuple(compressed_data):
    decompressed = json.loads(zlib.decompress(compressed_data).decode("utf-8"))
    for k in decompressed.keys():
        decompressed[k] = pd.read_json(StringIO(decompressed[k]), orient="index", typ="series")
    return decompressed
    
def remake_boolean_matrix(db_cur=cur):
    result = db_cur.execute("""
        SELECT id_game, embedding FROM boolean_vector
    """).fetchall()
    indices = [i[0] for i in result]
    values = [decompress_tuple(i[1]) for i in result]
    field_boolean_matrix = {}
    for field in values[0].keys():
        list_fieldvectors = []
        for i in range(len(values)):
            list_fieldvectors.append(values[i][field])
        field_boolean_matrix[field] = pd.DataFrame(list_fieldvectors, index=indices)
    return field_boolean_matrix

if __name__=="__main__":
    data = remake_boolean_matrix()
    for k, v in data.items():
        print(k)
        print(v.head())
        print(v.index)