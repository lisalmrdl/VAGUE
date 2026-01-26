
#importing all the libraries
import json, re, shlex
import numpy as np
from sklearn.feature_extraction.text import CountVectorizer

########## DEFINITION OF GLOBAL VARIABLES ##########

#adding the logic operators for the search
LOGIC = {
    "AND": "&", "and": "&",
    "OR": "|", "or": "|",
    "NOT": "1 -", "not": "1 -",
    "(": "(", ")": ")",
    "@1@" : "1", "@True@": 1
}

#defining the different categories
FIELDS = ["name", "genres", "developers", "publishers", "description_raw"]

########## DEFINITION OF FUNCTIONS ##########

#definition of a request token
def rewrite_token(token):
    #for a logic operator
    if token in LOGIC:
        return LOGIC[token]

    #for a field search
    if ":" in token:
        field, term = token.split(":", 1)
        #if the field doesn't exist
        if field not in vocabularies:
            return "0"
        
        if term[0] == '"' and term[-1] == '"':
            term = term.replace("\"", "")
            return f"np.array({[[1 if term in games[k][field].lower() else 0 for k in games.keys()]]})"

        #if the field doesn't contain the word
        if term not in vocabularies[field]:
            return "0"

        #binary vector
        return f"td_matrices['{field}'][vocabularies['{field}']['{term}']].todense()"

    #search without a field
    expr = []
    for f in FIELDS:
        if token in vocabularies[f]:
            expr.append(
                f"td_matrices['{f}'][vocabularies['{f}']['{token}']].todense()"
            )
        elif token[0] == '"' and token[-1] == '"':
            token = token.replace("\"", "")
            l = []
            for k in games.keys():
                full_tx = " ".join(list(map(str, games[k].values()))).lower()
                l.append(1 if token in full_tx else 0)
            return f"np.array({[l]})"
            
    #absent term in the whole database
    if not expr:
        return "0"

    return "(" + " | ".join(expr) + ")"

#definition of the rewritten request
def rewrite_query(query):
    split_query = re.findall(r"(?:\w+:)?\"[^\"]*\"|\S+", query)
    return " ".join(rewrite_token(t) for t in split_query)

def query(query):
    #finding a corresponding game
    hits_matrix = eval(rewrite_query(query))
    hits = hits_matrix.nonzero()[1]
    for rank, i in enumerate(hits, start=1):
            game_id = doc_ids[i]
            print(f"{rank}. {games[game_id]['name']} (ID: {game_id})")

################ MAIN LOGIC ###############

#preparation of the json data
with open("data/db.json", encoding="utf-8") as f:
    data = json.load(f)

#creation of a dictionnary for our data
games = data["games"]

#creation of a document for each game id
field_docs = {f: [] for f in FIELDS}
doc_ids = []

#filling up the documents with our data
for game_id, game in games.items():
    doc_ids.append(game_id)
    for f in FIELDS:
        field_docs[f].append(str(game.get(f, "")))

#necessary dictionnaries for the request evaluation
vectorizers = {}
td_matrices = {}
vocabularies = {}

#integration of the boolean search, transformation of the documents in matrix
for f in FIELDS:
    cv = CountVectorizer(lowercase=True, binary=True, token_pattern=r"\b\w+\b")
    X = cv.fit_transform(field_docs[f])
    vectorizers[f] = cv
    td_matrices[f] = X.T.tocsr()
    vocabularies[f] = cv.vocabulary_

#answering the search query
q = """genres:"shoot" AND name:"bad company" """
q2 = """genres:shooter AND name:"bad company" """
query(q)



