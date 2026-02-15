import pandas as pd
import numpy as np
import re
from sklearn.metrics.pairwise import cosine_similarity
import sys
import os
import src.database as db
from sklearn.feature_extraction.text import TfidfVectorizer

######## GLOBAL VARIABLES #########

#adding it for the stemming
try:
    from nltk.stem import SnowballStemmer
    stemmer = SnowballStemmer("english")
except ImportError:
    print("Missing NLTK")
    stemmer = None
    
#loading the different data
print("Vectors loading...")

#tf-idf
tfidf_matrix = db.load_tfidf_vectors()
#doing the list of words for the wildcard search
vocab_cols = pd.Series(tfidf_matrix.columns) 

#boolean
boolean_dict = db.load_boolean_vectors()
boolean_dict["any"] = (tfidf_matrix != 0).astype('int8') # NOTE: Temporary fix, turn everything that is not 0 into one.
boolean_unified = boolean_dict["any"] # NOTE: careful, this is a pointer

######## FUNCTIONS #######

def show_results(game_ids):
    if not game_ids:
        print("No game found.")
        return

    #getting back the data
    rows = db.get_all_gamedata(ids=game_ids)
    
    #converting in dataframe
    cols = ["id", "name", "genres", "desc", "devs", "pubs", "img", "web", "sold", "rating", "count", "meta", "date"]
    df_res = pd.DataFrame(rows, columns=cols)
    print(df_res.head())
    #only keeping the "interesting" informations
    display_cols = ["name", "genres", "date", "desc", "meta", "img", "rating", "devs", "pubs"]
    
    #reorganizing the dataframe per ids
    df_res = df_res.set_index("id").reindex(game_ids) # NOTE Why was the index being reset?!
    print(df_res.head())
    
    return df_res[display_cols]

def smart_search_router(query: str, top_k: int | None = None):
    #text for the exact match
    all_data = pd.read_sql(db.get_text_gamedata(as_text=True), db.get_db()) # NOTE: No need to load de data yet. We can first compute the search. DELETE
    if not all_data.empty:
        search_text_ref = all_data.set_index('id_game')['description'].fillna("").str.lower()
    else:
        search_text_ref = pd.Series()
    #boolean detection
    is_logic = bool(re.search(r'\b(AND|OR|NOT)\b', query))
    
    results = []
    mode = ""

    if is_logic:
        mode = "BOOLEAN (Strict)"
        results = search_boolean(query, boolean_unified)
        
        if results is None or len(results) == 0:
            print(f"Nothing found, switching up to TF-IDF")
            mode = "TF-IDF (Fallback)"
            results = search_tfidf(query, tfidf_matrix, search_text_ref, top_k=None)
            
    else:
        mode = "TF-IDF"
        results = search_tfidf(query, tfidf_matrix, search_text_ref, top_k=None)
    
    print(f"Mode : {mode} | Résultats : {len(results)}")
    return results

def parse_query(query: str):
#extracting the exact sentence depending on the typing
    #in quote text
    phrases = re.findall(r'"([^"]*)"', query)
    
    #putting aside the quotes for the rest of the query
    remaining = re.sub(r'"[^"]*"', '', query)
    
    #cleaning of the query (except for *)
    tokens = [t for t in remaining.lower().split() if t.strip()]
    
    return phrases, tokens

def expand_token(token: str, vocab: pd.Series):
#wildcard and stemming
    #Explicite wildcard
    if '*' in token:
        clean_pattern = token.replace('*', '.*') # Regex simple
        matches = vocab[vocab.str.match(f"^{clean_pattern}$", case=False)]
        return matches.tolist()
    
    #exact word found
    if token in vocab.values:
        return [token]
    
    #stemming if the word is not found
    if stemmer:
        stem = stemmer.stem(token)
        if stem in vocab.values:
            return [stem]

    return []

#TF-IDF (by default)
def search_tfidf(query: str, matrix: pd.DataFrame, text_ref: pd.Series, top_k: int | None = None):
    #parsing
    phrases, raw_tokens = parse_query(query)
    
    #for the treatment of the sentences
    for p in phrases:
        raw_tokens.extend(p.lower().split())
        
    if not raw_tokens:
        return []
    
    vectorizer = TfidfVectorizer(
        lowercase=True,
        token_pattern=r"\b\w+\b",
        dtype=np.float32,
        min_df=5,
        max_df=0.8,
        vocabulary=matrix.columns,
        ngram_range=(1, 1)
    )
    query_vec = np.asarray(vectorizer.fit_transform([query]).todense())
    #cosinus calculus
    similarities = cosine_similarity(query_vec, matrix)
    scores = pd.Series(similarities[0], index=matrix.index)
    
    #main changes here to delete the 10 limit
    sorted_scores = scores.sort_values(ascending=False)

    if top_k is None:
        candidates = sorted_scores
    else:
        candidates = sorted_scores.head(top_k * 5)

    
    #calling the exact words seen above
    if phrases and not candidates.empty:
        candidate_texts = text_ref.loc[candidates.index]
        
        #filter for every sentence
        for phrase in phrases:
            phrase_clean = phrase.lower()
            #only keeping the lines with the sentence
            candidate_texts = candidate_texts[candidate_texts.str.contains(phrase_clean, regex=False)]
            
        #update of the final candidates
        final_ids = candidate_texts.index.tolist()
        
        #sending back the candidates on order of the filter
        if top_k is None:
            return candidates.loc[final_ids].index.tolist()
        else:
            return candidates.loc[final_ids].head(top_k).index.tolist()


    if top_k is None:
        return candidates.index.tolist()
    else:
        return candidates.head(top_k).index.tolist()



#BOOLEAN (Strict logic)
def search_boolean(query: str, matrix: pd.DataFrame):
#only with a logic request and, or, not
    #cleaning and transition sql to panda
    trans = query.replace(" AND ", " & ").replace(" OR ", " | ").replace(" NOT ", " ~").replace("(", " ( ").replace(")", " ) ")
    
    q_list = [i for i in trans.split() if i != ""]
    for i in range(len(q_list)): # Parse the query to apply it over the Dataframe columns
        neg = q_list[i][0] == "~"
        if q_list[i] not in ("&", "|", "(", ")"):
            if neg:
                q_list[i] = f"~matrix['{q_list[i][1:]}']"
            else:
                q_list[i] = f"matrix['{q_list[i]}']"                
    
    #need to put everything on low cases
    trans = " ".join(q_list).lower()
    try:
        #evaluation of the dataframe
        mask = eval(trans)
        #True/False
        results = mask[mask==1].index.tolist()
        return results
        
    except Exception as e:
        #if the word doesn't exist
        print(f"DEBUG: Echec Boolean ({e})") 
        return None
    
############## MAIN LOGIC ################
if __name__=="__main__":
    print("--- TEST 2 : Boolean ---")
    q2 = "rpg OR action" 
    ids2 = smart_search_router(q2)
    print(show_results(ids2))

    print(boolean_dict.keys())
    print("--- TEST 2 : Boolean ---")
    q2 = "rpg AND (action OR adventure) AND NOT shooter" 
    ids2 = smart_search_router(q2)
    print(show_results(ids2))

    print("--- TEST 1 : TF-IDF ---")
    q1 = "open world rpg with magic"
    ids1 = smart_search_router(q1)
    print(show_results(ids1))

    print("\n" + "="*50 + "\n")

    print("\n" + "="*50 + "\n")

    print("--- TEST 2432 : TF-IDF ---")
    q1 = "Wester game about outlaws on the run with bounty hunters of the nation massing on their heels red dead redemption"
    ids1 = smart_search_router(q1)
    print(show_results(ids1))

    print("\n" + "="*50 + "\n")

    print("\n" + "="*50 + "\n")

    print("--- TEST 3 : Exact sentence ---")
    q1 = '"dark fantasy" rpg' 
    ids1 = smart_search_router(q1)
    print(show_results(ids1))

    print("\n" + "="*50 + "\n")

    print("--- TEST 4 : Wildcard ---")
    q2 = "redempt*" 
    ids2 = smart_search_router(q2)
    print(show_results(ids2))
