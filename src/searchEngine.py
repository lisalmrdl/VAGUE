import pandas as pd
import numpy as np
import re
from sklearn.metrics.pairwise import cosine_similarity
import sys
import os
import src.database as db
from sklearn.feature_extraction.text import TfidfVectorizer
from sentence_transformers import SentenceTransformer

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
#creating the list of words for the wildcard search
vocab_cols = pd.Series(tfidf_matrix.columns) 

#boolean
boolean_dict = db.load_boolean_vectors()
boolean_dict["any"] = (tfidf_matrix != 0).astype('int8') # NOTE: Temporary fix, turn everything that is not 0 into one.
boolean_unified = boolean_dict["any"] # NOTE: careful, this is a pointer

#neural
neural_matrix = db.load_neural_embeddings()
neural_model = SentenceTransformer("sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2")

######## FUNCTIONS #######

def show_results(game_ids):
    """
    Retrieves and displays game data for a given list of game IDs.

    Args:
        game_ids (list): A list of integer game IDs to retrieve.

    Returns:
        pd.DataFrame: A DataFrame containing the formatted game details 
        (name, genres, date, desc, meta, img, rating, devs, pubs), or None if no games are found.
    """
    if not game_ids:
        print("No game found.")
        return

    #getting back the data
    rows = db.get_all_gamedata(ids=game_ids)
    
    #converting in dataframe
    cols = ["id", "name", "genres", "desc", "devs", "pubs", "img", "web", "sold", "rating", "count", "meta", "date"]
    df_res = pd.DataFrame(rows, columns=cols)
    
    #only keeping the "interesting" informations
    display_cols = ["name", "genres", "date", "desc", "meta", "img", "rating", "devs", "pubs"]
    
    #reorganizing the dataframe per ids
    df_res = df_res.set_index("id").reindex(game_ids) # NOTE Why was the index being reset?!
    
    return df_res[display_cols]

def weighted_similarity(query_vec, matrix, sort=True, weights=[0.8, 0.1, 0.1]):
    """
    Calculates the cosine similarity between a query vector and a matrix, 
    and applies custom weights based on similarity, rating, and rating count.

    Args:
        query_vec (np.ndarray): The vectorized search query.
        matrix (pd.DataFrame): The matrix of game embeddings/vectors to compare against.
        sort (bool, optional): Whether to sort the results. Defaults to True.
        weights (list, optional): Weights applied to [similarity_score, rating, ratings_count]. 
                                  Defaults to [0.8, 0.1, 0.1].

    Returns:
        pd.Series: A pandas Series of weighted scores, indexed by game IDs, sorted descending.
    """
    similarities = cosine_similarity(query_vec, matrix.values)[0]
    # 4. Results
    w = db.get_similarity_weights(matrix.index)
    weigthed_scores = (weights[0] * pd.Series(similarities, index=matrix.index)) + (weights[1] * w["rating"]) + (weights[2] * w["ratings_count"])
    return weigthed_scores.sort_values(ascending=False)

def smart_search_router(query: str, literal_search: bool, top_k: int = 100):
    """
    Routes the search query to the appropriate search algorithm based on user parameters 
    and query syntax (Boolean, TF-IDF, or Neural).

    Args:
        query (str): The search query string.
        literal_search (bool): If True, forces Boolean or TF-IDF. If False, uses Neural search.
        top_k (int, optional): The maximum number of results to return. Defaults to 100.

    Returns:
        list: A list of matched game IDs based on the selected search strategy.
    """
    #text for the exact match
    all_data = pd.read_sql(db.get_text_gamedata(as_text=True), db.get_db()) # NOTE: No need to load de data yet. We can first compute the search. DELETE
    if not all_data.empty:
        all_data_indexed = all_data.set_index('id_game')
        titles = all_data_indexed['name'].fillna("")
        descriptions = all_data_indexed['description'].fillna("")
        search_text_ref = (titles + " " + descriptions).str.lower()
    else:
        search_text_ref = pd.Series()

    results = []
    mode = ""

    if literal_search:
        #boolean detection
        # TODO since the search is now decided via toggle, do we want to read lowercase operators as boolean too?
        is_logic = bool(re.search(r'\b(AND|OR|NOT)\b', query))

        if is_logic:
            mode = "BOOLEAN (Strict)"
            results = search_boolean(query, boolean_unified, top_k)
        
            if results is None or len(results) == 0:
                print(f"Nothing found, switching up to TF-IDF")
                mode = "TF-IDF (Fallback)"
                results = search_tfidf(query, tfidf_matrix, search_text_ref, top_k)
            
        else:
            mode = "TF-IDF"
            results = search_tfidf(query, tfidf_matrix, search_text_ref, top_k)
    else:
        mode = "NEURAL"
        results = search_neural(query, neural_matrix, top_k)
    
    print(f"Mode : {mode} | Résultats : {len(results)}")
    return results

def parse_query(query: str):
    """
    Parses a search query to extract exact sentence matches and individual tokens.

    Args:
        query (str): The raw search query.

    Returns:
        tuple: A tuple containing:
            - phrases (list): A list of exact match phrases found within quotes.
            - tokens (list): A list of cleaned, individual word tokens (lowercased).
    """
    #extracting the exact sentence depending on the typing
    #in quote text
    phrases = re.findall(r'"([^"]*)"', query)
    
    #putting aside the quotes for the rest of the query
    remaining = re.sub(r'"[^"]*"', '', query)
    
    #cleaning of the query (except for *)
    tokens = [t for t in remaining.lower().split() if t.strip()]
    
    return phrases, tokens

def expand_token(token: str, vocab: pd.Series):
    """
    Expands a search token by resolving wildcards or applying stemming 
    against a known vocabulary.

    Args:
        token (str): The individual word token to expand.
        vocab (pd.Series): A pandas Series containing the recognized vocabulary.

    Returns:
        list: A list of matched vocabulary words.
    """
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

# Neural Search (testing)
def search_neural(query: str, matrix: pd.DataFrame, top_k: int = 100):
"""
    Executes a semantic search using neural network embeddings.

    Args:
        query (str): The raw search query.
        matrix (pd.DataFrame): The database matrix of neural embeddings.
        top_k (int, optional): The maximum number of results to return. Defaults to 100.

    Returns:
        list: A list of matching game IDs sorted by weighted similarity.
    """
    # TODO Parse query to handle exact matches, skipped for now. Currently encodes the entire query
    # phrases, raw_tokens = parse_query(query)

    # 2. Encode query
    query_vec = neural_model.encode([query])[0]
    # 3. Compare embeddings
    sorted_scores = weighted_similarity([query_vec], matrix)

    # for game_id, score in sorted_scores.head(5).items():
    #     print(f"ID: {game_id} | Score: {score:.4f}")
        
    if top_k is None:
        return sorted_scores.index.tolist()
    else:
        return sorted_scores.head(top_k).index.tolist()

#TF-IDF (by default)
def search_tfidf(query: str, matrix: pd.DataFrame, text_ref: pd.Series, top_k: int = 100):
    """
    Executes a keyword-based search using Term Frequency-Inverse Document Frequency (TF-IDF).
    Also filters results to enforce exact phrase matches if quotes are used.

    Args:
        query (str): The search query.
        matrix (pd.DataFrame): The pre-computed TF-IDF matrix.
        text_ref (pd.Series): A pandas Series containing the reference text (e.g., descriptions) 
                              to check exact phrase matches against.
        top_k (int, optional): The maximum number of results to return. Defaults to 100.

    Returns:
        list: A list of matching game IDs sorted by relevance.
    """
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
    sorted_scores = weighted_similarity(query_vec, matrix)

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
def search_boolean(query: str, matrix: pd.DataFrame, top_k: int = 100):
    """
    Executes a strict Boolean search evaluating AND, OR, and NOT logical operators.

    Args:
        query (str): The search query containing boolean operators.
        matrix (pd.DataFrame): The boolean matrix mapping words to document presence.
        top_k (int, optional): The maximum number of results to return. Defaults to 100.

    Returns:
        list or None: A list of matching game IDs, or None if the query parsing fails.
    """
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
        if top_k is None:
            return results()
        else:
            return results(top_k).index.tolist()
        
    except Exception as e:
        #if the word doesn't exist
        print(f"DEBUG: Echec Boolean ({e})") 
        return None
    
