

"""
TF-IDF ranked search engine (cosine similarity) + Boolean search (original by Lisa).

This script loads db.json, builds:
  1) Boolean search indices using CountVectorizer (binary presence)
  2) TF-IDF matrices using TfidfVectorizer (ranked retrieval)

Query behavior:
  - If the query contains Boolean operators (AND/OR/NOT), Boolean search is used.
  - Otherwise TF-IDF ranked search is used.

Supported query formats:
  - Boolean: "genres:adventure AND description_raw:majora"
  - TF-IDF:  "genres:adventure description_raw:majora" or "majora adventure"
"""

import json
import re
import numpy as np

from sklearn.feature_extraction.text import CountVectorizer, TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity


# ----------------------------
# 1) Load data from db.json
# ----------------------------
with open("db.json", encoding="utf-8") as f:
    data = json.load(f)

games = data["games"]

# Fields that will be indexed/searched
FIELDS = ["name", "genres", "developers", "publishers", "description_raw"]

# Create one "document" per game_id per field (same as in the Boolean version)
field_docs = {f: [] for f in FIELDS}
doc_ids = []

for game_id, game in games.items():
    doc_ids.append(game_id)
    for f in FIELDS:
        field_docs[f].append(str(game.get(f, "")))


# -----------------------------------------
# 2) Boolean search index (by Lisa)
# -----------------------------------------
vectorizers = {}
td_matrices = {}
vocabularies = {}

# Build binary term-document matrices per field (transposed for fast term lookup)
for f in FIELDS:
    cv = CountVectorizer(lowercase=True, binary=True, token_pattern=r"\b\w+\b")
    X = cv.fit_transform(field_docs[f])      # shape: (n_docs, n_terms)
    vectorizers[f] = cv
    td_matrices[f] = X.T.tocsr()             # shape: (n_terms, n_docs)
    vocabularies[f] = cv.vocabulary_         # term -> term_index


# Logical operators for Boolean rewriting
LOGIC = {
    "AND": "&", "and": "&",
    "OR": "|", "or": "|",
    "NOT": "1 -", "not": "1 -",
    "(": "(", ")": ")"
}


def rewrite_token(token: str) -> str:
    """
    Rewrite one token of the Boolean query into a Python expression that will be eval()'d.
    - Supports logic operators AND/OR/NOT and parentheses.
    - Supports fielded queries: field:term
    - Supports unfielded terms: term (OR across all fields)
    """
    # Logical operator
    if token in LOGIC:
        return LOGIC[token]

    # Field-specific search: field:term
    if ":" in token:
        field, term = token.split(":", 1)

        # Unknown field
        if field not in vocabularies:
            return "0"

        # Term not in that field's vocabulary
        if term not in vocabularies[field]:
            return "0"

        # Binary vector for that term across documents
        return f"td_matrices['{field}'][vocabularies['{field}']['{term}']].todense()"

    # Unfielded term: OR across all fields where the term exists
    expr = []
    for f in FIELDS:
        if token in vocabularies[f]:
            expr.append(f"td_matrices['{f}'][vocabularies['{f}']['{token}']].todense()")

    # Term absent everywhere
    if not expr:
        return "0"

    return "(" + " | ".join(expr) + ")"


def rewrite_query(query: str) -> str:
    """
    Rewrite the Boolean query into a Python expression string.
    """
    return " ".join(rewrite_token(t) for t in query.split())


def boolean_search(query: str, top_k: int = 10):
    """
    Run Boolean search and return a list of (game_id, score).
    Boolean search has no relevance ranking, so the score is always 1.0.
    """
    hits_matrix = eval(rewrite_query(query))
    hits = hits_matrix.nonzero()[1]  # document indices

    results = []
    for i in hits[:top_k]:
        game_id = doc_ids[i]
        results.append((game_id, 1.0))
    return results


# -----------------------------------------
# 3) TF-IDF ranked search (cosine ranking)
# -----------------------------------------
tfidf_vectorizers = {}
tfidf_doc_matrices = {}

for f in FIELDS:
    tv = TfidfVectorizer(
        lowercase=True,
        token_pattern=r"\b\w+\b",
        ngram_range=(1, 2)  # optional: phrase-like matching
    )
    D = tv.fit_transform(field_docs[f])  # shape: (n_docs, n_terms)
    tfidf_vectorizers[f] = tv
    tfidf_doc_matrices[f] = D


def tfidf_search(query: str, top_k: int = 10):
    """
    Ranked TF-IDF search using cosine similarity.

    Supported query format:
      - Free text: "majora adventure"
      - Field-specific terms: "genres:adventure description_raw:majora"

    Boolean operators (AND/OR/NOT) are NOT interpreted here.
    """
    query = query.strip()
    if not query:
        return []

    # Separate field-specific terms from global terms
    field_terms = {f: [] for f in FIELDS}
    global_terms = []

    for token in query.split():
        if ":" in token:
            field, term = token.split(":", 1)
            if field in field_terms and term:
                field_terms[field].append(term)
            else:
                # Unknown field -> treat as a global term
                global_terms.append(token)
        else:
            global_terms.append(token)

    # Accumulate similarity scores across fields
    scores = np.zeros(len(doc_ids), dtype=float)

    # 1) Field-specific parts
    for f in FIELDS:
        if field_terms[f]:
            q_text = " ".join(field_terms[f])
            q_vec = tfidf_vectorizers[f].transform([q_text])
            sims = cosine_similarity(q_vec, tfidf_doc_matrices[f])[0]
            scores += sims

    # 2) Global terms: run against all fields
    if global_terms:
        q_text = " ".join(global_terms)
        for f in FIELDS:
            q_vec = tfidf_vectorizers[f].transform([q_text])
            sims = cosine_similarity(q_vec, tfidf_doc_matrices[f])[0]
            scores += sims

    # No matches
    if scores.max() <= 0:
        return []

    # Rank by descending score
    ranked_indices = np.argsort(-scores)

    results = []
    for i in ranked_indices[:top_k]:
        if scores[i] <= 0:
            break
        game_id = doc_ids[i]
        results.append((game_id, float(scores[i])))

    return results


# -----------------------------------------
# 4) Auto-select Boolean vs TF-IDF
# -----------------------------------------
BOOL_RE = re.compile(r"\b(AND|OR|NOT)\b", re.IGNORECASE)


def run_query(query: str, top_k: int = 10):
    """
    Run Boolean search if logical operators are present; otherwise run TF-IDF ranking.
    """
    if BOOL_RE.search(query):
        return boolean_search(query, top_k=top_k)
    return tfidf_search(query, top_k=top_k)


# -----------------------------------------
# 5) Simple CLI loop
# -----------------------------------------
if __name__ == "__main__":
    print("Search engine ready.")
    print("Boolean example: genres:adventure AND description_raw:majora")
    print("TF-IDF example:  genres:adventure description_raw:majora")
    print("Type 'quit' to exit.\n")

    while True:
        query = input("Query> ").strip()
        if query.lower() in {"quit", "exit"}:
            break

        results = run_query(query, top_k=10)

        if not results:
            print("No results.\n")
            continue

        for rank, (game_id, score) in enumerate(results, start=1):
            name = games.get(game_id, {}).get("name", "<unknown>")
            print(f"{rank}. {name} (ID: {game_id}) score={score:.4f}")
        print()
