from flask import Flask, render_template, request
import src.searchEngine as engine
from src.database import close_db, plot_ratings, plot_genre_pie
from pprint import pprint
import markdown
import re

# TODO: maybe change this (and quick_preprocess)
# Very simple preprocessing, the naive "take the first 1000 chars from the description works fine and the results are as expected, but I thought some basic text preprocessing might make it better
PUNCT_RE = re.compile(r"[^\w\s]")
STOPWORDS = {"the","and","a","an","of","in","on","to","at","from"}  # minimal set of stopwords for basic text preprocessing

app = Flask(__name__)

def query_backend(query, mode):
    """Wrapper for sending query to search router. It is used for search results and game recommendations. I moved it up here to make it easier to fix later.

    Args:
        query (str): the query to send to search router
        mode (bool): True for Literal search, False for Neural search

    Returns:
        A list of dicts
    """

    df_results = engine.show_results(engine.smart_search_router(query, mode))  # db version

    if df_results is None or df_results.empty:
        return {}
    # TODO: change this, I am going to do something very ugly and inefficient
    results = []
    for i in df_results.index:
        # print(i)
        results.append(df_results.loc[i].to_dict())
        results[-1]["id"] = i

    return results

def quick_preprocess(text):
    """Function for very basic text preprocessing, used in game details query to search for similar game recommendations.

    Args:
        text (str): the string to process
    Returns:
        text (str): the processed string
    """
    
    text = text.lower()

    text = PUNCT_RE.sub("", text) # removes punctuation

    tokens = text.split()
    tokens = [t for t in tokens if t not in STOPWORDS]

    return " ".join(tokens)


@app.teardown_appcontext
def teardown_db(exception):
    close_db(exception)
    
# Home Page
@app.route('/')
def home():
    """Homepage. Default search is ranked by top ratings."""
    return render_template("home.html")

@app.route('/results', methods=['GET', 'POST'])
def results():
    """Looks like homepage, but with query results."""

    if request.method == 'POST':
        query = request.form.get("query")
    else: # GET
        query = request.args.get("query")

    # Handle case where no input is provided
    if not query:
        return render_template(
            "search.html",
            query="",
            results={}
        )

    # query_mode is a bool
    # gets mode from the toggle next to the search bar
    # if = 1, then is True then Literal Search
    # else Neural Search.
    # matches the display
    # might be bad idk brain tired
    query_mode = request.args.get("literal", "0") == "literal"
    # print(query_mode)

    results = query_backend(query, query_mode)
    genre = (request.args.get("genre") or "").strip()
    
    if genre:
        gl = genre.lower()
        results = [
            r for r in results
            if gl in [p.strip().lower() for p in (r.get("genres") or "").split(",")]
        ]
        
    if len(results) < 1:
        return render_template(
            "search.html",
            query=query,
            results={}
        )

    # NOTE This creates a list for displaying all genres in the sidebar along with the plots. It functions exactly like the clikcable genres in the game cards, so it's not really necessary. I'm leaving it out for now.
    # all_genres = []
    # for result in results:
    #     genres_str = result.get("genres") or ""
    #     genres_list = [g.strip() for g in genres_str.split(",") if g.strip()]
    #     all_genres.extend(genres_list)
    #     found_genres = sorted(set(all_genres))
        
    ids = [r["id"] for r in results]

    plot_ratings(ids)
    plot_genre_pie(ids, top_n=5)
    
    return render_template(
        "search.html",
        query=query,
        results=results
        # genres=found_genres
    )    
    
@app.route("/game/<int:game_id>")
def game_details(game_id):
    df_game = engine.show_results([game_id])

    if df_game.empty:
        return "Game not found", 404

    game = df_game.iloc[0].to_dict()
    game["id"] = game_id

    # NOTE still using a shortened description, but I made a very basic preprocessing function to maybe fit more content into the query
    # this comes before markdown so the text doesn't have the html formatting
    short_desc = quick_preprocess(game["desc"][:2000])
    
    game["desc"] = markdown.markdown(game["desc"])
    
    similar = query_backend(short_desc[:1000], False)  # use neural search for recommendations
    if len(similar) < 1:
        return render_template("game_details.html", game=game, similar=similar)

    return render_template("game_details.html", game=game, similar=similar[1:8])

# About page
@app.route('/about')
def about():
    """About Page"""
    return render_template("about.html")

if __name__ == "__main__":
    app.run(debug=True, use_reloader=False)
