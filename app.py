from flask import Flask, render_template, request
import src.searchEngine as engine
from src.database import close_db, plot_ratings, plot_genre_pie
from pprint import pprint
import markdown

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

    
    # df_results = engine.show_results(engine.smart_search_router(query, query_mode))  # db version

    # if df_results is None or df_results.empty:
    #     return render_template(
    #         "search.html",
    #         query=query,
    #         results={}
    #     )

    # # TODO: change this, I am going to do something very ugly and inefficient
    # results = []
    # for i in df_results.index:
    #     # print(i)
    #     results.append(df_results.loc[i].to_dict())
    #     results[-1]["id"] = i
    # # pprint(results[0])

    results = query_backend(query, query_mode)
    if len(results) < 1:
        return render_template(
            "search.html",
            query=query,
            results={}
        )
    
    ids = [r["id"] for r in results]

    plot_ratings(ids, "static/plots/ranking.png")
    plot_genre_pie(ids, "static/plots/genres_pie.png")
    
    return render_template(
        "search.html",
        query=query,
        results=results
    )    
    
@app.route("/game/<int:game_id>")
def game_details(game_id):
    df_game = engine.show_results([game_id])

    if df_game.empty:
        return "Game not found", 404

    game = df_game.iloc[0].to_dict()
    game["id"] = game_id

    game["desc"] = markdown.markdown(game["desc"])

    # TODO: change this, I am going to do something very ugly and inefficient
    # naive recommendation system, takes the first 500 chars of the description and shoves it into a neural search
    # TODO maybe some text cleaning before encoding
    similar = query_backend(game["desc"][:1000], False)  # use neural search for recommendations
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
