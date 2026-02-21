from flask import Flask, render_template, request
import src.searchEngine as engine
from src.database import close_db, plot_ratings, plot_genre_pie
from pprint import pprint
import markdown

app = Flask(__name__)

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
    print(query_mode)
    
    df_results = engine.show_results(engine.smart_search_router(query, query_mode))  # db version

    if df_results is None or df_results.empty:
        return render_template(
            "search.html",
            query=query,
            results={}
        )

    # TODO: change this, I am going to do something very ugly and inefficient
    results = []
    for i in df_results.index:
        # print(i)
        results.append(df_results.loc[i].to_dict())
        results[-1]["id"] = i
    # pprint(results[0])

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

    return render_template("game_details.html", game=game)

# About page
@app.route('/about')
def about():
    """About Page"""
    return render_template("about.html")

if __name__ == "__main__":
    app.run(debug=True, use_reloader=False)
