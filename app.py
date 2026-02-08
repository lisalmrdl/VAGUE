from flask import Flask, render_template, request
#from src.searchEngine import smart_search_router, show_results
import src.searchEngine as engine
from src.database import close_db
from pprint import pprint


app = Flask(__name__)

dummy_data = [{"id": 1,
               "title": "Amazing Game",
               "year": 2002,
               "publisher": "Publisher",
               "genres": ["Genre 1"],
               "rating": 2.3,
               "description": "Here be the game description."
               },
              {"id": 2,
               "title": "Amazing Game 2",
               "year": 2022,
               "publisher": "Publisher",
               "genres": ["Genre 1", "Genre 2", "Genre 3"],
               "rating": 1.9,
               "description": "Another description."
              }]

@app.teardown_appcontext
def teardown_db(exception):
    close_db(exception)

def search_games(query):
    """Temporary query function for testing frontend with dummy data"""
    return [game for game in dummy_data if query.lower() in game["title"].lower()]
    
# Home Page
@app.route('/')
def home():
    """Homepage. Default search is ranked by top ratings."""
    return render_template(
        "search.html",
        query="Top Games (dummy data)",
        results=sorted(dummy_data, key=lambda g: g["rating"], reverse=True)[:10]
    )

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

    df_results = engine.show_results(engine.smart_search_router(query))  # db version
    #results = search_games(query)   # dummy data version
    # TODO: change this, I am going to do something very ugly and inefficient
    results = []
    for i in df_results.index:
        print(i)
        results.append(df_results.loc[i].to_dict())
        results[-1]["id"] = i
    pprint(results[0])
    
    return render_template(
        "search.html",
        #query=query,
        results=results
    )    
    
@app.route("/game/<int:game_id>")
def game_details(game_id):
    # Find the game by ID
    game = next((g for g in dummy_data if g["id"] == game_id), None)
    if game is None:
        return "Game not found", 404
    return render_template("game_details.html", game=game)

# About page
@app.route('/about')
def about():
    """About Page"""
    return render_template("about.html")

if __name__ == "__main__":
    app.run(debug=True)
