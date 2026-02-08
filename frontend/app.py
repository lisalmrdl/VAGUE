from flask import Flask, render_template, request

app = Flask(__name__)

# Home Page
@app.route('/')
def home():
    """Homepage. Default search is ranked by top ratings."""
    return render_template("search.html")

@app.route('/results', methods=['POST'])
def results():
    """Looks like homepage, but with query results."""
    if request.method == 'POST':
        query = request.form["query"]
        num_results = 0

        return render_template(
            "search.html",
            query=query,
            num_results=num_results
            )
    # Handle case where no input is provided
    if not query:
        return render_template(
            "search.html",
            query="",
            num_results=0
        )

# @app.route('/game/<game>')
# def show_game(game):
#     """Display page for individual games."""
#     return render_template("game.html")

### Do I want to make it a tr or keep it as a div???? optimization?

# 6. Random order generator of unordered list
# @app.route("/shuffle", methods=["GET"])
# def shuffle_names():
#     shuffled_names_html = ""

#     names = request.args.get("names", "")
    
#     if names != "":
#         name_list = [name.strip() for name in names.split(",") if name.strip()]
#         random.shuffle(name_list)
#         shuffled_names_html = "<h2>🔀 Shuffled Order 🔀</h2><ul>"
#         shuffled_names_html += "".join(f"<li>{name}</li>" for name in name_list)
#         shuffled_names_html += "</ul>"
    
#     print(shuffled_names_html)
    
#     return html_template.replace("{{ shuffled_names }}", shuffled_names_html)

# About page
@app.route('/about')
def about():
    """About Page"""
    return render_template("about.html")

if __name__ == "__main__":
    app.run(debug=True)
