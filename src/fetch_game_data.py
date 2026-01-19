import requests, os, json, time, datetime
from pathlib import Path

path = Path(os.path.abspath(os.path.dirname(__file__)))
API_KEY = json.load(open(path / ".." / "apikeys.json", "r"))["RAWG"]
BASE_URL = "https://api.rawg.io/api/games"
print(datetime.date.today())
n_items = 50000
items_per_page = 40 # 40 is tha maximum
try:
    games = json.load(open(path / ".." / "data" / "db.json", "r"))
    if len(games["games"].keys()): raise ValueError()
except:
    games = {
        "metadata" : {
            "date": str(datetime.date.today()),
            "pages": 0,
            "length": 0
        },
        "games" : {}
    }

params = {
    "key": API_KEY,
    "page": 1,
    "page_size": items_per_page,
    "ordering": "-metacritic"
}


for page in range(1, int(n_items/items_per_page)+1):
    t = time.time()
    params["page"] = page
    response = requests.get(BASE_URL, params=params)
    data = response.json()
    for i in data["results"]:
        id = i["id"]
        game = requests.get(
            f"https://api.rawg.io/api/games/{id}",
            params={"key": API_KEY}
        ).json()
        games["games"][id] = {
            "name" : game["name"],
            "released": game["released"],
            "genres": ", ".join([x["name"].lower() for x in game["genres"]]),
            "developers": ", ".join([x["name"] for x in game["developers"]]),
            "publishers": ", ".join([x["name"] for x in game["publishers"]]),
            "metacritic":game["metacritic"],
            "rating": game["rating"],
            "ratings_count": game["ratings_count"],
            "description_raw": game["description_raw"]
        }
    games["metadata"]["pages"] += 1
    games["metadata"]["length"] += items_per_page
    json.dump(games, open(path / ".." / "data" / "db.json", "w"))
    print(time.time() - t)
print("DONE")