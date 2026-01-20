import requests, os, json, time, datetime
from tqdm import tqdm
from pathlib import Path

path = Path(os.path.abspath(os.path.dirname(__file__)))
API_KEY = json.load(open(path / ".." / "apikeys.json", "r"))["RAWG"]
BASE_URL = "https://api.rawg.io/api/games"
n_items = 50000
items_per_page = 40 # 40 is tha maximum
try:
    metadata = json.load(open(path / ".." / "data" / "db.json", "r"))["metadata"]
    if "games "not in json.load(open(path / ".." / "data" / "db.json", "r"))["games"].keys(): raise KeyError()
except:
    json.dump({
        "metadata" : {
            "date": str(datetime.date.today()),
            "pages": 0,
            "length": 0
        },
        "games" : {}
    }, open(path / ".." / "data" / "db.json", "w"))
    metadata = json.load(open(path / ".." / "data" / "db.json", "r"))["metadata"]

params = {
    "key": API_KEY,
    "page": 1,
    "page_size": items_per_page,
    "ordering": "-metacritic"
}


for page in tqdm(range(1, int(n_items/items_per_page)+1)):
    games = {}
    params["page"] = page
    response = requests.get(BASE_URL, params=params)
    data = response.json()
    for i in data["results"]:
        try:
            id = i["id"]
            game = requests.get(
                f"https://api.rawg.io/api/games/{id}",
                params={"key": API_KEY}
            ).json()
            games[id] = {
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
        except: pass
    metadata["pages"] += 1
    metadata["length"] += items_per_page
    db = json.load(open(path / ".." / "data" / "db.json", "r"))
    db["metadata"] = metadata
    for k, v in games.items():
        db["games"][k] = v
    json.dump(db, open(path / ".." / "data" / "db.json", "w"))
    del db
        