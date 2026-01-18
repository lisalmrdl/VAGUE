import requests, os
from pathlib import Path
from bs4 import BeautifulSoup
import pandas as pd

# This is the path of this script (my interpreter takes the root of the repo as the root path)
path = Path(os.path.abspath(os.path.dirname(__file__)))

# URL to OpenCritic (It's illegal to scrape Metacritic)
url = "https://opencritic.com/browse/all"
response = requests.get(url)
soup = BeautifulSoup(response.content, 'html.parser')

# Find the label that contains the browse system and then find inside this the panel where the results are displayed
soup = soup.find("app-browse-platform").find("div", class_="desktop-game-display")

# Inside the panel find all individual results for the games (all the panels such as each contains a 'game')
games = soup.find_all("div", class_="row no-gutters py-2 game-row align-items-center")

df_games = pd.DataFrame(columns=["score", "name", "platforms", "genre", "first_release"]) # Imma save the games' info here
for game in games:
    # Get the label that contains the note/score and get its content
    score = game.find("div", class_="inner-orb small-orb").text.strip()
    # The name is inside a hyperlink (label <a>) label with some weird attribute
    # So it may be better to find the parent and then get the hyperlink label (and then its content) just to be safe (there may be other hyperlinks in the container)
    name = game.find("div", class_="game-name col ml-2").find("a").text.strip()
    # Get the platforms
    platforms = game.find("div", class_="platforms col-auto").text.strip()
    # Get the genres
    genre = game.find("div", class_="genres mx-4").text.strip()
    # Get the release date
    release = game.find("div", class_="first-release-date col-auto show-year").text.strip()
    # Get the append the info to the dataframe
    df_games.loc[len(df_games)] = [score, name, platforms, genre, release]
# Export it to a nice looking excel
df_games.to_excel(path / "Games.xlsx")

print(df_games)