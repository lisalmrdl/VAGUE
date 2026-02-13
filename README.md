# VAGUE
Group repository for Video-game Approximate Game-finding Utility Engine (V.A.G.U.E)

## Overview
This project is a search engine focused on video games that allows users to search our database of 10,000 games. 
Searches can be specific or related to a particular theme.
Possible queries are:
	- game title
	- genre
	- keywords
	- approximate or similarity-based queries

## Motivation
Industry standards, i.e. Steam, offer varying search results depending on different factors such as the day of the inquery.
VAGUE simplifies navigating through an overwhelming amount of data by streamlining the search process through a fixed database. 

## Project Goal
The goal of this project is to design and implement a prototype search engine that retrieves similar video games using approximate, similarity-based search methods with easily interpretable results.
It connects backend IR logic with a web-based frontend.

## Approach and Methods
The search engine combines Boolean search and TF-IDF-based ranking in a simple modular pipeline.
All queries are first checked by smart_search_router().
Tehank, if a query contains logical operators like AND, OR, or NOT, it is handled using Boolean retrieval. Otherwise, the system applies TF-IDF ranking with cosine similarity. 
If a Boolean query does not return results, the system automatically falls back to TF-IDF search.
Quoted phrases are treated as exact matches within the ranked results. 
The system also supports optional stemming and basic wildcard queries using *.

## Repository Structure
VAGUE/
│
├── app.py
│
├── src/
│   ├── searchEngine.py
│   ├── vector_processing.py
│   ├── database.py
│   ├── make_database.py
│   ├── fetch_game_data.py
│   └── testQuerydb.py
│
├── data/
│   ├── games.db
│   ├── tf_idf/
│   ├── boolean_vectors/
│   └── db.json
│
├── templates/
├── static/

## Setup
clone repository
	git clone https://github.com/lisalmrdl/VAGUE.git
change directory
	cd VAGUE
create virtual enviroment
	python3 -m venv venv
activate virtual enviroment
	source venv/bin/activate
install dependencies
	pip install flask pandas numpy scikit-learn


## Usage
Start the application:
	python app.py
open in your browser:
	#website?


## Team
Ivan Montejo de Garcini
Lisa Lemardele
Suchanya Limpakom
Ina Goettling

## Course Context
This project was developed as part of the Building NLP Applications course at the University of Helsinki.
