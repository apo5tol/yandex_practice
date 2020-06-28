import json
from flask import Flask, request
from flask import abort
import requests

ELASTIC_URL = "http://127.0.0.1:9200"

app = Flask(__name__)

@app.route("/api/movies/<movie_id>", methods=["GET"])
def movie_info(movie_id):
    movie_info_url = "{url}/movies/_doc/{movie_id}".format(
        url=ELASTIC_URL, movie_id=movie_id
    )
    response = requests.get(movie_info_url)
    resp_data = response.json()
    if not resp_data["found"]:
        return abort(404, description="Фильм не найден")

    elastic_movie_data = resp_data["_source"]
    answer = {
        "id": elastic_movie_data["id"],
        "title": elastic_movie_data["title"],
        "description": elastic_movie_data["description"],
        "imdb_rating": elastic_movie_data["imdb_rating"],
        "writers": elastic_movie_data["writers"],
        "actors": elastic_movie_data["actors"],
        "genre": elastic_movie_data["genre"],
        "director": elastic_movie_data["director"],
    }
    return answer


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=8000, debug=True)

