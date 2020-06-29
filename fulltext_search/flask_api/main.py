import json
from flask import Flask, request, jsonify
from flask import abort
import requests

ELASTIC_URL = "http://127.0.0.1:9200"

app = Flask(__name__)


def resp_error(field, msg, type_):
    return {"detail": [{"loc": ["query", field], "msg": msg, "type": type_,}]}


def int_resp_error(filed_name):
    return resp_error(filed_name, "value is not a valid integer", "type_error.integer")


def check_args_int_value(value, default_value):
    if value is None:
        return default_value

    value = int(value)
    if value <= 0:
        raise ValueError
    return value


def return_search_req_body(page, limit, sort_field, sort_order, search_query):
    body = {
        "from": page,
        "size": limit,
        "sort": [{sort_field: sort_order}],
    }
    if search_query:
        body["query"] = {
            "multi_match": {
                "fields": [
                    "title",
                    "description",
                    "actors_names",
                    "writers_names",
                    "director",
                ],
                "query": search_query,
            }
        }
    return body


def prepare_search_result_resp(elastic_search_resp):
    search_res_objects = elastic_search_resp["hits"]["hits"]
    result = []
    for res_object in search_res_objects:
        source = res_object["_source"]
        result.append(
            {
                "id": source["id"],
                "title": source["title"],
                "imdb_rating": source["imdb_rating"],
            }
        )

    return result


@app.route("/api/movies/", methods=["GET"])
def movies_list():
    accepted_query_params = ["limit", "page", "sort", "sort_order", "search"]
    accepted_sort_values = ["id", "title", "imdb_rating"]
    accepted_order_sort_values = ["asc", "desc"]

    query_params = request.args.keys()
    for param in query_params:
        if param not in accepted_query_params:
            return ("invalid request body format", 400)

    limit = request.args.get("limit")
    try:
        limit = check_args_int_value(limit, 50)
    except ValueError:
        return (int_resp_error("limit"), 422)

    page = request.args.get("page")
    try:
        page = check_args_int_value(page, 1)
    except ValueError:
        return (int_resp_error("page"), 422)

    sort = request.args.get("sort", "id", type=str)
    if sort not in accepted_sort_values:
        return (resp_error("sort", "sort field is not permitted", ""), 422)

    sort_order = request.args.get("sort_order", "asc", type=str)
    if sort_order not in accepted_order_sort_values:
        return (resp_error("sort", "sort order does not exist", ""), 422)

    search = request.args.get("search", "", type=str)

    search_req_body = json.dumps(
        return_search_req_body(page, limit, sort, sort_order, search)
    )
    search_url = "{}/movies/_search".format(ELASTIC_URL)
    headers = {"Content-Type": "application/json"}
    search_response = requests.get(search_url, data=search_req_body, headers=headers)
    result = prepare_search_result_resp(search_response.json())
    return jsonify(result)


@app.route("/api/movies/<string:movie_id>", methods=["GET"])
def movie_info(movie_id):
    movie_info_url = "{url}/movies/_doc/{movie_id}".format(
        url=ELASTIC_URL, movie_id=movie_id
    )
    response = requests.get(movie_info_url)
    resp_data = response.json()
    if not resp_data["found"]:
        return ("Фильм не найден", 404)

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

