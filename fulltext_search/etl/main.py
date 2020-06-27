import sqlite3
from collections import namedtuple
import json
import requests

EL_BULK_URL = 'http://127.0.0.1:9200/_bulk'


def download_sqlite_data():
    connnectioon = sqlite3.connect('db.sqlite')
    cursor = connnectioon.cursor()

    sql = """ SELECT m.id, m.imdb_rating, m.genre, m.title, m.plot AS description, m.director, GROUP_CONCAT(a.name,', ') AS actors_names, 
            (SELECT GROUP_CONCAT(w.name, ', ') from writers w WHERE CASE WHEN length(m.writers) > 0 THEN m.writers ELSE m.writer 
            end LIKE '%' || w.id || '%') writers_names, GROUP_CONCAT('{"id": "' || a.id || '", "name": "' || a.name || '"}',', ') AS actors,
            (SELECT GROUP_CONCAT('{"id": "' || w.id || '", "name": "' || w.name || '"}',', ') FROM writers w WHERE CASE WHEN
            length(m.writers) > 0 THEN m.writers ELSE m.writer end LIKE '%' || w.id || '%') writers FROM movies m LEFT JOIN movie_actors ma 
            ON ma.movie_id = m.id LEFT JOIN actors a ON ma.actor_id = a.id GROUP BY m.id """

    cursor.execute(sql)
    return cursor.fetchall()


def prepare_sqlite_data(sqlite_data):
    Movie = namedtuple('Movie', ['id', 'imdb_rating', 'genre', 'title', 'description',
                                 'director', 'actors_names', 'writers_names', 'actors', 'writers'])
    return list(map(Movie._make, sqlite_data))


def prepare_data_to_bulk_create(movies):
    bulk_data = ''
    for id_, movie in enumerate(movies, 1):
        bulk_data += '{{"index": {{"_index": "movies", "_id": "my_id_{}"}}}}\n'.format(
            id_)
        fields = {
            'id': movie.id,
            'imdb_rating': float(movie.imdb_rating if movie.imdb_rating != 'N/A' else 0),
            'genre': movie.genre.split(','),
            'title': movie.title,
            'description': movie.description,
            'director': movie.director,
            'actors_names': movie.actors_names.split(
                ',') if movie.actors_names != 'N/A' else [],
            'writers_names': movie.writers_names.split(
                ',') if movie.writers_names != 'N/A' else [],
            'actors': json.loads('[{}]'.format(movie.actors)),
            'writers': json.loads('[{}]'.format(movie.writers))
        }
        fields = json.dumps(fields)
        bulk_data += '{}\n'.format(fields)

    return bulk_data


def send_bulk_request(bulk_data):
    headers = {
        "Content-Type": "application/x-ndjson"
    }
    payload = {
        "filter_path": "items.*.error"
    }

    resp = requests.post(EL_BULK_URL, params=payload,
                         headers=headers, data=bulk_data)

    print("Incorrect items: {}".format(resp.json()))


if __name__ == '__main__':
    sqlite_data = download_sqlite_data()
    data = prepare_sqlite_data(sqlite_data)
    bulk_data = prepare_data_to_bulk_create(data)

    send_bulk_request(bulk_data)