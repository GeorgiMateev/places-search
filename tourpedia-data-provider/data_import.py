import json
import os
import re
import errno

import csv
import time

import requests

places_history_file_path = 'output/places_history.txt'
reviews_history_file_path = 'output/reviews_history.txt'
places_details_file_path = 'output/places_details.jl'
places_details_reviews_file_path = 'output/places_details_reviews.jl'
reviews_file_path = 'output/reviews.jl'
target_files = ['data/london-attraction.csv', 'data/london-poi.csv', 'data/london-restaurant.csv']


def get_history(file_path):
    if os.path.exists(file_path):
        with open(file_path, 'r') as history_file:
            history_content = history_file.readlines()

            return [x.strip() for x in history_content]
    else:
        try:
            if not os.path.exists(os.path.dirname(file_path)):
                os.makedirs(os.path.dirname(file_path))
        except OSError as exc:  # Guard against race condition
            if exc.errno != errno.EEXIST:
                raise
    return []


places_history = get_history(places_history_file_path)
reviews_history = get_history(reviews_history_file_path)


def retry_get_url(url):
    retry_count = 3
    success = False

    while retry_count > 0 and not success:
        res = requests.get(url)
        retry_count -= 1
        success = res.status_code == 200

        # Always sleep in order not to spam the server
        time.sleep(0.1)

    if not success:
        raise Exception(f'Unable to get url {url}')

    return res.json()


def dump_jsonl(data, writer):
    json.dump(data, writer)
    writer.write('\n')


def extract_details_url(text):
    url_pattern = '(?P<url>http://tour-pedia.org/api/getPlaceDetails\\?id=(?P<id>\d+))'
    res = re.search(url_pattern, text)
    url = res.group('url')
    pid = res.group('id')
    return url, pid


def import_for_file(target_file):
    with open(target_file, 'r', encoding='utf-8') as target_data:
        lines_reader = target_data.readlines()

        line_count = 0
        for row in lines_reader:
            if line_count == 0:
                print(f'Column names are {", ".join(row)}')
            else:
                details_url, place_id = extract_details_url(row)

                if place_id in places_history:
                    continue
                else:
                    place_details_json = retry_get_url(details_url)

                    if 'reviews' in place_details_json:
                        reviews_ids = place_details_json["reviews"]
                        reviews_details = dump_reviews(reviews_ids)

                        dump_jsonl(place_details_json, places_details_writer)

                        del place_details_json["reviews"]
                        place_details_json["reviews"] = reviews_details
                    else:
                        place_details_json["reviews"] = []
                        dump_jsonl(place_details_json, places_details_writer)

                    dump_jsonl(place_details_json, places_details_reviews_writer)

                    places_history_writer.write(f'{place_id}\n')

            line_count += 1


def dump_reviews(reviews_ids):
    reviews = []
    for review_id in reviews_ids:
        review = retry_get_url(f'http://tour-pedia.org/api/getReviewDetails?id={review_id}')
        reviews.append(review)

        if review_id not in reviews_history:
            dump_jsonl(review, reviews_writer)
            reviews_history_writer.write(f'{review_id}\n')

    return reviews


def start_import_data():
    for target_file in target_files:
        import_for_file(target_file)


if __name__ == '__main__':
    places_history_writer = open(places_history_file_path, 'a', 1)
    reviews_history_writer = open(reviews_history_file_path, 'a', 1)

    places_details_writer = open(places_details_file_path, 'a', 1)
    places_details_reviews_writer = open(places_details_reviews_file_path, 'a', 1)
    reviews_writer = open(reviews_file_path, 'a', 1)

    try:
        start_import_data()
    except:
        places_history_writer.close()
        reviews_history_writer.close()
        places_details_writer.close()
        places_details_reviews_writer.close()
        reviews_writer.close()
