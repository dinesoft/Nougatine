import json

import requests
from datetime import date, timedelta


def daily_request():
    yesterday = date.today() - timedelta(days=1)

    # Request
    headers = {
        'accept': '*/*',
    }

    params = (
        # ('limit', "1"),
        ('where', 't_1h >=  \'{}\''.format(yesterday)),
        ('offset', '0'),
        ('timezone', 'UTC'),
    )

    response = requests.get(
        'https://opendata.paris.fr/api/v2/catalog/datasets/comptages-routiers-permanents/exports/csv', headers=headers,
        params=params)

    data = response.content.decode('utf8')

    # Save file
    with open("data/raw/raw_data.csv", "w") as fo:
        fo.write(data)


if __name__ == '__main__':
    daily_request()
