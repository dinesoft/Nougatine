import requests

params = (
    ('limit', '10'),
    ('offset', '0'),
    ('timezone', 'UTC'),
)

response = requests.get('https://opendata.paris.fr/api/v2/catalog/datasets/comptages-routiers-permanents/records', params=params)
