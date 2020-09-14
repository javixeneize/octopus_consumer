import requests
from base64 import b64encode
import json
from elasticsearch import Elasticsearch
from elasticsearch import exceptions as es_ex
import hashlib
import datetime
from collections import defaultdict

API_KEY = 'YOUR_API_KEY'
AUTH_HEADER = b64encode(API_KEY.encode('ascii')).decode('ascii')
ELECTRICITY_POINT = 'YOUR_EP'
ELECTRICITY_METER = 'YOUR_EM'
GAS_POINT = 'YOUR_GAS_POINT'
GAS_METER = 'YOUR_GAS_METER'
ELEC_STANDING = 0.2005
ELEC_UNIT_PRICE = 0.1462
GAS_STANDING = 0.1785
GAS_UNIT_PRICE = 0.0255
M3_TO_KWH = 11.19
YESTERDAY = datetime.date.today() - datetime.timedelta(days=1)
DAY_BEFORE_YESTERDAY = datetime.date.today() - datetime.timedelta(days=2)
TODAY = datetime.date.today()

es = Elasticsearch([{'host': 'YOUR_ELASTIC_HOST', 'port': 9200}])
electricity_ep = 'https://api.octopus.energy/v1/electricity-meter-points/%s' \
                 '/meters/%s/consumption/?period_from=%sT23:00:00Z&period_to=%sT23:30:00Z' \
                 % (ELECTRICITY_POINT, ELECTRICITY_METER, DAY_BEFORE_YESTERDAY, YESTERDAY)
gas_ep = 'https://api.octopus.energy/v1/gas-meter-points/%s/meters/%s/' \
         'consumption/?period_from=%sT23:00:00Z&period_to=%sT23:30:00Z' \
         % (GAS_POINT, GAS_METER, DAY_BEFORE_YESTERDAY, YESTERDAY)

headers = {'Authorization': 'Basic {}'.format(AUTH_HEADER)}


class Octopus_data():
    def __init__(self):
        self.daily_usage = {}
        self.hourly_usage = {}

    def parse_gas(self, gas_data):
        total = 0
        day_consumption = {}
        for item in gas_data:
            item['consumption'] = round(item.get('consumption') * GAS_UNIT_PRICE * M3_TO_KWH, 2)
            total += item.get('consumption')
            item['gas_consumption'] = item.pop('consumption')
        total = round(total + GAS_STANDING, 2)
        day_consumption['date'] = YESTERDAY
        day_consumption['gas_cost'] = total
        return day_consumption, gas_data

    def parse_electricity(self, electricity_data):
        total = 0
        day_consumption = {}
        for item in electricity_data:
            item['consumption'] = round(item.get('consumption') * ELEC_UNIT_PRICE, 2)
            total += item.get('consumption')
            item['electricity_consumption'] = item.pop('consumption')
        total = round(total + ELEC_STANDING, 2)
        day_consumption['date'] = YESTERDAY
        day_consumption['electricity_cost'] = total
        return day_consumption, electricity_data

    def merge_daily_data(self, gas_data, elec_data):
        self.daily_usage = gas_data
        self.daily_usage.update(elec_data)
        self.daily_usage['total'] = round(self.daily_usage.get('electricity_cost') \
                                          + self.daily_usage.get('gas_cost'), 2)

    def merge_hourly_data(self, gas_hourly_data, elec_hourly_data):
        merged = defaultdict(dict)
        for item in gas_hourly_data + elec_hourly_data:
            merged[item['interval_start']].update(item)
        self.hourly_usage = list(merged.values())
        for item in self.hourly_usage:
            if not item.get('electricity_consumption'):
                item['electricity_consumption'] = 0.0
            if not item.get('gas_consumption'):
                item['gas_consumption'] = 0.0

    def reset_data(self):
        self.daily_usage = {}
        self.hourly_usage = {}


def insertElastic(json, index):
    hash = hashlib.sha256(str(json).encode('utf-8')).hexdigest()
    try:
        if (es.search(index=index, body={"query": {"match": {"_id": hash}}})['hits']['total']['value'] == 0):
            es.index(index=index, doc_type='json', id=hash, body=json)
            print('Item ingested')
        else:
            print('Duplicated item. Ignoring')
    except es_ex.NotFoundError as e:
        es.index(index=index, doc_type='json', id=hash, body=json)


octopus = Octopus_data()
electricity_data = json.loads(requests.get(electricity_ep, headers=headers).content.decode('utf-8')).get('results')[
                   0:48]
gas_data = json.loads(requests.get(gas_ep, headers=headers).content.decode('utf-8')).get('results')[0:48]
gas_daily_consumption, gas_hourly_consumption = octopus.parse_gas(gas_data)
elec_daily_consumption, elec_hourly_consumption = octopus.parse_electricity(electricity_data)
octopus.merge_daily_data(gas_daily_consumption, elec_daily_consumption)
octopus.merge_hourly_data(gas_hourly_consumption, elec_hourly_consumption)
for item in octopus.hourly_usage:
    insertElastic(item, 'octopus_hourly')
insertElastic(octopus.daily_usage, 'octopus_daily')
