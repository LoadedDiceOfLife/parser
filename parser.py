#!/usr/bin/python

import xmltodict
from collections import defaultdict

try:
    from urllib.request import urlopen
except:
    from urllib import urlopen


class Areas:
    area_data = defaultdict(dict)
    regions = defaultdict(list)

    def __init__(self, empty=False):
        print('initializing')
        return super(Areas, self).__init__()

    def add_all(self):
        self.add_deprication()
        self.add_education()
        self.add_telecom()
        self.add_housing()

    def add_deprication(self):
        with open('data/deprevation.xml') as xml_file:
            o = self.area_name_deprivation_mapping_dict = xmltodict.parse(
                    xml_file.read())
        for area in zip(
                o['AtlasData']['Geography']['FeatureList']['Feature'],
                o['AtlasData']['Geography']['ThemeList']['Theme']['Indicator']['Value']):
            area_name, area_dep = area
            self.regions[area_name['FilterValue']['#text']].append(area_name['@name'])
            assert(area_name['@id'] == area_dep['@for'])
            self.area_data.update({
                area_name['@name']: {
                    'au': area_name['@id'],
                    'name': area_name['@name'],
                    'deprivation': area_dep['#text']
                    }
                })

    def add_education(self):
        with open('data/area_unit_qualification_count_version1.txt') as f:
            keys = f.readline()[:-1].split('|')[1:]
            for line in f.readlines():
                data = line[:-1].split('|')
                self.area_data[data[0]].update({
                    'education': dict(zip(keys, data[1:]))
                    })

    def add_telecom(self):
        def key(code):
            return {
                    "-1": 'some',
                    "00": 'none',
                    "01": 'mobile',
                    "02": 'telephone',
                    "04": 'internet'
                    }[code]

            if code < 0:
                return "some"
            elif code < 1:
                return "none"
            elif code < 2:
                "mobile"
        with open('data/telecom_areas.txt') as f:
            f.readline()
            for line in f.readlines():
                content = line[:-1].split("|")
                area = self.area_data[content[1][1:-1]]
                if 'telecom' not in area:
                    area.update({'telecom': dict()})
                area['telecom'].update({
                            key(content[2][1:-1]): content[4][1:-1]})

    def add_housing(self):
        with open('data/housing.csv') as f:
            region = None
            age = None
            for line in f.readlines():
                data = line.split('|')
                if region != data[0][1:-1]:
                    region = data[0][1:-1]
                    new = True
                else:
                    new = False
                if age != data[1][1:-1]:
                    age = data[1][1:-1]
                    new_age = True
                else:
                    new_age = False
                situation = data[2][1:-1]
                amount = data[3]
                for area in self.regions[region]:
                    if new_age or new:
                        if new:
                            self.area_data[area].update({'housing': dict()})
                        self.area_data[area]['housing'].update({
                            'age-%s' % age: dict()
                            })
                    self.area_data[area]['housing']['age-%s' % age].update({
                            situation: amount
                        })

        with open('data/home_ownership.csv') as f:
            for line in f.readlines():
                row_data = line.split('|')
                area_name = row_data[0]
                yes = float(row_data[6])
                area = self.area_data[area_name]
                if not 'housing' in self.area_data[area_name]:
                    area.update({
                        'housing': dict()
                        })
                area['housing'].update({'owns': yes})

    def fill_db(self):
        from pymongo import MongoClient
        client = MongoClient()
        db = client.test_database_alpha
        print("Parsing data", end="")
        areas = self
        print('done.\nFilling database...', end='')
        db.area_data.insert_many(list(areas.area_data.values()))
        db.regions.insert_one(areas.regions).inserted_id
        print('done.')

    def dump_JSON():
        import simplejson as json
        json.dumps(self.area_data)
        json.dumps(self.regions)

b = Areas()
b.add_deprication()
b.add_housing()
