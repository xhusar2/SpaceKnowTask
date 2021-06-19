import geojson
import requests


class Client:
    URL = 'https://api.spaceknow.com'
    providers = {"gbdx": ['preview-multispectral',
                          'preview-swir', 'idaho-pansharpened',
                          'idaho-swir', 'idaho-panchromatic'],
                 "maxar": ['ard']
                 }

    def __init__(self):
        pass


    def authenticate(self):
        pass

    #TODO sort out provider and dataset params better, add cursor
    def prepare_payload(self, coords_file, time_range, provider='gbdx'):
        with open(coords_file) as f:
            gj = geojson.load(f)
        extent = gj
        cursor = ''
        dataset = self.providers[provider][2]
        start_datetime = time_range[0]
        end_datetime = time_range[1]
        payload = {"provider": provider,
                   "dataset": dataset,
                   "startDatetime": start_datetime,
                   "endDatetime": end_datetime,
                   "extent": extent}
        if cursor != '':
            payload['cursor'] = cursor
        print(geojson.dumps(payload, indent=4))
        return payload

    def download_images(self, time_range, geometry):
        payload = self.prepare_payload(geometry, time_range)
        search_url = '/imagery/search/initiate'
        response = requests.post(self.URL + search_url, json=payload)
        print(response.text)

    def analyze_images(self, images):
        pass
