import geojson
import requests
import time
import json

class Client:
    URL = 'https://api.spaceknow.com'
    AUTHENTICATE_URL = 'https://spaceknow.auth0.com/oauth/ro'

    providers = {"gbdx": ['preview-multispectral',
                          'preview-swir', 'idaho-pansharpened',
                          'idaho-swir', 'idaho-panchromatic'],
                 "maxar": ['ard']
                 }
    token = ''

    def __init__(self, email, password):
        # take environment variables from .env.
        try:
            self.authenticate(email, password)
        except Exception:
            print("Authentication failed!")

    def authenticate(self, email, password):
        payload = {
            "client_id": "hmWJcfhRouDOaJK2L8asREMlMrv3jFE1",
            "username": email,
            "password": password,
            "connection": "Username-Password-Authentication",
            "grant_type": "password",
            "scope": "openid"
            }
        response = requests.post(self.AUTHENTICATE_URL,json=payload)
        self.token = response.json()['id_token']
        print('Successfuly authorized!')
        return True

    def get_extent(self, geojson_file):
        with open(geojson_file) as f:
            gj = geojson.load(f)
        return gj

    def get_headers(self):
        return {"Authorization": "Bearer " + self.token}

    # TODO sort out provider and dataset params better, add cursor
    def prepare_payload(self, geojson_file, time_range, provider='gbdx'):
        extent = self.get_extent(geojson_file)
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
        # print(geojson.dumps(payload, indent=4))
        return payload

    def run_pipeline(self, url, payload):
        # TODO implement retrieval when cursor not null (concaternate to results)
        # init
        response = json.loads(requests.post(self.URL + url + '/initiate', json=payload, headers=self.get_headers()).text)
        next_try, pipeline_id, status = response['nextTry'], response['pipelineId'], response['status']
        # retrieve
        while status not in ['RESOLVED', 'FAILED']:
            # check status
            print(f'Pipeline status: {status}')
            status_check_url = "/tasking/get-status"
            print(f'Waiting {next_try} seconds...')
            time.sleep(next_try)
            status = json.loads(requests.post(self.URL + status_check_url, json={"pipelineId": pipeline_id}).text)[
                'status']

        if status == 'FAILED':
            print("Pipeline initialization failed!")
            return []
        elif status == 'RESOLVED':
            # retrieve scenes
            result_scenes = json.loads(
                requests.post(self.URL + url + '/retrieve', json={"pipelineId": pipeline_id}, headers=self.get_headers()).text)
            print(json.dumps(result_scenes, indent=4))
            # TODO implement retrieval when cursor not null (concaternate to results)
        else:
            print("Unexpected status for pipeline.")
        return result_scenes

    # TODO cover exceptions - wrap into try block
    def find_scenes(self, time_range, geometry):
        pipeline_url = '/imagery/search'
        payload = self.prepare_payload(geometry, time_range)
        scenes = self.run_pipeline(pipeline_url, payload)['results']
        return scenes

    def analyze_images(self, scenes, geojson_file, map_type="cars"):
        kraken_url = f'/kraken/release/{map_type}/geojson'
        results = {}
        for scene in scenes:
            payload = {"sceneId": scene['sceneId'], "extent": self.get_extent(geojson_file)}
            maps = self.run_pipeline(kraken_url, payload)
            results[scene] = maps
        return results
