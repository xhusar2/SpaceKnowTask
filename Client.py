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

    # TODO sort out provider and dataset params better, add cursor
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
        # print(geojson.dumps(payload, indent=4))
        return payload

    def download_scenes(self, time_range, geometry):
        initiate_pipeline_url = '/imagery/search/initiate'
        retrieve_pipeline_url = '/imagery/search/retrieve'
        payload = self.prepare_payload(geometry, time_range)
        headers = {"Authorization": "Bearer " + self.token}
        # initialize pipeline
        response = json.loads(requests.post(self.URL + initiate_pipeline_url, json=payload, headers=headers).text)
        next_try, pipeline_id, status = response['nextTry'], response['pipelineId'], response['status']

        while status not in ['RESOLVED', 'FAILED']:
            # check status
            print(f'Pipeline status: {status}')
            status_check_url = "/tasking/get-status"
            print(f'Waiting {next_try} seconds...')
            time.sleep(next_try)
            status = json.loads(requests.post(self.URL + status_check_url, json={"pipelineId":pipeline_id}).text)['status']

        if status == 'FAILED':
            print("Pipeline initialization failed!")
            return []
        else:
            # retrieve scenes
            result_scenes = json.loads(requests.post(self.URL + retrieve_pipeline_url, json={"pipelineId": pipeline_id}, headers=headers).text)
            print(json.dumps(result_scenes, indent=4))
        return result_scenes



    def analyze_images(self, images):
        pass
