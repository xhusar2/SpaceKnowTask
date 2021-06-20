import geojson
import requests
import time
import json
import shutil
import os


class Client:
    URL = 'https://api.spaceknow.com'
    AUTHENTICATE_URL = 'https://spaceknow.auth0.com/oauth/ro'
    DOWNLOAD_FOLDER = 'download'
    providers = {"gbdx": ['preview-multispectral',
                          'preview-swir', 'idaho-pansharpened',
                          'idaho-swir', 'idaho-panchromatic'],
                 "maxar": ['ard']
                 }
    token = ''
    imagery_type = ['imagery']

    def __init__(self, email, password):
        # take environment variables from .env.
        try:
            self.authenticate(email, password)
        # TODO fix exception too broad
        except Exception:
            print("Authentication failed!")
        # clean download folder
        shutil.rmtree(self.DOWNLOAD_FOLDER)
        os.mkdir(self.DOWNLOAD_FOLDER)

    def authenticate(self, email, password):
        payload = {
            "client_id": "hmWJcfhRouDOaJK2L8asREMlMrv3jFE1",
            "username": email,
            "password": password,
            "connection": "Username-Password-Authentication",
            "grant_type": "password",
            "scope": "openid"
        }
        response = requests.post(self.AUTHENTICATE_URL, json=payload)
        self.token = response.json()['id_token']
        print('Successfuly authorized!')
        return True

    def get_extent(self, geojson_file):
        with open(geojson_file) as f:
            gj = geojson.load(f)
        return gj

    def get_headers(self):
        return {"Authorization": "Bearer " + self.token}

    def analyze_location(self, time_range, geojson_file, map_types):
        scenes = self.find_scenes(time_range, geojson_file)
        for scene in scenes:
            for map_type in map_types:
                # TODO get bands for imagery
                self.download_imagery(scene, geojson_file)
                self.download_detectables(scene, geojson_file, map_type)
        # for k, v in maps.items():
        #    for tile in v:
        #        self.download_images(k, tile)

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
        response = json.loads(
            requests.post(self.URL + url + '/initiate', json=payload, headers=self.get_headers()).text)
        next_try, pipeline_id, status = response['nextTry'], response['pipelineId'], response['status']
        # retrieve
        while status not in ['RESOLVED', 'FAILED']:
            # check status
            print(f'Pipeline status: {status}')
            status_check_url = "/tasking/get-status"
            print(f'Waiting {next_try} seconds...')
            time.sleep(next_try)
            # TODO implement status_code check
            status = json.loads(requests.post(self.URL + status_check_url, json={"pipelineId": pipeline_id}).text)[
                'status']

        if status == 'FAILED':
            print("Pipeline initialization failed!")
            return []
        elif status == 'RESOLVED':
            # retrieve scenes
            result_scenes = json.loads(
                requests.post(self.URL + url + '/retrieve', json={"pipelineId": pipeline_id},
                              headers=self.get_headers()).text)
            print(f'Pipeline resolved successfuly!')
            # TODO implement retrieval when cursor not null (concaternate to results)
        else:
            print("Unexpected status for pipeline.")
        return result_scenes

    # TODO cover exceptions - wrap into try block
    def find_scenes(self, time_range, geometry):
        pipeline_url = '/imagery/search'
        payload = self.prepare_payload(geometry, time_range)
        print('Initializing pipeline to find scenes.')
        scenes = self.run_pipeline(pipeline_url, payload)['results']
        return scenes

    def get_map(self, scene, geojson_file, map_type="imagery"):
        kraken_url = f'/kraken/release/{map_type}/geojson'
        payload = {"sceneId": scene['sceneId'], "extent": self.get_extent(geojson_file)}
        print(f'Initializing pipeline for scene {scene["sceneId"]}')
        grid = self.run_pipeline(kraken_url, payload)
        return grid

    def download_imagery(self, scene, geojson_file):
        grid = self.get_map(scene, geojson_file, "imagery")
        for tile in grid['tiles']:
            str_coords = [str(coord) for coord in tile]
            kraken_grid_url = "/kraken/grid/"
            #for color in ['red', 'green', 'blue', 'near-ir']:
            imagery_url = self.URL + kraken_grid_url + \
                             grid['mapId'] + '/-/' + '/'.join(str_coords) + \
                             "/truecolor.png"
            imagery_filename = "imagery_" + str.replace(grid['mapId'][:10], ".", "_") + "_" + \
                               str(hash(grid['mapId']))[10] + "_" + "_".join(str_coords) + \
                               "truecolor.png"
            self.dowload_and_save_tile(imagery_filename, imagery_url)

    def download_detectables(self, scene, geojson_file, map_type):
        grid = self.get_map(scene, geojson_file, map_type)
        for tile in grid['tiles']:
            str_coords = [str(coord) for coord in tile]
            kraken_grid_url = "/kraken/grid/"
            map_type_url = self.URL + kraken_grid_url + grid['mapId'] + '/-/' + '/'.join(str_coords) + "/" + map_type +".png"
            detections_url = self.URL + kraken_grid_url + grid['mapId'] + '/-/' + '/'.join(str_coords) + "/detections.geojson"
            # create unique names for files
            map_type_filename = "detected_img_" + str.replace(grid['mapId'][:10], ".", "_") + \
                                 str(hash(grid['mapId']))[10] + "_".join(str_coords) + \
                                 "_" + map_type + ".png"
            detections_filename = "detected_img_" + str.replace(grid['mapId'][:10], ".", "_") + \
                                  str(hash(grid['mapId']))[10] + "_" + "_".join(str_coords) + \
                                  "_detections.geojson"
            self.dowload_and_save_tile(map_type_filename, map_type_url)
            self.dowload_and_save_tile(detections_filename, detections_url)

    # TODO try to implement in one function
    def download_images(self, scene, geojson_file, map_type):
        self.download_detectables(scene, geojson_file, map_type)

    def dowload_and_save_tile(self, output_file_name, url):
        # save image
        response = requests.get(url, headers=self.get_headers(), stream=True)
        if response.status_code == 200:
            with open(os.path.join(self.DOWNLOAD_FOLDER, output_file_name), 'wb') as out_file:
                shutil.copyfileobj(response.raw, out_file)
            del response
            print(f'File downloaded to {output_file_name}')
            return True
        else:
            print(f'File download failed for file from url {url}')
            return False
