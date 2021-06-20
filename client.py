import functools
import json
import os
import shutil
import time
import sys

import cv2
import geojson
import numpy as np
import requests
import yaml
from dotenv import load_dotenv

DEBUG = True


def log(msg):
    if DEBUG:
        print(msg)


class Client:
    URL = 'https://api.spaceknow.com'
    AUTHENTICATE_URL = 'https://spaceknow.auth0.com/oauth/ro'
    DOWNLOAD_FOLDER = 'download'
    OUTPUT_FOLDER = 'output'
    providers = {}
    token = ''

    def __init__(self, email, password):
        # TODO check if file exists
        with open("providers.yaml", "r") as ymlfile:
            self.providers = yaml.load(ymlfile, Loader=yaml.FullLoader)
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
        print('Successfully authenticated!')
        return True

    @staticmethod
    def get_extent(geojson_file):
        with open(geojson_file) as f:
            gj = geojson.load(f)
        return gj

    def get_headers(self):
        return {"Authorization": "Bearer " + self.token}

    def analyze_location(self, time_range, geojson_file, image_type):
        """
        Function which tries to analyze given location in given time range.
        Function returns number of detected objects and saves images with detected objects into the output file

        :param time_range: range where images are queried
        :param geojson_file: path to file with location
        :param image_type: 'cars' or other type supported by Kraken API
        :return: number of objects detected in images in total
        """
        scenes = self.find_scenes(time_range, geojson_file)
        total_detected_objects = 0
        for i, scene in enumerate(scenes):
            # TODO based on # of bands create PNG, for now suppose RGB + near-ir
            imagery_img = self.recreate_image(scene, geojson_file, "truecolor", ".png", "imagery")
            detected_img = self.recreate_image(scene, geojson_file, image_type, ".png", image_type)
            # join images into one output
            if imagery_img is not None and detected_img is not None:
                added_image = cv2.addWeighted(imagery_img, 1, detected_img, 1, 0)
                timestr = time.strftime("%Y%m%d-%H%M%S")
                cv2.imwrite(f'output/output_{scene["sceneId"][:20]}_{timestr}.png', added_image)
            else:
                print(f'Failed to process scene {scene}')
            # detect and count objects from scene
            detected_objects = self.detect_objects(scene, geojson_file, "detections", ".geojson", image_type)
            if detected_objects is not None:
                print(f'Number of objects of class "{image_type}" detected from scene #{i + 1}: {detected_objects}')
                total_detected_objects += detected_objects
        return total_detected_objects

    def detect_objects(self, scene, geojson_file, suffix_name, suffix_format, image_type):
        """
        Function tries to detect and count objects from location provided in geojson_file

        :param scene: scene which is analyzed
        :param geojson_file: path to file with location
        :param suffix_name: for example 'imagery' or 'detections' (defined by API)
        :param suffix_format: based on  suffinx_name (e.g .png or .geojson)
        :param image_type: e.g 'cars' or 'imagery' (defined by API)
        :return: number of detected objects in scene
        """
        scene_objects_counter = 0
        # download geojson files for scene
        try:
            grid_tiles, map_id, identifier, image_type = self.download_grid_tiles_for_scene(scene,
                                                                                            geojson_file, suffix_name,
                                                                                            suffix_format, image_type)
        except Exception:
            print(f'Failed to detect objects for scene {scene}.')
            return None
        # open each grid tile geojson file and count objects
        for tile in grid_tiles:
            str_coords = [str(coord) for coord in tile]
            file_name = image_type + "_" + str.replace(map_id[:10], ".", "_") + "_" + \
                        identifier + "_" + "_".join(str_coords) + \
                        suffix_name + suffix_format
            with open(os.path.join(self.DOWNLOAD_FOLDER, file_name))as f:
                gj = geojson.load(f)
                scene_objects_counter += self.count_objects(gj, image_type)
        return scene_objects_counter

    @staticmethod
    def count_objects(gj, object_class):
        """
        Utility function to process geojson from Kraken response and count objects
        :param gj: geojson from Kraken output
        :param object_class: objects counted
        :return: number of objects in geojson
        """
        total_object_count = 0
        if "features" in gj:
            for feature in gj["features"]:
                if "properties" in feature and "class" in feature["properties"]:
                    if object_class == feature["properties"]["class"] and "count" in feature["properties"]:
                        total_object_count += feature["properties"]["count"]
        return total_object_count

    # TODO sort out provider and dataset params better, add cursor
    def prepare_payload(self, geojson_file, time_range, provider, dataset):
        extent = self.get_extent(geojson_file)
        cursor = ''
        # dataset = self.providers[provider][2]
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
        # TODO implement retrieval when cursor not null (concatenate to results)
        # init pipeline
        response = requests.post(self.URL + url + '/initiate', json=payload, headers=self.get_headers())
        if response.status_code != 200:
            return None

        response = json.loads(response.text)
        next_try, pipeline_id, status = response['nextTry'], response['pipelineId'], response['status']
        # retrieve
        while status not in ['RESOLVED', 'FAILED']:
            # check status
            log(f'Pipeline status: {status}')
            status_check_url = "/tasking/get-status"
            # TODO implement final number of tries (e.g. 50)
            log(f'Waiting {next_try} seconds...')
            time.sleep(next_try)
            # TODO implement status_code check
            status = json.loads(requests.post(self.URL + status_check_url, json={"pipelineId": pipeline_id}).text)[
                'status']

        if status == 'FAILED':
            log("Pipeline initialization failed!")
        elif status == 'RESOLVED':
            # retrieve scenes
            result_scenes = json.loads(
                requests.post(self.URL + url + '/retrieve', json={"pipelineId": pipeline_id},
                              headers=self.get_headers()).text)
            log(f'Pipeline resolved successfully!')
            return result_scenes
            # TODO implement retrieval when cursor not null (concatenate to results)
        else:
            log("Unexpected status for pipeline.")
        return None

    # TODO cover exceptions - wrap into try block
    def find_scenes(self, time_range, geometry):
        pipeline_url = '/imagery/search'
        scenes = []
        for provider, datasets in self.providers.items():
            for dataset in datasets:
                payload = self.prepare_payload(geometry, time_range, provider, dataset)
                print(f'Initializing pipeline to find scenes from provider {provider} and dataset {dataset}.')
                pipeline_result = self.run_pipeline(pipeline_url, payload)
                if pipeline_result is not None and 'results' in pipeline_result:
                    scenes = scenes + pipeline_result['results']
        return scenes

    def get_map(self, scene, geojson_file, map_type="imagery"):
        kraken_url = f'/kraken/release/{map_type}/geojson'
        payload = {"sceneId": scene['sceneId'], "extent": self.get_extent(geojson_file)}
        log(f'Initializing pipeline for scene {scene["sceneId"]}')
        grid = self.run_pipeline(kraken_url, payload)
        return grid

    # TODO consider if saving is necessary
    def download_and_save_tile(self, output_file_name, url):
        # save image
        response = requests.get(url, headers=self.get_headers(), stream=True)
        if response.status_code == 200:
            with open(os.path.join(self.DOWNLOAD_FOLDER, output_file_name), 'wb') as out_file:
                shutil.copyfileobj(response.raw, out_file)
            del response
            log(f'File downloaded to {output_file_name}')
            return True
        else:
            log(f'File download failed for file from url {url}')
            return False

    def get_tile_image(self, image_type, map_id, identifier, coords):
        str_coords = [str(coord) for coord in coords]
        suffix_name = "truecolor" if image_type == "imagery" else image_type
        img_file_name = image_type + "_" + str.replace(map_id[:10], ".", "_") + "_" + \
                        identifier + "_" + "_".join(str_coords) + \
                        suffix_name + ".png"
        return cv2.imread(os.path.join(self.DOWNLOAD_FOLDER, img_file_name), cv2.IMREAD_UNCHANGED)

    # TODO handle exceptions (file not saved/downloaded)
    def download_grid_tiles_for_scene(self, scene, geojson_file, suffix_name, suffix_format, image_type="imagery"):
        grid = self.get_map(scene, geojson_file, image_type)
        if grid == [] or grid is None:
            return None
        identifier = str(hash(grid['mapId']))[:10]
        # download and save grid tiles then concatenate them into one big png
        for tile in grid['tiles']:
            str_coords = [str(coord) for coord in tile]
            kraken_grid_url = "/kraken/grid/"
            imagery_url = self.URL + kraken_grid_url + \
                          grid['mapId'] + '/-/' + '/'.join(str_coords) + \
                          "/" + suffix_name + suffix_format
            imagery_filename = image_type + "_" + str.replace(grid['mapId'][:10], ".", "_") + "_" + \
                               identifier + "_" + "_".join(str_coords) + \
                               suffix_name + suffix_format
            self.download_and_save_tile(imagery_filename, imagery_url)
        # TODO for each band merge into one
        return grid['tiles'], grid['mapId'], identifier, image_type

    # wrapper to download grid tiles and concatenate them into one
    def recreate_image(self, scene, geojson_file, suffix_name, suffix_format, image_type="imagery"):
        recreated_image = None
        try:
            recreated_image = self.concatenate_image(
                *self.download_grid_tiles_for_scene(scene, geojson_file, suffix_name, suffix_format, image_type))
        except Exception:
            # print(f'Failed to recreate image for scene {scene}')
            pass
        return recreated_image

    def concatenate_image(self, grid_tiles, map_id, identifier, image_type):
        """
        Utility function to concaternate grid tiles into one big png

        :param grid_tiles: grid tiles
        :param map_id: map id
        :param identifier: to identify associated files together
        :param image_type: e.g 'cars'
        :return: concaternated image
        """
        # sort coordinates
        sorted_coords = sorted(grid_tiles, key=lambda k: [k[1], k[2]])
        row_coords = set([coord[1] for coord in sorted_coords])
        rows = np.array_split(np.array(sorted_coords), len(row_coords))
        # create rows with images
        v_strips = []
        # concetenate tiled into grid (vertically first and horizontally second)
        for row in rows:
            images = [self.get_tile_image(image_type, map_id, identifier, tile) for tile in row]
            v_strips.append(functools.reduce(lambda x, y: cv2.vconcat([x, y]), images))
        reconstructed_img = functools.reduce(lambda x, y: cv2.hconcat([x, y]), v_strips)
        # clean download folder
        shutil.rmtree(self.DOWNLOAD_FOLDER)
        os.mkdir(self.DOWNLOAD_FOLDER)
        return reconstructed_img


def main():
    image_type = 'cars'
    args = sys.argv[1:]
    time_range = []
    input_file = ""
    if len(args) == 3:
        input_file = args[0]
        time_range = [args[1], args[2]]
    elif len(args) == 4:
        input_file = args[0]
        time_range = [args[1], args[2]]
        image_type = args[3]
    else:
        print("Parameters for script: client.py <input_file> <timerange> [<image_type>]")
        print("<input_file> - path to geojson with single geometry (required)  [<image_type>]")
        print("<start_time> - timerange for analysis (required) (e.g. \"2018-01-01 00:00:00\"")
        print("<end_time> - timerange for analysis (required) (e.g. \"2018-01-31 23:59:59\"")
        print("[<image_type>] - analysis type (optional) 'cars' is default value")
        exit(1)
    load_dotenv()
    client = Client(os.getenv("SPACEKNOW_EMAIL"), os.getenv("SPACEKNOW_PASSWORD"))
    count = client.analyze_location(time_range, input_file, image_type)
    print(f'Number of cars total in location for given timerange is {count}')


if __name__ == "__main__":
    main()
