import geojson
import requests
import time
import json
import shutil
import os
import cv2
import numpy as np
import functools

DEBUG = False


def log(msg):
    if DEBUG:
        print(msg)


class Client:
    URL = 'https://api.spaceknow.com'
    AUTHENTICATE_URL = 'https://spaceknow.auth0.com/oauth/ro'
    DOWNLOAD_FOLDER = 'download'
    OUTPUT_FOLDER = 'output'
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

    def analyze_location(self, time_range, geojson_file, image_type):
        scenes = self.find_scenes(time_range, geojson_file)
        total_detected_objects = 0
        for i, scene in enumerate(scenes):
            # TODO based on # of bands create PNG, for now suppose RGB + near-ir
            imagery_img = self.recreate_image(scene, geojson_file, "truecolor", ".png", "imagery")
            detected_img = self.recreate_image(scene, geojson_file, image_type, ".png", image_type)
            # join images into one output
            added_image = cv2.addWeighted(imagery_img, 1, detected_img, 1, 0)
            timestr = time.strftime("%Y%m%d-%H%M%S")
            cv2.imwrite(f'output/output_{scene["sceneId"][:20]}_{timestr}.png', added_image)
            # detect and count objects from scene
            detected_objects = self.detect_objects(scene, geojson_file, "detections", ".geojson", image_type)
            print(f'Number of objects of class "{image_type}" detected from scene #{i + 1}: {detected_objects}')
            total_detected_objects += detected_objects
        return total_detected_objects

    def detect_objects(self, scene, geojson_file, suffix_name, suffix_format, image_type):
        scene_objects_counter = 0
        # download geojson files for scene
        grid_tiles, map_id, identifier, image_type = self.download_grid_tiles_for_scene(scene,
                                                                                        geojson_file, suffix_name,
                                                                                        suffix_format, image_type)
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

    def count_objects(self, gj, object_class):
        total_object_count = 0
        if "features" in gj:
            for feature in gj["features"]:
                if "properties" in feature and "class" in feature["properties"]:
                    if object_class == feature["properties"]["class"] and "count" in feature["properties"]:
                        total_object_count += feature["properties"]["count"]
        return total_object_count

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
            log(f'Pipeline status: {status}')
            status_check_url = "/tasking/get-status"
            log(f'Waiting {next_try} seconds...')
            time.sleep(next_try)
            # TODO implement status_code check
            status = json.loads(requests.post(self.URL + status_check_url, json={"pipelineId": pipeline_id}).text)[
                'status']

        if status == 'FAILED':
            log("Pipeline initialization failed!")
            return []
        elif status == 'RESOLVED':
            # retrieve scenes
            result_scenes = json.loads(
                requests.post(self.URL + url + '/retrieve', json={"pipelineId": pipeline_id},
                              headers=self.get_headers()).text)
            log(f'Pipeline resolved successfuly!')
            # TODO implement retrieval when cursor not null (concaternate to results)
        else:
            log("Unexpected status for pipeline.")
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
        log(f'Initializing pipeline for scene {scene["sceneId"]}')
        grid = self.run_pipeline(kraken_url, payload)
        return grid

    # TODO consider if saving is necessary
    def dowload_and_save_tile(self, output_file_name, url):
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
        identifier = str(hash(grid['mapId']))[:10]
        # download and save grid tiles then concaternate them into one big png
        for tile in grid['tiles']:
            str_coords = [str(coord) for coord in tile]
            kraken_grid_url = "/kraken/grid/"
            imagery_url = self.URL + kraken_grid_url + \
                          grid['mapId'] + '/-/' + '/'.join(str_coords) + \
                          "/" + suffix_name + suffix_format
            imagery_filename = image_type + "_" + str.replace(grid['mapId'][:10], ".", "_") + "_" + \
                               identifier + "_" + "_".join(str_coords) + \
                               suffix_name + suffix_format
            self.dowload_and_save_tile(imagery_filename, imagery_url)
        # TODO for each band merge into one
        return grid['tiles'], grid['mapId'], identifier, image_type

    # wrapper to download grid tiles and concaternate them into one
    def recreate_image(self, scene, geojson_file, suffix_name, suffix_format, image_type="imagery"):
        return self.concatenate_image(
            *self.download_grid_tiles_for_scene(scene, geojson_file, suffix_name, suffix_format, image_type))

    def concatenate_image(self, grid_tiles, map_id, identifier, image_type):
        # sort coordinates
        sorted_coords = sorted(grid_tiles, key=lambda k: [k[1], k[2]])
        row_coords = set([coord[1] for coord in sorted_coords])
        rows = np.array_split(np.array(sorted_coords), len(row_coords))
        # create rows with images
        v_strips = []
        # conceternate tiled into grid (verticaly first and horizontaly second)
        for row in rows:
            images = [self.get_tile_image(image_type, map_id, identifier, tile) for tile in row]
            v_strips.append(functools.reduce(lambda x, y: cv2.vconcat([x, y]), images))
        reconstructed_img = functools.reduce(lambda x, y: cv2.hconcat([x, y]), v_strips)
        # clean download folder
        shutil.rmtree(self.DOWNLOAD_FOLDER)
        os.mkdir(self.DOWNLOAD_FOLDER)
        return reconstructed_img
