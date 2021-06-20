from Client import Client
from dotenv import load_dotenv
import os

coords = "over_brisbane_airport.geojson"
time_range = ["2018-01-01 00:00:00", "2018-01-31 23:59:59"]
#TODO make as input arguments (coords to path to GEOJson file)

def main():
    load_dotenv()
    client = Client(os.getenv("SPACEKNOW_EMAIL"), os.getenv("SPACEKNOW_PASSWORD") )
    client.download_scenes(time_range, coords)





if __name__ == "__main__":
    main()