from Client import Client
from dotenv import load_dotenv
import os

coords = "over_brisbane_airport.geojson"
time_range = ["2018-01-01 00:00:00", "2018-01-31 23:59:59"]
# TODO make as input arguments (coords to path to GEOJson file)


def main():
    load_dotenv()
    client = Client(os.getenv("SPACEKNOW_EMAIL"), os.getenv("SPACEKNOW_PASSWORD") )
    count = client.analyze_location(time_range, coords, 'cars')
    print(f'Number of cars total in location for given timerange is {count}')


if __name__ == "__main__":
    main()