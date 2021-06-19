from Client import Client

coords = "over_brisbane_airport.geojson"
time_range = ["2018-01-01 00:00:00", "2018-01-31 23:59:59"]
#TODO make as input arguments (coords to path to GEOJson file)

def main():
    client = Client()
    client.download_images(time_range, coords)





if __name__ == "__main__":
    main()