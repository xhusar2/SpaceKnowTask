# SPACEKNOW TASK

## Objective

Purpouse of a script is to analyse location given by input geojson file under given time period.
Analysis is defaultly set to detection of cars. Script therefore implicitly downloads all possible images from available providers, reconstructs images with detected items on top of them and also returns count of detected objects (cars) for each image and also as a total for the whole period.


## How to use script

python3 client.py <input_geojson> <start_time> <end_time> [<analysis_type>]

e.g. python3 client.py brisbane_airport.geojson" "2018-01-01 00:00:00" "2018-01-31 23:59:59" 
