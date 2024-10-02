import pandas as pd
import numpy as np
from shapely.geometry import Point, Polygon 
import os
import argparse

# parse coordinates from probe csv files 
def parse(coordinates):
    coordinates = coordinates.replace('POLYGON ((', '')
    coordinates = coordinates.replace('))', '')
    coordinate_array = coordinates.split(",")
    parsed = [tuple(map(float, coordinate.split())) for coordinate in coordinate_array]
    return parsed

# haversine function to calculate distance between two lat/lon points in meters 
def haversine(lat1, lon1, lat2, lon2):
    # radius of the Earth in meters
    R = 6371000  
    phi1 = np.radians(lat1)
    phi2 = np.radians(lat2)
    delta_phi = np.radians(lat2 - lat1)
    delta_lambda = np.radians(lon2 - lon1)
    
    a = np.sin(delta_phi / 2) ** 2 + np.cos(phi1) * np.cos(phi2) * np.sin(delta_lambda / 2) ** 2
    c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))
    
    return R * c

def is_within_distance(row, locations, distance_threshold):
        return any(haversine(row['latitude'], row['longitude'], lat, lon) < distance_threshold for lat, lon in zip(locations['latitude'], locations['longitude']))

# read command line input 
parser = argparse.ArgumentParser()
parser.add_argument('bubble', type=int, help='which bubble to access') 
args = parser.parse_args()
bubble = args.bubble
folder_path = f"data_chicago_hackathon_2024/probe_data/{bubble}"

# opening csv files
df_stop = pd.read_csv("data_chicago_hackathon_2024/hamburg_extra_layers/hamburg_stop_signs.csv")
df_light = pd.read_csv("data_chicago_hackathon_2024/hamburg_extra_layers/hamburg_traffic_lights.csv")
#df_yield = pd.read_csv("data_chicago_hackathon_2024/hamburg_extra_layers/hamburg_yield_signs.csv")
df_bb = pd.read_csv("data_chicago_hackathon_2024/hamburg_extra_layers/roundabout_bbox.csv")

coordinates = tuple()
row = df_bb.loc[df_bb['bbox'] == bubble]
# coordinates for boundary box 
coordinates = parse(row['_geometry'].values[0])

# filter out stop signs that are not in the boundary 
polygon = Polygon(coordinates)
drop = []
for stop_sign in df_stop.itertuples(index=True):
    stop_sign_coordinate = (stop_sign.longitude, stop_sign.latitude)
    point = Point(stop_sign_coordinate)
    if not polygon.contains(point):
        drop.append(stop_sign.Index)
df_stop.drop(index=drop, inplace=True)

# filter out stop lights that are not in the boundary
drop = []
for stop_light in df_light.itertuples(index=True):
    stop_light_coordinate = (stop_light.longitude, stop_light.latitude)
    point = Point(stop_light_coordinate)
    if not polygon.contains(point):
        drop.append(stop_light.Index)
df_light.drop(index=drop, inplace=True)

df_stop.reset_index(drop=True, inplace=True)
df_light.reset_index(drop=True, inplace=True)

first_entry = True
file_count = 1

# filtering data
for file_name in os.listdir(folder_path):
    print(f"analyzing file: {file_name} ({file_count})")
    file_count += 1
    file_path = os.path.join(folder_path, file_name)
    df_probe = pd.read_csv(file_path) # reading rows from 1 file

    # filter out points within 100 meters of stop signs
    mask_stop = df_probe.apply(lambda row: is_within_distance(row, df_stop, 100), axis=1)
    # filter out points within 100 meters of stop lights
    mask_light = df_probe.apply(lambda row: is_within_distance(row, df_light, 100), axis=1)

    # data frame clean from points near stop signs and lights 
    df_clean = df_probe[~(mask_stop | mask_light)]

    # half of circumference in kilometers 
    circumference = (np.pi * 0.065)
    # time = distance / speed (50km/hr is typical for roundabout)
    time = (circumference / 50) * 3600
    upper = int(time)
    lower = 0
    result = []
    while upper < len(df_clean):
        lower_row = df_clean.iloc[lower]
        upper_row = df_clean.iloc[upper]

        # check if upper and lower row have same id 
        if not lower_row.traceid == upper_row.traceid:
            upper += 10
            lower += 10
            continue 

        # checking for large gaps in timestamps 
        upper_time = pd.to_datetime(upper_row.sampledate)
        lower_time = pd.to_datetime(lower_row.sampledate)
        delta = (upper_time - lower_time).total_seconds()

        if delta > time:
            lower += 10
            upper += 10
            continue

        # check in chunks of time if direction is changing 
        elif abs(lower_row.heading - upper_row.heading) < 180:
            lower += 10
            upper += 10
            continue
        
        # check if distance traveled is greater than radius in meters
        elif abs(haversine(lower_row.latitude, lower_row.longitude, upper_row.latitude, upper_row.longitude)) > 37.5:
            lower += 10
            upper += 10
            continue
        
        else:
            result.append(lower_row)
            result.append(upper_row)
        lower += 10
        upper += 10
    # final data frame after cleaning data 
    df_final = pd.DataFrame(result, columns=['heading', 'latitude', 'longitude', 'traceid', 'sampledate', 'speed'])
    # dropping all columns except longitude and latitude for easy display 
    df_final.drop(columns=['heading', 'traceid', 'sampledate', 'speed'], inplace=True)
    # printing to csv file to show on gis
    df_final.to_csv(f'roundabout_{bubble}.csv', mode='a', header=first_entry, index=False)
    # will not continue to print header 
    first_entry = False