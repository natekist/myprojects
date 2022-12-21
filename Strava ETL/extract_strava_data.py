from inspect import trace
import pandas as pd
from pandas import json_normalize
import requests
import urllib3
import os
from sqlalchemy import create_engine
import sqlalchemy
import psycopg2
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
import warnings
warnings.filterwarnings("ignore")

postgresuser = 'postgres'
postgrespass = 'Crosby87'
server = 'localhost'

auth_url = "https://www.strava.com/oauth/token"
activities_url = 'https://www.strava.com/api/v3/activities'

payload = {
    'client_id': "42399",
    'client_secret': '750191abe2d99529b503cbfe70b86ad8f3d8ae70',
    'refresh_token': '4053c255fe9432ade612c43f4bf83f59f3153316',
    'grant_type': "refresh_token",
    'f': 'json'
}

print("Requesting Token...\n")
res = requests.post(auth_url, data=payload, verify=False)
access_token = res.json()['access_token']
print("Access Token = {}\n".format(access_token))

header = {'Authorization': 'Bearer ' + access_token}
param = {'per_page': 200, 'page': 1}
my_dataset = requests.get(activities_url, headers=header, params=param).json()

activities_num = 200 # Number of activities to extract

activities = json_normalize(my_dataset)
activities = activities[activities['trainer'] == False].iloc[:activities_num]
activities_cols_to_keep = ['resource_state', 'name', 'distance', 'moving_time', 'elapsed_time', 'total_elevation_gain', 'type', 'sport_type', 'workout_type', 'id', 'start_date', 'start_date_local', 'timezone', 'utc_offset', 'location_city', 'location_state', 'location_country', 'achievement_count', 'kudos_count', 'comment_count', 'athlete_count', 'photo_count', 'trainer', 'commute', 'manual', 'private', 'visibility', 'flagged', 'gear_id', 'start_latlng', 'end_latlng', 'average_speed', 'max_speed', 'average_temp', 'average_watts', 'kilojoules', 'device_watts', 'has_heartrate', 'average_heartrate', 'max_heartrate', 'heartrate_opt_out', 'display_hide_heartrate_option', 'elev_high', 'elev_low', 'upload_id', 'upload_id_str', 'external_id', 'from_accepted_tag', 'pr_count', 'total_photo_count', 'has_kudoed', 'suffer_score', 'description', 'calories', 'perceived_exertion', 'prefer_perceived_exertion', 'hide_from_home', 'device_name', 'embed_token', 'available_zones', 'athlete.id', 'athlete.resource_state', 'map.id', 'map.polyline', 'map.resource_state', 'map.summary_polyline', 'gear.id', 'gear.primary', 'gear.name', 'gear.nickname', 'gear.resource_state', 'gear.retired', 'gear.distance', 'gear.converted_distance', 'photos.primary', 'photos.count']

for j in range(len(activities)):
    activities_detail_url = activities_url + '/' + str(activities.iloc[j]['id']) + '?access_token=' + access_token
    activity_detail = requests.get(activities_detail_url, headers=header).json()
    activity_detail = json_normalize(activity_detail)
    if j == 0:
        segments = json_normalize(activity_detail['segment_efforts'][0])
        activity_details = activity_detail[activity_detail.columns.intersection(activities_cols_to_keep)]
    else:    
        segments = segments.append(json_normalize(activity_detail['segment_efforts'][0]), ignore_index=True)  
        activity_details = activity_details.append(activity_detail[activity_detail.columns.intersection(activities_cols_to_keep)], ignore_index=True)





def load(df, tbl):
    try:
        rows_imported = 0
        engine = create_engine(f'postgresql://{postgresuser}:{postgrespass}@{server}:5432/strava')  
        print(f'importing rows {rows_imported} to {rows_imported + len(df)}... for table {tbl}')
        df.to_sql(f'stg_{tbl}', engine, if_exists='replace', index=False, dtype={"achievements": sqlalchemy.types.JSON})
    finally:
        exit

load(segments, 'segment_efforts')
load(activity_details, 'activity_detail')