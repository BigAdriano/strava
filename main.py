import requests
import urllib3
import pandas as pd
import boto3
from decimal import Decimal
import json
import six

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def empty_string2none(obj):
    for k, v in six.iteritems(obj):
        if v == '' or str(v) == "0.0":
            obj[k] = 0
    return obj


auth_url = "https://www.strava.com/oauth/token"
activites_url = "https://www.strava.com/api/v3/athlete/activities"

payload = {
    'client_id': '<YOUR_CLIENT_ID>',
    'client_secret': '<YOUR_CLIENT_SECRET>',
    'refresh_token': '<YOUR_REFRESH_TOKEN>',
    'grant_type': "refresh_token",
    'f': 'json'
}

print("Requesting Token...\n")
res = requests.post(auth_url, data=payload, verify=False)
access_token = res.json()['access_token']
print("Access Token = {}\n".format(access_token))

header = {'Authorization': 'Bearer ' + access_token}
param = {'per_page': 200, 'page': 1}
my_dataset = requests.get(activites_url, headers=header, params=param).json()
activities = pd.DataFrame(data=my_dataset)
drop_activities = ['location_city', 'location_state', 'location_country', 'map', 'trainer', 'commute', 'manual',
                   'private', 'visibility', 'flagged', 'has_heartrate', 'heartrate_opt_out', 'display_hide_heartrate_option',
                   'from_accepted_tag', 'has_kudoed', 'average_heartrate', 'max_heartrate', 'average_watts', 'kilojoules', 'device_watts', 'athlete']
activities = activities.drop(columns=drop_activities)
dynamodb = boto3.resource('dynamodb', endpoint_url="http://localhost:8000")
try:
    table2 = dynamodb.Table('Activities')
    table2.delete()
finally:

    table = dynamodb.create_table(
            TableName='Activities',
            KeySchema=[
                {
                    'AttributeName': 'sport_type',
                    'KeyType': 'HASH'  # Partition key
                },
                {
                    'AttributeName': 'distance',
                    'KeyType': 'Range'  # Sort key
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'sport_type',
                    # AttributeType defines the data type. 'S' is string type and 'N' is number type
                    'AttributeType': 'S'
                },
                {
                    'AttributeName': 'distance',
                    # AttributeType defines the data type. 'S' is string type and 'N' is number type
                    'AttributeType': 'N'
                }
            ],
            ProvisionedThroughput={
                # ReadCapacityUnits set to 10 strongly consistent reads per second
                'ReadCapacityUnits': 10,
                'WriteCapacityUnits': 10  # WriteCapacityUnits set to 10 writes per second
            }
        )
    activities = activities.reset_index()
    for i in activities.iterrows():
        a = dict(i[1:2][0])
        a = json.loads(json.dumps(a), parse_float=Decimal)

        print(a)
        try:
            table.put_item(Item=a,)
        except:
            print("Activity has been added manually, missing some info")

    x = table.scan()
    print('sss')
    dynamodb2 = boto3.client('dynamodb', endpoint_url="http://localhost:8000")
    response = dynamodb2.query(
        TableName='Activities',
        KeyConditionExpression='sport_type = :t AND distance >= :m',
        ExpressionAttributeValues={
            ':m': {'N': '21097.5'},
            ':t': {'S': 'Run'}
        }
    )

    results = pd.DataFrame(response['Items'])
    results.to_csv(r'C:\Users\adrog\OneDrive\Desktop\Strava\running.csv')