import requests, boto3, datetime

s3 = boto3.client('s3')
bucket = 'boston-transit-data'

urls = {
    "trip_updates": "https://cdn.mbta.com/realtime/TripUpdates.json",
    "vehicle_positions": "https://cdn.mbta.com/realtime/VehiclePositions.json",
    "alerts": "https://cdn.mbta.com/realtime/Alerts.json"
}

for name, url in urls.items():
    response = requests.get(url)
    if response.status_code == 200:
        key = f"raw/{name}/{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        s3.put_object(Bucket=bucket, Key=key, Body=response.content)
        print(f"{name} data saved successfully to S3.")
    else:
        print(f"Failed to fetch {name} data.")
