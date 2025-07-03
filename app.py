import requests
import pandas as pd
from utils import upload_df_to_s3, get_data_athlinks


def scrape_sports_maniacs(event, context):
    # URL of the API endpoint
    url = f"https://sportmaniacs.com/races/rankings/{event['race_id']}"

    # Send a GET request to fetch the data
    response = requests.get(url)

    # Parse the JSON response
    json_data = response.json()

    # Extract the Rankings data
    rankings = json_data.get("data", {}).get("Rankings", [])

    # Create a pandas DataFrame
    df = pd.DataFrame(rankings)

    upload_df_to_s3(
        df,
        bucket_name="zarruk",
        key=f"datarunner/data/{event['race_name']}/datos_{event['year']}_{event['distance']}_.csv"
    )


def scrape_athlinks(event, context):
    df_all = pd.DataFrame()
    i = 0

    while True:
        try:
            df = get_data_athlinks(i * 50, event['year'], event['event_id'], event['race_id'], event['race_distance'])
            df_all = pd.concat([df_all, df])
            i += 1
        except Exception as e:
            print(f"Failed at iteration {i}: {e}")
            break

    df_all = df_all[df_all['status']=='CONF']

    upload_df_to_s3(
        df,
        bucket_name="zarruk",
        key=f"datarunner/data/{event['race_name']}/datos_{event['year']}_{event['distance']}_.csv"
    )


def scrape_results(event, context):

    if event['source'] == 'athlinks':
        scrape_athlinks(event, context)

    elif event['source'] == 'sportsmaniacs':
        scrape_sports_maniacs(event, context)
    

if __name__ == "__main__":

    #scrape_results({
    #    'race_id': '684e9e1a-9c7c-449e-83bd-4235ac1f1da3',
    #    'race_name': 'media_maraton_cordoba',
    #    'year': '2025',
    #    'distance': '21k'
    #}, {})

    scrape_results({
        'event_id': 1112509,
        'race_id': 2602990,
        'race_distance': 21114,
        'year': 2025,
        'race_name': 'media_maraton_cali',
        'distance': '21k'
    }, {})
