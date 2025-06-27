import requests
import pandas as pd
from utils import upload_df_to_s3


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


if __name__ == "__main__":

    scrape_sports_maniacs({
        'race_id': '684e9e1a-9c7c-449e-83bd-4235ac1f1da3',
        'race_name': 'media_maraton_cordoba',
        'year': '2025',
        'distance': '21k'
    }, {})
