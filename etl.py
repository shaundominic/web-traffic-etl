import requests
import pandas as pd
from io import StringIO
from urllib.parse import urljoin

# Root URL
ROOT_URL = "https://public.wiwdata.com/engineering-challenge/data/"

def download_csv_file(file_name):
    """
    Download csv file  URL and return the contetn as string 
    """
    url = urljoin(ROOT_URL, file_name)
    response = requests.get(url)
    if response.status_code == 200:
        return response.text
    else:
        print(f"Failed to download {file_name}")
        return None

def process_csv_content(csv_content):
    """
    Return df with relevant cols
    """
    df = pd.read_csv(StringIO(csv_content))
    return df[['user_id', 'length', 'path']]

def aggregate_data(dataframes):
    """
    Aggregate df with user_id and path and sum of length 
    """
    aggregated_df = pd.concat(dataframes).groupby(['user_id', 'path']).agg({'length': 'sum'}).unstack(fill_value=0)
    aggregated_df.columns = aggregated_df.columns.droplevel()
    return aggregated_df.reset_index()

def main():
    # Downnload csv, process and outptu as csv
    dataframes = []
    for file_name in [chr(ord('a') + i) + '.csv' for i in range(26)]:
        csv_content = download_csv_file(file_name)
        if csv_content:
            df = process_csv_content(csv_content)
            dataframes.append(df)

    aggregated_df = aggregate_data(dataframes)

    # csv output 
    aggregated_df.to_csv("output.csv", index=False)

if __name__ == "__main__":
    main()
