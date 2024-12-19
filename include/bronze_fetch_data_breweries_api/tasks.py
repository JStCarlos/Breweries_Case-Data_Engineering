import requests
from urllib.parse import urlencode
import pytz
import os
from config.config import BREWERY_API_URL

class BonzeFetchDataBreweriesApi:
    def __init__(self):
        self.api_url = BREWERY_API_URL
        self.per_page = 200

    # Get metadata about the total number of breweries to evaluate the number of pages to fetch
    def get_total_breweries_metadata(self):
        metadata = requests.get(self.api_url + '/meta')
        metadata.raise_for_status()
        total_breweries = int(metadata.json()['total'])
        print(f"Total breweries: {total_breweries}")
        pages_to_fetch = total_breweries // self.per_page + (total_breweries % self.per_page > 0)
        return pages_to_fetch
        


    # Fetch breweries data from the breweries API
    def fetch_breweries_data(self, pages_to_fetch): 
        page = 1       
        print(f"Pages to fetch COLLEC: {pages_to_fetch}")
        breweries_data = []
        while page <= pages_to_fetch:
            url = self.api_url + f"?page={page}&per_page={self.per_page}"
            response = requests.get(url)
            response.raise_for_status()
            breweries_data.extend(response.json())
            print(f"Page {page} fetched")
            page += 1
        print(f"Total breweries fetched: {len(breweries_data)}")
        return breweries_data